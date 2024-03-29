mod common;
mod redis_db;

use redis_db::RedisDB;
use std::collections::{BinaryHeap, HashSet};
use std::env;

use dotenv::dotenv;

const PROJECT_ID: &str = "transfer_b_to_tb";

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PairUpdate {
    account_id: String,
    token_id: String,
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("transfer_b_to_tb=info,redis=info");

    tracing::log::info!(target: PROJECT_ID, "Starting FT Redis Indexer");

    let mut read_redis_db = RedisDB::new(Some(
        env::var("EXPORT_READ_REDIS_URL").expect("Missing env EXPORT_READ_REDIS_URL"),
    ))
    .await;

    let mut redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|arg| arg.as_str()).unwrap_or("");

    let mut tokens = HashSet::new();
    let mut cursor = "0".to_string();
    let mut total_tokens = 0;
    let mut last_multiplier = 0;
    loop {
        let res = with_retries!(read_redis_db, |connection| async {
            let res: redis::RedisResult<(String, Vec<String>)> = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg("b:*")
                .arg("COUNT")
                .arg(100000)
                .query_async(connection)
                .await;
            res
        })
        .expect("Failed to scan keys");
        let (next_cursor, keys) = res;
        cursor = next_cursor;
        total_tokens += keys.len();
        tokens.extend(keys);
        let mult = tokens.len() / 100000;
        if mult > last_multiplier {
            last_multiplier = mult;
            tracing::info!(target: PROJECT_ID, "Scanned {} tokens", total_tokens);
        }
        if cursor == "0" {
            break;
        }
    }

    tracing::info!(target: PROJECT_ID, "Total tokens: {}", total_tokens);
    let max_top_holders_count: u64 = env::var("MAX_TOP_HOLDERS_COUNT")
        .unwrap_or("1000".to_string())
        .parse()
        .expect("Invalid MAX_TOP_HOLDERS_COUNT");
    for (i, token_key) in tokens.into_iter().enumerate() {
        let token_id = token_key.split(':').last().unwrap();
        tracing::info!(target: PROJECT_ID, "Processing token {}/{}: {}", i, total_tokens, token_id);
        let res = with_retries!(read_redis_db, |connection| async {
            let res: redis::RedisResult<Vec<(String, String)>> = redis::cmd("HGETALL")
                .arg(&token_key)
                .query_async(connection)
                .await;
            res
        })
        .expect("Failed to get tokens for account");
        // Extracting top holders
        let mut heap = BinaryHeap::with_capacity(max_top_holders_count as usize + 1);

        for (account_id, balance) in res {
            if let Ok(balance) = balance.parse::<u128>() {
                heap.push((balance, account_id));
                if heap.len() > max_top_holders_count as usize {
                    heap.pop();
                }
            }
        }

        let top_holders = heap.into_sorted_vec();
        tracing::info!(target: PROJECT_ID, "Top holders: {:?}", &top_holders.iter().take(10).collect::<Vec<_>>());

        if command == "run" {
            let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
                let mut pipe = redis::pipe();
                pipe.cmd("ZADD").arg(format!("tb:{}", token_id)).arg("NX");
                for (balance, account_id) in &top_holders {
                    pipe.arg(balance.to_string()).arg(account_id).ignore();
                }

                // Keeping sorted sets lengths to a fixed size
                pipe.cmd("ZREMRANGEBYRANK")
                    .arg(format!("tb:{}", token_id))
                    .arg(0)
                    .arg(-(max_top_holders_count as i64 + 1))
                    .ignore();

                pipe.query_async(connection).await
            });
            res.expect("Failed to update");
        }
    }
}
