mod common;
mod redis_db;

use redis_db::RedisDB;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Prefix;

use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::StreamerMessage;
use tokio::sync::mpsc;

const PROJECT_ID: &str = "ft_injector";

const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";
const SAFE_OFFSET: u64 = 100;
const MIN_BATCH: usize = 10000;

async fn inject(
    redis_db: &mut RedisDB,
    prefix: &str,
    records: &[(String, Vec<(String, String)>)],
) -> redis::RedisResult<()> {
    let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();
        for (account_id, token_ids) in records {
            pipe.cmd("HSET")
                .arg(format!("{}:{}", prefix, account_id))
                .arg(token_ids)
                .ignore();
        }
        pipe.query_async(connection).await
    });
    res
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("ft_injector=info,redis=info");

    tracing::log::info!(target: PROJECT_ID, "Starting FT/Stake Injector");

    let mut write_redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|arg| arg.as_str()).unwrap_or("");

    let (prefix, env_name) = match command {
        "ft" => ("ft", "FT_CSV_PATH"),
        "stake" => ("st", "STAKE_CSV_PATH"),
        _ => {
            panic!("Invalid command {:?}, expected ft or stake", command);
        }
    };

    let csv_fn = env::var(env_name).expect(&format!("Missing env {}", env_name));
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(std::fs::File::open(csv_fn).expect("Failed to open the file"));

    let mut account_to_field: HashMap<String, Vec<(String, String)>> = HashMap::new();
    let mut total_records: usize = 0;
    for (i, result) in csv_reader.records().enumerate() {
        if i % 10000 == 0 {
            tracing::log::info!(target: PROJECT_ID, "Processed {} records", i);
        }
        let record = result.expect("Failed to read the record");
        let account_id = record.get(0).expect("Missing account_id").to_string();
        let field = record.get(1).expect("Missing field").to_string();
        total_records += 1;
        account_to_field
            .entry(account_id)
            .or_insert_with(Vec::new)
            .push((field, "".to_string()));
    }

    tracing::log::info!(target: PROJECT_ID, "Going to inject {} accounts and {} records", account_to_field.len(), total_records);

    let mut records = Vec::new();
    let mut cnt = 0;
    let mut total_cnt = 0;
    for (i, (account_id, fields)) in account_to_field.into_iter().enumerate() {
        cnt += fields.len();
        records.push((account_id, fields));
        if i % 100 == 0 || cnt >= MIN_BATCH {
            tracing::log::info!(target: PROJECT_ID, "Injecting progress: {} accounts and {} records", i, total_cnt);
        }
        if cnt >= MIN_BATCH {
            inject(&mut write_redis_db, prefix, &records)
                .await
                .expect("Failed to inject");
            records.clear();
            total_cnt += cnt;
            cnt = 0;
        }
    }
    if !records.is_empty() {
        inject(&mut write_redis_db, prefix, &records)
            .await
            .expect("Failed to inject");
        total_cnt += cnt;
    }

    tracing::log::info!(target: PROJECT_ID, "DONE: Injected {} records", total_cnt);
}
