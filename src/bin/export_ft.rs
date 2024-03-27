mod click;
mod common;
mod redis_db;

use redis_db::RedisDB;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;

use crate::click::{extract_rows, ActionKind, ActionRow, EventRow, ReceiptStatus};
use dotenv::dotenv;
use near_crypto::PublicKey;
use near_indexer::near_primitives::types::{AccountId, BlockHeight};
use near_indexer::StreamerMessage;
use serde_json::json;
use tokio::sync::mpsc;
use tracing_subscriber::fmt::format;

const PROJECT_ID: &str = "export_ft";

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PairUpdate {
    account_id: String,
    token_id: String,
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("export_ft=info,redis=info");

    tracing::log::info!(target: PROJECT_ID, "Starting FT Redis Indexer");

    let mut read_redis_db = RedisDB::new(Some(
        env::var("EXPORT_READ_REDIS_URL").expect("Missing env EXPORT_READ_REDIS_URL"),
    ))
    .await;

    let mut accounts = HashSet::new();
    let mut cursor = "0".to_string();
    let mut total_accounts = 0;
    let mut last_multiplier = 0;
    loop {
        let res = with_retries!(read_redis_db, |connection| async {
            let res: redis::RedisResult<(String, Vec<String>)> = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg("ft:*")
                .arg("COUNT")
                .arg(100000)
                .query_async(connection)
                .await;
            res
        })
        .expect("Failed to scan keys");
        let (next_cursor, keys) = res;
        cursor = next_cursor;
        total_accounts += keys.len();
        accounts.extend(keys);
        let mult = accounts.len() / 100000;
        if mult > last_multiplier {
            last_multiplier = mult;
            tracing::info!(target: PROJECT_ID, "Scanned {} accounts", total_accounts);
        }
        if cursor == "0" {
            break;
        }
    }

    let export_fn = env::var("EXPORT_FN").expect("Missing env EXPORT_FN");
    let f = std::fs::File::create(export_fn).expect("Failed to create export file");
    let mut total_pairs = 0;
    let mut w = csv::Writer::from_writer(f);
    for (i, account_key) in accounts.into_iter().enumerate() {
        if i % 10000 == 0 {
            tracing::info!(target: PROJECT_ID, "Exported {} tokens out of {}. Total pairs: {}", i, total_accounts, total_pairs);
        }
        let res = with_retries!(read_redis_db, |connection| async {
            let res: redis::RedisResult<Vec<(String, String)>> = redis::cmd("HGETALL")
                .arg(&account_key)
                .query_async(connection)
                .await;
            res
        })
        .expect("Failed to get tokens for account");
        let account_id = account_key.split(':').last().unwrap();
        total_pairs += res.len();
        for (token_id, _) in res {
            w.write_record(&[&token_id, account_id])
                .expect("Failed to write record");
        }
    }
}
