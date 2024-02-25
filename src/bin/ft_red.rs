mod click;
mod common;
mod redis_db;

use redis_db::RedisDB;
use std::collections::{HashMap, HashSet};
use std::env;

use crate::click::{extract_rows, ActionRow, EventRow, ReceiptStatus};
use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::StreamerMessage;
use tokio::sync::mpsc;

const PROJECT_ID: &str = "ft_red";

const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";

async fn start(
    mut last_id: String,
    mut redis_db: RedisDB,
    blocks_sink: mpsc::Sender<StreamerMessage>,
) {
    loop {
        let res = redis_db.xread(1, FINAL_BLOCKS_KEY, &last_id).await;
        let res = match res {
            Ok(res) => res,
            Err(err) => {
                tracing::log::error!(target: PROJECT_ID, "Error: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                let _ = redis_db.reconnect().await;
                continue;
            }
        };
        let (id, key_values) = res.into_iter().next().unwrap();
        assert_eq!(key_values.len(), 1, "Expected 1 key-value pair");
        let (key, value) = key_values.into_iter().next().unwrap();
        assert_eq!(key, BLOCK_KEY, "Expected key to be block");
        let streamer_message: StreamerMessage = serde_json::from_str(&value).unwrap();
        blocks_sink.send(streamer_message).await.unwrap();
        last_id = id;
    }
}

pub fn streamer(last_id: String, redis_db: RedisDB) -> mpsc::Receiver<StreamerMessage> {
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(start(last_id, redis_db, sender));
    receiver
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("ft_red=info,redis=info,clickhouse=info");

    tracing::log::info!(target: PROJECT_ID, "Starting FT Redis Indexer");

    let mut read_redis_db = RedisDB::new(None).await;
    let write_redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    let (id, _key_values) = read_redis_db
        .xread(1, FINAL_BLOCKS_KEY, "0")
        .await
        .expect("Failed to get the first block from Redis")
        .into_iter()
        .next()
        .unwrap();
    let first_block_height: BlockHeight = id.split_once("-").unwrap().0.parse().unwrap();
    tracing::log::info!(target: PROJECT_ID, "First redis block {}", first_block_height);

    // TODO: last_block_height
    let last_block_height = Some(first_block_height + 1000);
    let last_id = last_block_height
        .map(|h| format!("{}-0", h))
        .unwrap_or("0".to_string());
    tracing::log::info!(target: PROJECT_ID, "Resuming from {}", last_block_height.unwrap_or(0));

    if first_block_height + 30 > last_block_height.unwrap_or(0) {
        panic!("The first block in the redis is too close to the last block");
    }

    let stream = streamer(last_id, read_redis_db);
    listen_blocks(stream, write_redis_db).await;
}

async fn listen_blocks(mut stream: mpsc::Receiver<StreamerMessage>, mut redis_db: RedisDB) {
    while let Some(streamer_message) = stream.recv().await {
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", streamer_message.block.header.height);
        let (actions, events) = extract_rows(streamer_message);

        update_ft_pairs(&actions, &events, &mut redis_db)
            .await
            .expect("Failed to update FT pairs");
    }
}

async fn update_ft_pairs(
    actions: &[ActionRow],
    events: &[EventRow],
    redis_db: &mut RedisDB,
) -> redis::RedisResult<()> {
    // Extract matching (token_id, account_id) for FT changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &action.account_id;
        if token_id.ends_with(".poolv1.near") || token_id.ends_with(".pool.near") {
            continue;
        }
        if let Some(method_name) = action.method_name.as_ref() {
            if [
                "ft_transfer_call",
                "ft_transfer",
                "ft_mint",
                "ft_burn",
                "near_withdraw",
                "near_deposit",
                "deposit_and_stake",
            ]
            .contains(&method_name.as_str())
            {
                pairs.insert((action.predecessor_id.clone(), token_id.clone()));
                if let Some(account_id) = action.args_receiver_id.as_ref() {
                    pairs.insert((account_id.clone(), token_id.clone()));
                }
            }
        }
    }
    for event in events {
        if event.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &event.account_id;
        if let Some(event_type) = event.event.as_ref() {
            if ["ft_mint", "ft_burn"].contains(&event_type.as_str()) {
                if let Some(account_id) = event.data_owner_id.as_ref() {
                    pairs.insert((account_id.clone(), token_id.clone()));
                }
            } else if event_type == "ft_transfer" {
                if let Some(account_id) = event.data_new_owner_id.as_ref() {
                    pairs.insert((account_id.clone(), token_id.clone()));
                }
                if let Some(account_id) = event.data_old_owner_id.as_ref() {
                    pairs.insert((account_id.clone(), token_id.clone()));
                }
            }
        }
    }

    // Update the pairs in the Redis
    if pairs.is_empty() {
        return Ok(());
    }

    let account_id_to_token_id: HashMap<String, Vec<(String, String)>> =
        pairs
            .into_iter()
            .fold(HashMap::new(), |mut acc, (account_id, token_id)| {
                acc.entry(account_id)
                    .or_insert_with(Vec::new)
                    .push((token_id, "".to_string()));
                acc
            });
    tracing::log::info!(target: PROJECT_ID, "Updating {} pairs", account_id_to_token_id.len());

    let _: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();
        for (account_id, token_ids) in &account_id_to_token_id {
            pipe.cmd("HSET")
                .arg(format!("ft:{}", account_id))
                .arg(token_ids)
                .ignore();
        }
        pipe.query_async(connection).await
    });

    Ok(())
}
