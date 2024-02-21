mod click;
mod common;
mod redis_db;

use click::ClickDB;
use redis_db::RedisDB;

use crate::click::extract_info;
use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::StreamerMessage;
use tokio::sync::mpsc;

const PROJECT_ID: &str = "redis_indexer";

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

    common::setup_tracing("clickhouse=info,redis_indexer=info");

    tracing::log::info!(target: PROJECT_ID, "Starting NEAR Redis Indexer");

    let mut click_db = ClickDB::new(1);
    let mut redis_db = RedisDB::new().await;

    let (id, _key_values) = redis_db
        .xread(1, FINAL_BLOCKS_KEY, "0")
        .await
        .expect("Failed to get the first block from Redis")
        .into_iter()
        .next()
        .unwrap();
    let first_block_height: BlockHeight = id.split_once("-").unwrap().0.parse().unwrap();
    tracing::log::info!(target: PROJECT_ID, "First redis block {}", first_block_height);

    let last_block_height = click_db
        .last_block_height()
        .await
        .expect("Failed to get last block from clickhouse");
    let last_id = last_block_height
        .map(|h| format!("{}-0", h))
        .unwrap_or("0".to_string());
    tracing::log::info!(target: PROJECT_ID, "Resuming from {}", last_block_height.unwrap_or(0));

    if first_block_height + 30 > last_block_height.unwrap_or(0) {
        panic!("The first block in the redis is too close to the last block in the clickhouse");
    }

    let stream = streamer(last_id, redis_db);
    listen_blocks(stream, click_db).await;
}

async fn listen_blocks(mut stream: mpsc::Receiver<StreamerMessage>, mut db: ClickDB) {
    while let Some(streamer_message) = stream.recv().await {
        tracing::log::debug!(target: PROJECT_ID, "Received streamer message: {:?}", streamer_message.block.header.height);
        extract_info(&mut db, streamer_message).await.unwrap();
    }
    db.commit().await.unwrap();
}
