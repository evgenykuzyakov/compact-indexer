mod click;
mod common;

use click::*;
use clickhouse::Client;
use dotenv::dotenv;
use flate2::read::GzDecoder;
use near_indexer::near_primitives::types::BlockHeight;
use near_indexer::StreamerMessage;
use std::collections::HashMap;
use std::io::Read;
use std::{env, fs};
use tokio::sync::mpsc;
use walkdir::WalkDir;

const PROJECT_ID: &str = "lake_indexer";
const BLOCK_FOLDER: &str = "block";

pub struct Config {
    from_block: BlockHeight,
    to_block: BlockHeight,
    path: String,
}

fn read_folder(path: &str) -> Vec<String> {
    let mut entries: Vec<String> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    entries.sort();
    entries
}

fn read_archive(path: &str) -> HashMap<String, String> {
    if !std::path::Path::new(path).exists() {
        return HashMap::new();
    }
    tar::Archive::new(GzDecoder::new(std::fs::File::open(path).unwrap()))
        .entries()
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|mut e| {
            let path = e.path().unwrap().to_string_lossy().to_string();
            let mut content = String::new();
            e.read_to_string(&mut content).unwrap();
            (path, content)
        })
        .collect()
}

pub async fn start(config: Config, blocks_sink: mpsc::Sender<StreamerMessage>) {
    let mut shards = read_folder(&config.path);
    assert!(
        shards.contains(&BLOCK_FOLDER.to_string()),
        "Block folder not found"
    );
    shards.retain(|s| s != BLOCK_FOLDER);
    let mut current_block_file = "".to_string();
    let mut block_entries = HashMap::new();
    let mut shard_entries = vec![];

    for block_height in config.from_block..config.to_block {
        tracing::log::debug!(target: PROJECT_ID, "Processing block: {}", block_height);
        let padded_block_height = format!("{:0>12}", block_height / 5 * 5);
        let expected_sub_path = format!(
            "{}/{}/{}.tgz",
            &padded_block_height[..6],
            &padded_block_height[6..9],
            padded_block_height
        );
        let block_path = format!("{}/{}/{}", config.path, BLOCK_FOLDER, expected_sub_path);
        if block_path != current_block_file {
            tracing::log::info!(target: PROJECT_ID, "Reading block: {}", block_path);
            current_block_file = block_path;
            block_entries = read_archive(&current_block_file);
            shard_entries = shards
                .iter()
                .map(|shard_id| {
                    let shard_path = format!("{}/{}/{}", config.path, shard_id, expected_sub_path);
                    read_archive(&shard_path)
                })
                .collect();
        }
        let block_str = block_entries.get(&format!("{}.json", padded_block_height));
        if block_str.is_none() {
            tracing::log::info!(target: PROJECT_ID, "Block not found: {}", block_height);
            continue;
        }
        let block = serde_json::from_str(&block_str.unwrap()).unwrap();
        let shards = shard_entries
            .iter()
            .filter_map(|shard| shard.get(&format!("{}.json", padded_block_height)))
            .map(|s| serde_json::from_str(s).unwrap())
            .collect::<Vec<_>>();
        let streamer_message = StreamerMessage { block, shards };
        blocks_sink.send(streamer_message).await.unwrap();
    }
}

pub fn streamer(config: Config) -> mpsc::Receiver<StreamerMessage> {
    let (sender, receiver) = mpsc::channel(100);
    actix::spawn(start(config, sender));
    receiver
}

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("clickhouse=info,lake_indexer=info");

    tracing::log::info!(target: PROJECT_ID, "Starting NEAR Lake Indexer");

    let client = establish_connection();

    let config = Config {
        from_block: 9_820_200,
        to_block: 9_820_300,
        path: env::var("LAKE_DATA_PATH").unwrap(),
    };

    let sys = actix::System::new();
    sys.block_on(async move {
        let stream = streamer(config);
        listen_blocks(stream, client).await;

        actix::System::current().stop();
    });
    sys.run().unwrap();
}

async fn listen_blocks(mut stream: mpsc::Receiver<StreamerMessage>, client: Client) {
    while let Some(streamer_message) = stream.recv().await {
        tracing::log::info!(target: PROJECT_ID, "Received streamer message: {:?}", streamer_message);
        //extract_info(&client, streamer_message).await.unwrap();
    }
}
