mod common;
mod xblock;

use crate::xblock::XBlock;
use dotenv::dotenv;
use flate2::read::GzDecoder;
use near_indexer::near_primitives::borsh::BorshSerialize;
use near_indexer::near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::io::Read;
use std::{env, fs};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "lake_convert";
const BLOCK_FOLDER: &str = "block";
const SAVE_EVERY_N: BlockHeight = 1000;

pub struct Config {
    pub from_block: BlockHeight,
    pub to_block: BlockHeight,
    pub path: String,
}

pub struct WriterConfig {
    pub path: String,
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

pub async fn start(config: Config, blocks_sink: mpsc::Sender<XBlock>) {
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
        let rounded_padded_block_height = format!("{:0>12}", block_height / 5 * 5);
        let padded_block_height = format!("{:0>12}", block_height);
        let expected_sub_path = format!(
            "{}/{}/{}.tgz",
            &rounded_padded_block_height[..6],
            &rounded_padded_block_height[6..9],
            rounded_padded_block_height
        );
        let block_path = format!("{}/{}/{}", config.path, BLOCK_FOLDER, expected_sub_path);
        if block_path != current_block_file {
            tracing::log::debug!(target: PROJECT_ID, "Reading block: {}", block_path);
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
            tracing::log::debug!(target: PROJECT_ID, "Block not found: {}", block_height);
            continue;
        }
        let block = serde_json::from_str(&block_str.unwrap()).unwrap();
        let shards = shard_entries
            .iter()
            .filter_map(|shard| shard.get(&format!("{}.json", padded_block_height)))
            .map(|s| serde_json::from_str(s).unwrap())
            .collect::<Vec<_>>();
        let streamer_message = XBlock { block, shards };
        blocks_sink.send(streamer_message).await.unwrap();
    }
}

pub fn streamer(config: Config) -> mpsc::Receiver<XBlock> {
    let (sender, receiver) = mpsc::channel(100);
    actix::spawn(start(config, sender));
    receiver
}

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("lake_convert=info");

    tracing::log::info!(target: PROJECT_ID, "Starting NEAR Lake Indexer");

    let config = Config {
        from_block: env::var("FROM_BLOCK")
            .expect("FROM_BLOCK is required")
            .parse::<BlockHeight>()
            .unwrap()
            / SAVE_EVERY_N
            * SAVE_EVERY_N,
        to_block: env::var("TO_BLOCK")
            .expect("TO_BLOCK (exclusive) is required")
            .parse::<BlockHeight>()
            .unwrap()
            / SAVE_EVERY_N
            * SAVE_EVERY_N,
        path: env::var("LAKE_DATA_PATH").unwrap(),
    };

    let writer_config = WriterConfig {
        path: env::var("WRITE_DATA_PATH").expect("Missing env WRITE_DATA_PATH"),
    };

    let sys = actix::System::new();
    sys.block_on(async move {
        let stream = streamer(config);
        listen_blocks(stream, writer_config).await;

        actix::System::current().stop();
    });
    sys.run().unwrap();
}

fn save_blocks(blocks: &[XBlock], config: &WriterConfig) -> std::io::Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    let starting_block = blocks[0].block.header.height / SAVE_EVERY_N * SAVE_EVERY_N;
    let padded_block_height = format!("{:0>12}", starting_block);
    let path = format!("{}/{}", config.path, &padded_block_height[..6]);
    fs::create_dir_all(&path)?;
    let filename = format!("{}/{}.tgz", path, padded_block_height);
    tracing::log::info!(target: PROJECT_ID, "Saving blocks to: {}", filename);
    // Creating archive
    let tar_gz = fs::File::create(&filename)?;
    let enc = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);
    for block in blocks {
        let block_str = block.try_to_vec().unwrap();
        let padded_block_height = format!("{:0>12}", block.block.header.height);
        let mut header = tar::Header::new_gnu();
        header.set_path(&format!("{}.borsh", padded_block_height))?;
        header.set_size(block_str.len() as u64);
        header.set_cksum();
        tar.append(&header, &block_str[..])?;
    }

    tar.into_inner()?.finish()?;
    Ok(())
}

async fn listen_blocks(mut stream: mpsc::Receiver<XBlock>, config: WriterConfig) {
    let mut blocks: Vec<XBlock> = vec![];
    while let Some(xblock) = stream.recv().await {
        let block_height = xblock.block.header.height;
        if blocks.get(0).map(|b| b.block.header.height).unwrap_or(0) / SAVE_EVERY_N
            != block_height / SAVE_EVERY_N
        {
            save_blocks(&blocks, &config).expect("Failed to save blocks");
            blocks.clear();
        }

        tracing::log::debug!(target: PROJECT_ID, "Processing block: {}", block_height);
        //
        // let v = serde_json::to_vec(&streamer_message).unwrap();
        // let xblock: XBlock = serde_json::from_slice(&v).expect("Failed to deserialize");
        blocks.push(xblock);
    }
    if !blocks.is_empty() {
        save_blocks(&blocks, &config).expect("Failed to save blocks");
    }
}
