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

const PROJECT_ID: &str = "ft_red";

const FINAL_BLOCKS_KEY: &str = "final_blocks";
const BLOCK_KEY: &str = "block";
const SAFE_OFFSET: u64 = 100;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PairUpdate {
    account_id: String,
    token_id: String,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PublicKeyPair {
    account_id: String,
    public_key: PublicKey,
}

#[derive(Debug, Copy, Clone)]
pub enum PublicKeyUpdateType {
    AddedFullAccess,
    AddedLimitedAccessKey,
    RemovedKey,
}

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

    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|arg| arg.as_str()).unwrap_or("");

    let mut read_redis_db = RedisDB::new(None).await;
    let mut write_redis_db = RedisDB::new(Some(
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

    if command == "backfill" {
        let block_height: BlockHeight = env::args()
            .nth(2)
            .expect("Missing block height")
            .parse()
            .expect("Invalid block height");
        tracing::log::info!(target: PROJECT_ID, "Backfilling block {}", block_height);

        if first_block_height > block_height {
            panic!("Too late to backfill");
        }

        let id = format!("{}-0", block_height - 1);
        let res = read_redis_db
            .xread(1, FINAL_BLOCKS_KEY, &id)
            .await
            .expect("Failed to get the block");
        let (_, key_values) = res.into_iter().next().unwrap();
        assert_eq!(key_values.len(), 1, "Expected 1 key-value pair");
        let (key, value) = key_values.into_iter().next().unwrap();
        assert_eq!(key, BLOCK_KEY, "Expected key to be block");
        let streamer_message: StreamerMessage = serde_json::from_str(&value).unwrap();

        let block_height = streamer_message.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        let (actions, events) = extract_rows(streamer_message);

        let ft_pairs = extract_ft_pairs(&actions, &events);

        {
            let path = env::var("BACKFILL_FILE").expect("BACKFILL_FILE is not set");
            let f = std::fs::File::create(path).expect("Failed to create backfill file");
            serde_json::to_writer(f, &json!({
                "block_height": block_height,
                "pairs": ft_pairs.iter().map(|pair| format!("{}:{}", pair.token_id, pair.account_id)).collect::<Vec<_>>()
            })).expect("Failed to write backfill file");
        }
        return;
    }
    if command != "run" {
        panic!("Invalid command");
    }

    let last_block_height: BlockHeight = write_redis_db
        .get("meta:latest_block")
        .await
        .expect("Failed to get the latest block")
        .map(|s| s.parse().unwrap())
        .unwrap_or(first_block_height + SAFE_OFFSET);

    if first_block_height + SAFE_OFFSET > last_block_height {
        panic!("The first block in the redis is too close to the last block");
    }

    let last_id = format!("{}-0", last_block_height);
    tracing::log::info!(target: PROJECT_ID, "Resuming from {}", last_block_height);

    let stream = streamer(last_id, read_redis_db);
    listen_blocks(stream, write_redis_db).await;
}

async fn listen_blocks(mut stream: mpsc::Receiver<StreamerMessage>, mut redis_db: RedisDB) {
    while let Some(streamer_message) = stream.recv().await {
        let block_height = streamer_message.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        let (actions, events) = extract_rows(streamer_message);

        let mut to_update: HashMap<String, Vec<(String, String)>> = HashMap::new();

        let ft_pairs = extract_ft_pairs(&actions, &events);

        add_pairs_to_update("ft", ft_pairs.clone(), &mut to_update, block_height);
        add_pairs_to_update(
            "nf",
            extract_nft_pairs(&actions, &events),
            &mut to_update,
            block_height,
        );
        add_pairs_to_update(
            "st",
            extract_staking_pairs(&actions),
            &mut to_update,
            block_height,
        );
        let public_key_updates = extract_public_keys(&actions);

        tracing::log::info!(target: PROJECT_ID, "Updating {} accounts, {} keys", to_update.len(), public_key_updates.len());
        // tracing::log::info!(target: PROJECT_ID, "Updating keys {:?}", public_key_updates);
        // tracing::log::info!(target: PROJECT_ID, "Updating accounts {:?}", to_update);

        let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
            let mut pipe = redis::pipe();
            for (key, fields_data) in &to_update {
                pipe.cmd("HSET").arg(key).arg(fields_data).ignore();
            }
            for (pair, update_type) in &public_key_updates {
                let key = format!("pk:{}", pair.public_key);
                match update_type {
                    PublicKeyUpdateType::AddedFullAccess => {
                        pipe.cmd("HSET")
                            .arg(key)
                            .arg(&pair.account_id)
                            .arg("f")
                            .ignore();
                    }
                    PublicKeyUpdateType::AddedLimitedAccessKey => {
                        pipe.cmd("HSET")
                            .arg(key)
                            .arg(&pair.account_id)
                            .arg("l")
                            .ignore();
                    }
                    PublicKeyUpdateType::RemovedKey => {
                        pipe.cmd("HDEL").arg(key).arg(&pair.account_id).ignore();
                    }
                }
            }

            pipe.cmd("SET")
                .arg("meta:latest_block")
                .arg(block_height)
                .ignore();

            pipe.cmd("RPUSH")
                .arg("ft_updates")
                .arg(json!({
                    "block_height": block_height,
                    "pairs": ft_pairs.iter().map(|pair| format!("{}:{}", pair.token_id, pair.account_id)).collect::<Vec<_>>()
                }).to_string())
                .ignore();

            pipe.query_async(connection).await
        });
        res.expect("Failed to update");
    }
}

fn extract_staking_pairs(actions: &[ActionRow]) -> HashSet<PairUpdate> {
    // Extract matching (account_id, validator_id) for staking changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success || action.action != ActionKind::FunctionCall {
            continue;
        }
        if action.account_id.ends_with(".poolv1.near") || action.account_id.ends_with(".pool.near")
        {
            pairs.insert(PairUpdate {
                account_id: action.predecessor_id.clone(),
                token_id: action.account_id.clone(),
            });
        }
    }

    pairs
}

fn extract_public_keys(actions: &[ActionRow]) -> HashMap<PublicKeyPair, PublicKeyUpdateType> {
    // Extract matching (account_id, validator_id) for staking changes
    let mut pairs = HashMap::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        match action.action {
            ActionKind::AddKey | ActionKind::DeleteKey => {
                let public_key = PublicKey::from_str(
                    &action
                        .public_key
                        .as_ref()
                        .expect("Missing PublicKey for AddKey action"),
                )
                .expect("Invalid public key");
                if action.action == ActionKind::AddKey {
                    pairs.insert(
                        PublicKeyPair {
                            account_id: action.account_id.clone(),
                            public_key,
                        },
                        if action.access_key_contract_id.is_none() {
                            PublicKeyUpdateType::AddedFullAccess
                        } else {
                            PublicKeyUpdateType::AddedLimitedAccessKey
                        },
                    );
                } else {
                    pairs.insert(
                        PublicKeyPair {
                            account_id: action.account_id.clone(),
                            public_key,
                        },
                        PublicKeyUpdateType::RemovedKey,
                    );
                }
            }
            ActionKind::Transfer => {
                if action.predecessor_id == "system" {
                    continue;
                }
                let account_id =
                    AccountId::from_str(&action.account_id).expect("Invalid account_id");
                if account_id.is_implicit() {
                    let bytes = hex::decode(&account_id.as_str()).expect("Invalid hex");
                    let public_key = PublicKey::ED25519(bytes.as_slice().try_into().unwrap());
                    pairs.insert(
                        PublicKeyPair {
                            account_id: account_id.to_string(),
                            public_key,
                        },
                        PublicKeyUpdateType::AddedFullAccess,
                    );
                }
            }
            _ => {}
        }
    }

    pairs
}

fn extract_ft_pairs(actions: &[ActionRow], events: &[EventRow]) -> HashSet<PairUpdate> {
    // Extract matching (account_id, token_id) for FT changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &action.account_id;
        if token_id.ends_with(".poolv1.near")
            || token_id.ends_with(".pool.near")
            || token_id.ends_with(".lockup.near")
        {
            continue;
        }
        // Special case for HERE staking contract
        if token_id == "storage.herewallet.near" {
            if let Some(method_name) = action.method_name.as_ref() {
                if ["deposit", "withdraw"].contains(&method_name.as_str()) {
                    pairs.insert(PairUpdate {
                        account_id: action.predecessor_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
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
                pairs.insert(PairUpdate {
                    account_id: action.predecessor_id.clone(),
                    token_id: token_id.clone(),
                });
                if let Some(account_id) = action.args_receiver_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
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
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            } else if event_type == "ft_transfer" {
                if let Some(account_id) = event.data_new_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
                if let Some(account_id) = event.data_old_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
        }
    }

    pairs
}

fn extract_nft_pairs(actions: &[ActionRow], events: &[EventRow]) -> HashSet<PairUpdate> {
    // Extract matching (account_id, token_id) for FT changes
    let mut pairs = HashSet::new();
    for action in actions {
        if action.status != ReceiptStatus::Success {
            continue;
        }
        let token_id = &action.account_id;
        if let Some(method_name) = action.method_name.as_ref() {
            if [
                "nft_transfer_call",
                "nft_transfer",
                "nft_approve",
                "nft_revoke",
                "nft_revoke_all",
                "nft_mint",
                "nft_burn",
            ]
            .contains(&method_name.as_str())
            {
                pairs.insert(PairUpdate {
                    account_id: action.predecessor_id.clone(),
                    token_id: token_id.clone(),
                });
                if let Some(account_id) = action.args_receiver_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
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
            if ["nft_mint", "nft_burn"].contains(&event_type.as_str()) {
                if let Some(account_id) = event.data_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            } else if event_type == "nft_transfer" {
                if let Some(account_id) = event.data_new_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
                if let Some(account_id) = event.data_old_owner_id.as_ref() {
                    pairs.insert(PairUpdate {
                        account_id: account_id.clone(),
                        token_id: token_id.clone(),
                    });
                }
            }
        }
    }

    pairs
}

fn add_pairs_to_update(
    prefix: &str,
    pairs: HashSet<PairUpdate>,
    to_update: &mut HashMap<String, Vec<(String, String)>>,
    block_height: BlockHeight,
) {
    for PairUpdate {
        account_id,
        token_id,
    } in pairs
    {
        to_update
            .entry(format!("{}:{}", prefix, account_id))
            .or_insert_with(Vec::new)
            .push((token_id, block_height.to_string()));
    }
}
