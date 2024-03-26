mod common;
mod redis_db;

use redis_db::RedisDB;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::path::Prefix;
use std::str::FromStr;

use dotenv::dotenv;
use near_crypto::PublicKey;
use near_indexer::near_primitives::types::{AccountId, BlockHeight};
use near_indexer::StreamerMessage;
use tokio::sync::mpsc;
use tracing_subscriber::fmt::format;

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
    let offset: usize = args.get(2).map(|arg| arg.parse().unwrap()).unwrap_or(0);
    let limit: usize = args
        .get(3)
        .map(|arg| arg.parse().unwrap())
        .unwrap_or(usize::MAX);
    let block_height = env::var("BACKFILL_BLOCK_HEIGHT")
        .expect("Missing env BACKFILL_BLOCK_HEIGHT")
        .parse::<BlockHeight>()
        .expect("Failed to parse BACKFILL_BLOCK_HEIGHT");

    if command == "pk" {
        inject_pk(write_redis_db, offset, limit).await;
        return;
    }

    let (prefix, env_name) = match command {
        "ft" => ("ft", "FT_CSV_PATH"),
        "stake" => ("st", "STAKE_CSV_PATH"),
        "nft" => ("nf", "NFT_CSV_PATH"),
        "here" => ("ft", "HERE_CSV_PATH"),
        _ => {
            panic!("Invalid command {:?}, expected ft, nft or stake", command);
        }
    };

    let csv_fn = env::var(env_name).expect(&format!("Missing env {}", env_name));
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(std::fs::File::open(csv_fn).expect("Failed to open the file"));

    let mut account_to_field: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut total_records: usize = 0;
    for (i, result) in csv_reader.records().enumerate() {
        if i % 100000 == 0 {
            tracing::log::info!(target: PROJECT_ID, "Processed {} records", i);
        }
        let record = result.expect("Failed to read the record");
        let account_id = record.get(0).expect("Missing account_id").to_string();
        let field = record.get(1).expect("Missing field").to_string();
        total_records += 1;
        account_to_field
            .entry(account_id)
            .or_insert_with(Vec::new)
            .push((field, block_height.to_string()));
    }

    tracing::log::info!(target: PROJECT_ID, "Going to inject {} accounts and {} records", account_to_field.len(), total_records);

    let mut records = Vec::new();
    let mut cnt = 0;
    let mut total_cnt = 0;
    for (i, (account_id, fields)) in account_to_field
        .into_iter()
        .skip(offset)
        .take(limit)
        .enumerate()
    {
        cnt += fields.len();
        records.push((account_id, fields));
        if i % 10000 == 0 || cnt >= MIN_BATCH {
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

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
enum PublicKeyUpdateType {
    AddedFullAccess,
    AddedLimitedAccessKey,
    RemovedKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
struct PkPair {
    account_id: String,
    public_key: String,
}

#[derive(Debug, Clone)]
struct PkRes {
    block_height: BlockHeight,
    update_type: PublicKeyUpdateType,
}

pub async fn inject_pk(mut write_redis_db: RedisDB, offset: usize, limit: usize) {
    let mut pairs: HashMap<PkPair, PkRes> = HashMap::new();
    let mut csv_reader = csv::ReaderBuilder::new().has_headers(false).from_reader(
        std::fs::File::open(env::var("PK_CSV_PATH").expect("Missing env PK_CSV_PATH"))
            .expect("Failed to open the file"),
    );

    for (i, result) in csv_reader.records().enumerate() {
        if i % 100000 == 0 {
            tracing::log::info!(target: PROJECT_ID, "Processed {} PK", i);
        }
        let record = result.expect("Failed to read the record");
        let account_id = record.get(0).expect("Missing account_id").to_string();
        let public_key = record.get(1).expect("Missing public_key").to_string();
        let action = record.get(2).expect("Missing action").to_string();
        let access_key_contract_id = record
            .get(3)
            .expect("Missing access_key_contract_id")
            .to_string();
        let block_height = record
            .get(4)
            .expect("Missing block_height")
            .parse::<BlockHeight>()
            .expect("Failed to parse block_height");
        let pair = PkPair {
            account_id,
            public_key,
        };
        let res = if action == "ADD_KEY" {
            if access_key_contract_id.is_empty() {
                PkRes {
                    block_height,
                    update_type: PublicKeyUpdateType::AddedFullAccess,
                }
            } else {
                PkRes {
                    block_height,
                    update_type: PublicKeyUpdateType::AddedLimitedAccessKey,
                }
            }
        } else if action == "DELETE_KEY" {
            PkRes {
                block_height,
                update_type: PublicKeyUpdateType::RemovedKey,
            }
        } else {
            unreachable!("Invalid action: {}", action);
        };
        if let Some(prev) = pairs.get(&pair) {
            if prev.block_height < block_height {
                pairs.insert(pair, res);
            }
        } else {
            pairs.insert(pair, res);
        }
    }

    let mut csv_reader = csv::ReaderBuilder::new().has_headers(false).from_reader(
        std::fs::File::open(env::var("IMP_CSV_PATH").expect("Missing env IMP_CSV_PATH"))
            .expect("Failed to open the file"),
    );

    for (i, result) in csv_reader.records().enumerate() {
        if i % 100000 == 0 {
            tracing::log::info!(target: PROJECT_ID, "Processed {} IMP", i);
        }
        let record = result.expect("Failed to read the record");
        let account_id = record.get(0).expect("Missing account_id").to_string();
        let block_height = record
            .get(1)
            .expect("Missing block_height")
            .parse::<BlockHeight>()
            .expect("Failed to parse block_height");
        let account_id = AccountId::from_str(&account_id).expect("Invalid account_id");
        if !account_id.is_implicit() {
            tracing::log::info!(target: PROJECT_ID, "Skipping account_id: {}", account_id);
            continue;
        }
        let bytes = hex::decode(&account_id.as_str()).expect("Invalid hex");
        let public_key = PublicKey::ED25519(bytes.as_slice().try_into().unwrap());
        let pair = PkPair {
            account_id: account_id.to_string(),
            public_key: public_key.to_string(),
        };
        let res = PkRes {
            block_height,
            update_type: PublicKeyUpdateType::AddedFullAccess,
        };
        if let Some(prev) = pairs.get(&pair) {
            if prev.block_height < block_height {
                pairs.insert(pair, res);
            }
        } else {
            pairs.insert(pair, res);
        }
    }

    // Clearing deletes
    pairs.retain(|pair, res| {
        if res.update_type == PublicKeyUpdateType::RemovedKey {
            false
        } else {
            true
        }
    });

    let mut total_records: usize = 0;

    // Group by public key
    let mut account_to_field: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (pair, res) in pairs.into_iter() {
        let field = match res.update_type {
            PublicKeyUpdateType::AddedFullAccess => "f",
            PublicKeyUpdateType::AddedLimitedAccessKey => "l",
            PublicKeyUpdateType::RemovedKey => unreachable!("Should be filtered out"),
        };
        account_to_field
            .entry(pair.public_key)
            .or_insert_with(Vec::new)
            .push((pair.account_id, field.to_string()));
        total_records += 1;
    }

    tracing::log::info!(target: PROJECT_ID, "Going to inject {} accounts and {} records", account_to_field.len(), total_records);

    let prefix: &str = "pk";

    let mut records = Vec::new();
    let mut cnt = 0;
    let mut total_cnt = 0;
    for (i, (public_key, fields)) in account_to_field
        .into_iter()
        .skip(offset)
        .take(limit)
        .enumerate()
    {
        cnt += fields.len();
        records.push((public_key, fields));
        if i % 10000 == 0 || cnt >= MIN_BATCH {
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
