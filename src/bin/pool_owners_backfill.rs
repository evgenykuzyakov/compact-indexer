mod common;
mod redis_db;
mod rpc;

use itertools::Itertools;
use redis_db::RedisDB;
use std::collections::{HashMap, HashSet};
use std::env;

use crate::rpc::{fetch_from_rpc, RpcResultPair, RpcTask};
use dotenv::dotenv;
use tokio::sync::mpsc;
use tracing_subscriber::fmt::format;

const PROJECT_ID: &str = "pool_owners_backfill";
const ST_POOL_INFO_KEY_PREF: &str = "st_pool_info";

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("pool_owners_backfill=info,redis=info,rpc=debug");

    tracing::log::info!(target: PROJECT_ID, "Starting Pool Owners backfill");

    let rpc_config = rpc::RpcConfig::from_env();

    let streamer = redis_streamer();

    let redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    process_owners(streamer, redis_db, &rpc_config).await;
}

fn redis_streamer() -> mpsc::Receiver<Vec<String>> {
    let (sender, receiver) = mpsc::channel(100);
    tokio::spawn(redis_start(sender));
    receiver
}

async fn redis_start(pairs_sync: mpsc::Sender<Vec<String>>) {
    let mut read_redis_db = RedisDB::new(Some(
        env::var("EXPORT_READ_REDIS_URL").expect("Missing env EXPORT_READ_REDIS_URL"),
    ))
    .await;

    let mut delegators = HashSet::new();
    let mut cursor = "0".to_string();
    let mut total_accounts = 0;
    let mut last_multiplier = 0;
    // Get all delegators and pools they staked to
    loop {
        let res: redis::RedisResult<(String, Vec<String>)> =
            with_retries!(read_redis_db, |connection| async {
                redis::cmd("SCAN")
                    .arg(&cursor)
                    .arg("MATCH")
                    .arg("st:*")
                    .arg("COUNT")
                    .arg(1000)
                    .query_async(connection)
                    .await
            });
        let (next_cursor, keys) = res.expect("Failed to scan delegators");
        cursor = next_cursor;
        total_accounts += keys.len();
        delegators.extend(keys);
        let mult = delegators.len() / 1000;
        if last_multiplier < mult {
            last_multiplier = mult;
            tracing::info!(target: PROJECT_ID, "Scanned {} delegators", total_accounts);
        }
        if cursor == "0" {
            break;
        }
    }
    tracing::info!(target: PROJECT_ID, "Total delegators scanned: {}", total_accounts);

    // Collect all pools
    let mut all_pools = HashSet::new();
    for (i, key) in delegators.iter().enumerate() {
        if i % 1000 == 0 {
            tracing::info!(target: PROJECT_ID, "Processed {} delegators out of {}. Total pools to check: {}", i, total_accounts, all_pools.len());
        }
        let res: HashMap<String, String> = with_retries!(read_redis_db, |connection| async {
            redis::cmd("HGETALL").arg(key).query_async(connection).await
        })
        .expect("Failed to get staking pools");

        // (staking_pool, block_height)
        for (pool_id, _) in res {
            if pool_id.ends_with(".poolv1.near")
                || pool_id.ends_with(".pool.near")
                || pool_id.ends_with(".pool.f863973.m0")
            {
                all_pools.insert(pool_id);
            }
        }
    }

    tracing::log::info!(target: PROJECT_ID, "Total pools collected: {}", all_pools.len());

    // NOTE: not sure about processing it by chunks like this - won't this reallocate memory and be
    // slower?
    let all_pools = all_pools.iter().cloned().collect_vec();
    let mut total_pools_to_update = 0;
    let mut pools_to_update = vec![];
    for pools_chunk in all_pools.chunks(1000) {
        let owner_exists: Vec<i64> = with_retries!(read_redis_db, |connection| async {
            let mut pipe = redis::pipe();

            for pool in pools_chunk {
                pipe.cmd("EXISTS")
                    .arg(format!("{}:{}", ST_POOL_INFO_KEY_PREF, pool));
            }

            pipe.query_async(connection).await
        })
        .expect("Failed to check if owner exists for pool");

        for (owner_exists, pool) in owner_exists.iter().zip(pools_chunk) {
            if *owner_exists == 0 {
                total_pools_to_update += 1;
                pools_to_update.push(pool.clone());
                if pools_to_update.len() == 1000 {
                    let mut new_to_update = vec![];
                    std::mem::swap(&mut new_to_update, &mut pools_to_update);
                    pairs_sync
                        .send(new_to_update)
                        .await
                        .expect("Failed to send");
                }
            }
        }
    }

    if !pools_to_update.is_empty() {
        pairs_sync
            .send(pools_to_update)
            .await
            .expect("Failed to send");
    }

    tracing::info!(target: PROJECT_ID, "Total pools to update: {}", total_pools_to_update);
}

async fn process_owners(
    mut stream: mpsc::Receiver<Vec<String>>,
    mut redis_db: RedisDB,
    rpc_config: &rpc::RpcConfig,
) {
    let mut total_pools = 0;
    while let Some(pools) = stream.recv().await {
        total_pools += pools.len();
        update_owners(&mut redis_db, pools, rpc_config).await;
        tracing::info!(target: PROJECT_ID, "Processed {} pools", total_pools);
    }
}

async fn update_owners(redis_db: &mut RedisDB, pools: Vec<String>, rpc_config: &rpc::RpcConfig) {
    let mut tasks = vec![];

    tasks.extend(pools.iter().map(|pool| RpcTask::Custom {
        block_height: None,
        account_id: pool.clone(),
        method_name: "get_owner_id".to_string(),
        args: "".to_string(),
    }));

    let results = fetch_from_rpc(&tasks, rpc_config)
        .await
        .expect("Failed to fetch updates from the RPC");

    let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();

        for RpcResultPair { task, result } in &results {
            if result.is_none() {
                tracing::info!(target: PROJECT_ID, "No result for task {:?}", task);
                continue;
            }
            let pool_id = match task {
                RpcTask::Custom { account_id, .. } => account_id,
                _ => unreachable!(),
            };

            let owner_id = result.as_ref().unwrap().unwrap_as_custom().as_str();

            if owner_id.is_none() {
                tracing::info!(target: PROJECT_ID, "No owner_id for pool_id {}", pool_id);
                continue;
            }

            let owner_id = owner_id.unwrap();

            // Set the pool_owner for pool_id
            // (st_pool_info of `pool_id` -> `owner_id:account_id`, `latest_stake_block:block_height`)
            pipe.cmd("HSET")
                .arg(format!("{}:{}", ST_POOL_INFO_KEY_PREF, pool_id))
                .arg("owner_id")
                .arg(owner_id)
                .arg("latest_stake_block")
                .arg("0")
                .ignore();

            // Add owner as automatically staked to this pool
            // TODO: add adequate block height
            pipe.cmd("HSET")
                .arg(format!("st:{}", owner_id))
                .arg(pool_id)
                .arg("0")
                .ignore();
        }

        pipe.query_async(connection).await
    });
    res.expect("Failed to update");
}
