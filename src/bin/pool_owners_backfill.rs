mod common;
mod redis_db;
mod rpc;

use fastnear_primitives::near_primitives::types::BlockHeight;
use itertools::Itertools;
use redis_db::RedisDB;
use std::collections::HashMap;
use std::env;

use crate::rpc::{fetch_from_rpc, RpcResultPair, RpcTask};
use dotenv::dotenv;

const PROJECT_ID: &str = "pool_owners_backfill";

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("pool_owners_backfill=info,redis=info,rpc=debug");

    tracing::log::info!(target: PROJECT_ID, "Starting Pool Owners backfill");

    let rpc_config = rpc::RpcConfig::from_env();

    let read_redis_db = RedisDB::new(Some(
        env::var("EXPORT_READ_REDIS_URL").expect("Missing env EXPORT_READ_REDIS_URL"),
    ))
    .await;

    let write_redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    process_owners(read_redis_db, write_redis_db, &rpc_config).await;
}

async fn process_owners(
    mut read_redis_db: RedisDB,
    mut write_redis_db: RedisDB,
    rpc_config: &rpc::RpcConfig,
) {
    let delegators_list = scan_all_delegators(&mut read_redis_db).await;

    let pools_to_process =
        collect_pools_from_delegators(&mut read_redis_db, &delegators_list).await;

    let owner_pools = fetch_pool_owners(&mut read_redis_db, &pools_to_process, rpc_config).await;

    let owners_to_update = fetch_missing_owner_stakes(&mut read_redis_db, &owner_pools).await;

    update_owner_stakes(&mut write_redis_db, &owners_to_update, &pools_to_process).await;
}

async fn scan_all_delegators(read_redis_db: &mut RedisDB) -> Vec<String> {
    let mut delegators_list = vec![];
    let mut total_accounts = 0;
    let mut cursor = "0".to_string();
    let mut last_multiplier = 0;

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

        let (next_cursor, delegators) = res.expect("Failed to scan delegators");
        cursor = next_cursor;
        total_accounts += delegators.len();
        delegators_list.extend(delegators);

        let mult = delegators_list.len() / 1000;
        if last_multiplier < mult {
            last_multiplier = mult;
            tracing::info!(target: PROJECT_ID, "Scanned {} delegators", total_accounts);
        }
        if cursor == "0" {
            break;
        }
    }
    tracing::info!(target: PROJECT_ID, "Total delegators scanned: {}", total_accounts);

    delegators_list
}

async fn collect_pools_from_delegators(
    read_redis_db: &mut RedisDB,
    delegators_list: &[String],
) -> HashMap<String, BlockHeight> {
    let mut pools_to_process: HashMap<String, BlockHeight> = HashMap::new();
    let total_accounts = delegators_list.len();

    for (i, key) in delegators_list.iter().enumerate() {
        if i % 1000 == 0 {
            tracing::info!(target: PROJECT_ID, "Processed {} delegators out of {}. Total pools to check: {}", i, total_accounts, pools_to_process.len());
        }
        let delegator_staking_pools: HashMap<String, String> =
            with_retries!(read_redis_db, |connection| async {
                redis::cmd("HGETALL").arg(key).query_async(connection).await
            })
            .expect("Failed to get staking pools");

        for (pool_id, block_height) in delegator_staking_pools {
            let curr_block_height: BlockHeight = block_height.parse().unwrap_or(0);

            if pool_id.ends_with(".poolv1.near")
                || pool_id.ends_with(".pool.near")
                || pool_id.ends_with(".pool.f863973.m0")
            {
                pools_to_process
                    .entry(pool_id)
                    .and_modify(|block_height_entry| {
                        if curr_block_height < *block_height_entry {
                            *block_height_entry = curr_block_height;
                        }
                    })
                    .or_insert(curr_block_height);
            }
        }
    }

    tracing::log::info!(target: PROJECT_ID, "Total pools collected: {}", pools_to_process.len());

    pools_to_process
}

async fn fetch_pool_owners(
    _read_redis_db: &mut RedisDB,
    pools_to_process: &HashMap<String, BlockHeight>,
    rpc_config: &rpc::RpcConfig,
) -> HashMap<String, Vec<String>> {
    let mut owner_pools: HashMap<String, Vec<String>> = HashMap::new();

    for pools_chunk in pools_to_process.keys().cloned().collect_vec().chunks(1000) {
        let mut tasks = vec![];

        tasks.extend(pools_chunk.iter().map(|pool| RpcTask::Custom {
            block_height: None,
            account_id: pool.clone(),
            method_name: "get_owner_id".to_owned(),
            args: "".to_owned(),
        }));

        let rpc_result_owners = fetch_from_rpc(&tasks, rpc_config)
            .await
            .expect("Failed to fetch owners from RPC");

        for RpcResultPair { task, result } in rpc_result_owners {
            let pool = match task {
                RpcTask::Custom { account_id, .. } => account_id,
                _ => unreachable!(),
            };

            let owner_id = result.as_ref().unwrap().unwrap_as_custom().as_str();
            if owner_id.is_none() {
                tracing::info!(target: PROJECT_ID, "No owner_id for pool_id {}", pool);
                continue;
            }
            let owner_id = owner_id.unwrap();

            owner_pools
                .entry(owner_id.to_string())
                .and_modify(|owner_pools| owner_pools.push(pool.clone()))
                .or_insert(vec![pool]);
        }
    }

    tracing::log::info!(target: PROJECT_ID, "Total owner collected: {}", owner_pools.len());

    owner_pools
}

async fn fetch_missing_owner_stakes(
    read_redis_db: &mut RedisDB,
    owner_pools: &HashMap<String, Vec<String>>,
) -> Vec<(String, String)> {
    let mut owners_to_update = vec![];

    for owner_chunk in owner_pools.iter().collect_vec().chunks(100) {
        let check_results: Vec<Option<String>> = with_retries!(read_redis_db, |connection| async {
            let mut pipe = redis::pipe();

            for (owner, pools_vec) in owner_chunk {
                for pool in *pools_vec {
                    pipe.cmd("HGET").arg(format!("st:{}", owner)).arg(pool);
                }
            }

            pipe.query_async(connection).await
        })
        .expect("Failed to batch check pools");

        let mut result_idx = 0;
        for (owner, pools_vec) in owner_chunk {
            for pool in *pools_vec {
                if check_results[result_idx].is_none() {
                    owners_to_update.push(((*owner).clone(), (*pool).clone()));
                }
                result_idx += 1;
            }
        }
    }
    tracing::log::info!(target: PROJECT_ID, "Total owners to process: {}", owners_to_update.len());

    owners_to_update
}

async fn update_owner_stakes(
    write_redis_db: &mut RedisDB,
    owners_to_update: &[(String, String)],
    pools_to_process: &HashMap<String, BlockHeight>,
) {
    let res: redis::RedisResult<()> = with_retries!(write_redis_db, |connection| async {
        let mut pipe = redis::pipe();

        for (owner, pool) in owners_to_update {
            if let Some(block_height) = pools_to_process.get(pool) {
                pipe.cmd("HSET")
                    .arg(format!("st:{}", owner))
                    .arg(pool)
                    .arg(block_height)
                    .ignore();
            }
        }

        pipe.query_async(connection).await
    });
    res.expect("Failed to update");
}
