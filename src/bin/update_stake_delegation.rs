mod common;
mod redis_db;
mod rpc;

use common::EPOCH_DURATION;
use fastnear_primitives::near_primitives::types::EpochHeight;
use redis_db::RedisDB;
use std::collections::HashMap;
use std::env;

use crate::rpc::*;
use dotenv::dotenv;
use fastnear_primitives::near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};

const PROJECT_ID: &str = "update_stake_delegation";

#[derive(Debug, Deserialize, Serialize)]
pub struct BlockUpdate {
    pub block_height: BlockHeight,
    pub st_pools: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StakingFieldOwnerUpdate {
    pub staking_pool: String,
    pub owner_id: String,
    pub internal_restake_block_height: u64,
}

#[derive(Debug)]
pub struct StakingPoolData {
    pub latest_epoch: EpochHeight,
    pub latest_interaction_block: BlockHeight,
    pub first_interaction_block_in_epoch: BlockHeight,
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("update_stake_delegation=info,redis=info,rpc=debug");

    tracing::log::info!(target: PROJECT_ID, "Starting Automatic Delegation updater");

    let mut redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    let rpc_config = rpc::RpcConfig::from_env();

    let stake_delegation_batch_size: Option<usize> = env::var("STAKE_DELEGATION_BATCH_SIZES")
        .ok()
        .and_then(|s| s.parse().ok());

    if let Some(batch_size) = stake_delegation_batch_size {
        if batch_size > 1 {
            tracing::info!(target: PROJECT_ID, "Backfill using batch size: {}", batch_size);
            loop {
                let response: redis::RedisResult<Vec<String>> =
                    with_retries!(redis_db, |connection| async {
                        redis::cmd("LRANGE")
                            .arg("st_updates")
                            .arg("0")
                            .arg(batch_size - 1)
                            .query_async(connection)
                            .await
                    });
                let updates: Vec<String> = response.expect("Failed to get st_updates");
                let updates: Vec<BlockUpdate> = updates
                    .into_iter()
                    .map(|s| serde_json::from_str(&s).expect("Invalid JSON"))
                    .collect();
                let count = updates.len();
                if !updates.is_empty() {
                    update_auto_delegation(&mut redis_db, updates, &rpc_config).await;
                }
                if count < batch_size {
                    tracing::info!(target: PROJECT_ID, "Backfill completed");
                    break;
                }
            }
        }
    }

    loop {
        let response: redis::RedisResult<String> = with_retries!(redis_db, |connection| async {
            redis::cmd("BLMOVE")
                .arg("st_updates")
                .arg("st_updates")
                .arg("LEFT")
                .arg("LEFT")
                .arg(0)
                .query_async(connection)
                .await
        });
        let s = response.expect("Failed to get st_updates");
        let st_updates: BlockUpdate = serde_json::from_str(&s).expect("Invalid JSON");
        update_auto_delegation(&mut redis_db, vec![st_updates], &rpc_config).await;
    }
}

async fn update_auto_delegation(
    redis_db: &mut RedisDB,
    block_updates: Vec<BlockUpdate>,
    _config: &RpcConfig,
) {
    if block_updates.is_empty() {
        tracing::info!(target: PROJECT_ID, "No block updates to process");
        return;
    }
    let count = block_updates.len();
    let first_block_height = block_updates.first().unwrap().block_height;
    let last_block_height = block_updates.last().unwrap().block_height;
    let mut unique_pools: HashMap<String, StakingPoolData> = HashMap::new();

    for block in &block_updates {
        let curr_epoch = block.block_height / EPOCH_DURATION;

        for pool in &block.st_pools {
            unique_pools
                .entry(pool.clone())
                .and_modify(|entry| {
                    if entry.latest_epoch < curr_epoch {
                        entry.latest_epoch = curr_epoch;
                        entry.first_interaction_block_in_epoch = block.block_height;
                    }

                    entry.latest_interaction_block = block.block_height;
                })
                .or_insert(StakingPoolData {
                    latest_epoch: curr_epoch,
                    latest_interaction_block: block.block_height,
                    first_interaction_block_in_epoch: block.block_height,
                });
        }
    }

    // Collect owner_stake delegation
    let mut owners_to_update: Vec<StakingFieldOwnerUpdate> = vec![];
    for (staking_pool, data) in &unique_pools {
        let res: redis::RedisResult<Vec<Option<String>>> =
            with_retries!(redis_db, |connection| async {
                let mut pipe = redis::pipe();
                pipe.cmd("HGET")
                    .arg(format!("st_pool_info:{}", staking_pool))
                    .arg("owner_id");

                pipe.cmd("HGET")
                    .arg(format!("st_pool_info:{}", staking_pool))
                    .arg("latest_stake_block");

                pipe.query_async(connection).await
            });

        let vals = res.expect("Failed to get owner_id and latest_stake_block for pool");

        let owner_id = vals[0].clone().unwrap_or_default();
        let latest_stake_block: BlockHeight = vals[1]
            .clone()
            .unwrap_or_else(|| "0".to_string())
            .parse()
            .unwrap_or(0);

        let latest_interaction_epoch = latest_stake_block / EPOCH_DURATION;
        if latest_interaction_epoch < data.latest_epoch {
            owners_to_update.push(StakingFieldOwnerUpdate {
                staking_pool: staking_pool.to_string(),
                owner_id,
                internal_restake_block_height: data.first_interaction_block_in_epoch,
            });
        }
    }

    // Save balances to redis
    let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();
        pipe.cmd("LPOP").arg("st_updates").arg(count).ignore();

        // update latest_stake_block
        for (
            pool,
            StakingPoolData {
                latest_interaction_block,
                ..
            },
        ) in &unique_pools
        {
            pipe.cmd("HSET")
                .arg(format!("st_pool_info:{}", pool))
                .arg("latest_stake_block")
                .arg(latest_interaction_block.to_string())
                .ignore();
        }

        // update owner automatic stake delegation
        for StakingFieldOwnerUpdate {
            staking_pool,
            owner_id,
            internal_restake_block_height,
        } in &owners_to_update
        {
            pipe.cmd("HSET")
                .arg(format!("st:{}", owner_id))
                .arg(staking_pool)
                .arg(internal_restake_block_height.to_string())
                .ignore();
        }

        pipe.query_async(connection).await
    });
    res.expect("Failed to update");

    tracing::info!(target: PROJECT_ID,
        "Updated {} pools and {} owners for {}",
        unique_pools.len(),
        owners_to_update.len(),
        if block_updates.len() > 1 { format!("blocks {}-{}", first_block_height, last_block_height) } else { format!("block {}", first_block_height) }
    );
}
