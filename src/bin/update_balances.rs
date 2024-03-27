mod common;
mod redis_db;
mod rpc;

use redis_db::RedisDB;
use std::env;

use crate::rpc::{get_ft_balances, PairBalanceUpdate};
use dotenv::dotenv;
use near_indexer::near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};

const PROJECT_ID: &str = "update_balances";

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PairUpdate {
    account_id: String,
    token_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FtUpdate {
    pub block_height: BlockHeight,
    pub pairs: Vec<String>,
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("update_balances=info,redis=info,rpc=debug");

    tracing::log::info!(target: PROJECT_ID, "Starting Balance updater");

    let mut redis_db = RedisDB::new(Some(
        env::var("WRITE_REDIS_URL").expect("Missing env WRITE_REDIS_URL"),
    ))
    .await;

    loop {
        let ft_updates: redis::RedisResult<Option<String>> =
            with_retries!(redis_db, |connection| async {
                redis::cmd("LPOP")
                    .arg("ft_updates")
                    .query_async(connection)
                    .await
            });
        let ft_updates = ft_updates.expect("Failed to get ft_updates");
        if let Some(ft_updates) = ft_updates {
            let ft_updates: FtUpdate = serde_json::from_str(&ft_updates).expect("Invalid JSON");
            update_balances(&mut redis_db, ft_updates).await;
        } else {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}

async fn update_balances(redis_db: &mut RedisDB, ft_update: FtUpdate) {
    // Save pending update
    {
        let path = env::var("PENDING_UPDATE_FN").expect("PENDING_UPDATE_FN is not set");
        let f = std::fs::File::create(path).expect("Failed to create pending update file");
        serde_json::to_writer(f, &ft_update).expect("Failed to write pending update");
    }
    // Fetching balances
    let balances = get_ft_balances(&ft_update.pairs, ft_update.block_height)
        .await
        .expect("Failed to get balances");

    // Save balances to redis
    let res: redis::RedisResult<()> = with_retries!(redis_db, |connection| async {
        let mut pipe = redis::pipe();
        for PairBalanceUpdate {
            account_id,
            token_id,
            balance,
        } in &balances
        {
            pipe.cmd("HSET")
                .arg(format!("b:{}", token_id))
                .arg(account_id)
                .arg(balance.as_ref().map(|s| s.as_str()).unwrap_or(""))
                .ignore();
        }

        pipe.cmd("SET")
            .arg("meta:latest_balance_block")
            .arg(ft_update.block_height)
            .ignore();

        pipe.query_async(connection).await
    });
    res.expect("Failed to update");

    tracing::info!(target: PROJECT_ID, "Updated {} balances for block {}", balances.len(), ft_update.block_height);

    // Delete pending update
    {
        let path = env::var("PENDING_UPDATE_FN").expect("PENDING_UPDATE_FN is not set");
        std::fs::remove_file(path).expect("Failed to remove pending update file");
    }
}
