mod common;
mod redis_db;
mod rpc;

use redis_db::RedisDB;
use std::collections::HashSet;
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

pub struct Config {
    pub max_top_holders_count: u64,
    pub rpc_config: rpc::RpcConfig,
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

    let config = Config {
        max_top_holders_count: env::var("MAX_TOP_HOLDERS_COUNT")
            .unwrap_or("1000".to_string())
            .parse()
            .expect("Invalid MAX_TOP_HOLDERS_COUNT"),
        rpc_config: rpc::RpcConfig::from_env(),
    };
    assert!(config.max_top_holders_count <= i64::MAX as u64);

    let path = env::var("PENDING_UPDATE_FN").expect("PENDING_UPDATE_FN is not set");
    if std::path::Path::new(&path).exists() {
        let f = std::fs::File::open(path).expect("Failed to open pending update file");
        let ft_update: FtUpdate =
            serde_json::from_reader(f).expect("Failed to read pending update");
        update_balances(&mut redis_db, ft_update, &config).await;
    }

    loop {
        let response: redis::RedisResult<(String, String)> =
            with_retries!(redis_db, |connection| async {
                redis::cmd("BLPOP")
                    .arg("ft_updates")
                    .arg(0)
                    .query_async(connection)
                    .await
            });
        let (_key_name, s) = response.expect("Failed to get ft_updates");
        let ft_updates: FtUpdate = serde_json::from_str(&s).expect("Invalid JSON");
        update_balances(&mut redis_db, ft_updates, &config).await;
    }
}

async fn update_balances(redis_db: &mut RedisDB, ft_update: FtUpdate, config: &Config) {
    // Save pending update
    {
        let path = env::var("PENDING_UPDATE_FN").expect("PENDING_UPDATE_FN is not set");
        let f = std::fs::File::create(path).expect("Failed to create pending update file");
        serde_json::to_writer(f, &ft_update).expect("Failed to write pending update");
    }
    // Fetching balances
    let balances = get_ft_balances(
        &ft_update.pairs,
        Some(ft_update.block_height),
        &config.rpc_config,
    )
    .await
    .expect("Failed to get balances");

    let all_tokens: HashSet<String> = balances.iter().map(|b| b.token_id.clone()).collect();
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

            if let Some(balance) = balance {
                pipe.cmd("ZADD")
                    .arg(format!("tb:{}", token_id))
                    .arg(balance)
                    .arg(account_id)
                    .ignore();
            }
        }

        // Keeping sorted sets lengths to a fixed size
        for token_id in &all_tokens {
            pipe.cmd("ZREMRANGEBYRANK")
                .arg(format!("tb:{}", token_id))
                .arg(0)
                .arg(-(config.max_top_holders_count as i64 + 1))
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
