mod click;
mod common;
mod redis_db;
mod rpc;

use clap::builder::Str;
use redis_db::RedisDB;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;

use crate::rpc::RpcAccountStateResult;
use crate::rpc::RpcTask::AccountState;
use dotenv::dotenv;
use itertools::Itertools;
use near_crypto::PublicKey;
use near_indexer::near_primitives::serialize::dec_format;
use near_indexer::near_primitives::types::{AccountId, BlockHeight};
use serde::{Deserialize, Serialize};
use serde_json::json;

const PROJECT_ID: &str = "data_export";

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub account_id: String,
    pub tokens: Vec<(String, String)>,
    #[serde(with = "dec_format")]
    pub near_balance: u128,
    pub staking_pools: Vec<(String, String)>,
    pub burrow_balances: Vec<(String, String)>,
}

impl Account {
    pub fn new(account_id: String) -> Self {
        Self {
            account_id,
            tokens: vec![],
            near_balance: 0,
            staking_pools: vec![],
            burrow_balances: vec![],
        }
    }
}

#[tokio::main]
async fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    common::setup_tracing("data_export=info,redis=info");

    tracing::log::info!(target: PROJECT_ID, "Starting Data Export");

    let mut read_redis_db = RedisDB::new(Some(
        env::var("EXPORT_READ_REDIS_URL").expect("Missing env EXPORT_READ_REDIS_URL"),
    ))
    .await;

    let args: Vec<String> = std::env::args().collect();
    let ft_token_id = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You fungible token ID");

    // Extract all token holder accounts
    let res: redis::RedisResult<Vec<(String, String)>> =
        with_retries!(read_redis_db, |connection| async {
            redis::cmd("HGETALL")
                .arg(format!("b:{}", &ft_token_id))
                .query_async(connection)
                .await
        });
    let account_ids: Vec<(String, String)> = res.expect("Failed to get token holders");
    // Store accounts into a file
    std::fs::write(
        "res/account_ids.txt",
        account_ids
            .iter()
            .map(|(a, b)| format!("{} {}", a, b))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("Failed to write accounts");
    tracing::info!(target: PROJECT_ID, "Extracted {} accounts", account_ids.len());
    // Extract their tokens
    let mut all_token_pairs = vec![];
    for chunk in account_ids.chunks(100000) {
        let res: redis::RedisResult<Vec<Vec<(String, String)>>> =
            with_retries!(read_redis_db, |connection| async {
                let mut pipe = redis::pipe();
                for (account_id, _) in chunk {
                    pipe.cmd("HGETALL").arg(format!("ft:{}", &account_id));
                }
                pipe.query_async(connection).await
            });
        let tokens = res.expect("Failed to get balances");
        for (pairs, (account_id, _)) in tokens.iter().zip(chunk) {
            all_token_pairs.extend(
                pairs
                    .iter()
                    .map(|(token_id, _block_height)| (account_id.clone(), token_id.clone())),
            );
        }
    }
    // Store tokens into a file
    std::fs::write(
        "res/token_pairs.txt",
        all_token_pairs
            .iter()
            .map(|(a, b)| format!("{} {}", a, b))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("Failed to write token pairs");
    tracing::info!(target: PROJECT_ID, "Extracted {} token pairs", all_token_pairs.len());
    // Extracting token balances
    let mut all_balances = vec![];
    for chunk in all_token_pairs.chunks(100000) {
        let res: redis::RedisResult<Vec<Option<String>>> =
            with_retries!(read_redis_db, |connection| async {
                let mut pipe = redis::pipe();
                for (account_id, token_id) in chunk {
                    pipe.cmd("HGET")
                        .arg(format!("b:{}", token_id))
                        .arg(account_id);
                }
                pipe.query_async(connection).await
            });
        let balances = res.expect("Failed to get balances");
        for (balance, pair) in balances.iter().zip(chunk) {
            if let Some(balance) = balance {
                all_balances.push((pair.clone(), balance.clone()));
            }
        }
    }
    std::fs::write(
        "res/balances.txt",
        all_balances
            .iter()
            .map(|((a, b), c)| format!("{} {} {}", a, b, c))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("Failed to write balances");
    tracing::info!(target: PROJECT_ID, "Extracted {} balances", all_balances.len());
    // Group token balances by account_id
    let mut accounts = HashMap::new();
    for (pair, balance) in all_balances {
        accounts
            .entry(pair.0.clone())
            .or_insert_with(|| Account::new(pair.0.clone()))
            .tokens
            .push((pair.1.clone(), balance));
    }
    tracing::info!(target: PROJECT_ID, "Grouped {} accounts", accounts.len());
    std::fs::write(
        "res/accounts.json",
        serde_json::to_string(&accounts).expect("Failed to serialize accounts"),
    )
    .expect("Failed to write accounts");
    // Extract account state
    let mut num_account_states = 0;
    for chunk in account_ids.chunks(100000) {
        let res: redis::RedisResult<Vec<Option<String>>> =
            with_retries!(read_redis_db, |connection| async {
                let mut pipe = redis::pipe();
                for (account_id, _) in chunk {
                    pipe.cmd("HGET").arg("accounts").arg(account_id);
                }
                pipe.query_async(connection).await
            });
        let account_states = res.expect("Failed to get account states");
        for (account_state, (account_id, _)) in account_states.iter().zip(chunk) {
            if let Some(account_state) = account_state {
                if let Ok(account_state) =
                    serde_json::from_str::<RpcAccountStateResult>(&account_state)
                {
                    num_account_states += 1;
                    accounts
                        .entry(account_id.clone())
                        .or_insert_with(|| Account::new(account_id.clone()))
                        .near_balance = account_state.balance;
                }
            }
        }
    }
    tracing::info!(target: PROJECT_ID, "Extracted {} account states", num_account_states);
    std::fs::write(
        "res/accounts.json",
        serde_json::to_string(&accounts).expect("Failed to serialize accounts"),
    )
    .expect("Failed to write accounts");

    // Extract their staking pools, then for each extract their staked balance
    let mut all_staking_pools = vec![];
    for chunk in account_ids.chunks(100000) {
        let res: redis::RedisResult<Vec<Vec<(String, String)>>> =
            with_retries!(read_redis_db, |connection| async {
                let mut pipe = redis::pipe();
                for (account_id, _) in chunk {
                    pipe.cmd("HGETALL").arg(format!("st:{}", &account_id));
                }
                pipe.query_async(connection).await
            });
        let staking_pools = res.expect("Failed to get staking pools");
        for (pairs, (account_id, _)) in staking_pools.iter().zip(chunk) {
            all_staking_pools.extend(pairs.iter().map(|(staking_pool_id, _block_height)| {
                (account_id.clone(), staking_pool_id.clone())
            }));
        }
    }
    std::fs::write(
        "res/staking_pools.txt",
        all_staking_pools
            .iter()
            .map(|(a, b)| format!("{} {}", a, b))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("Failed to write staking pools");
    tracing::info!(target: PROJECT_ID, "Extracted {} staking pools", all_staking_pools.len());

    // Extract all burrow accounts, match accounts with burrow accounts
    // Extract NFTs
}
