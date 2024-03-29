use base64::prelude::*;
use near_indexer::near_primitives::types::BlockHeight;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;

const RPC_TIMEOUT: Duration = Duration::from_millis(5000);
const TARGET_RPC: &str = "rpc";

#[derive(Debug)]
pub enum RpcError {
    ReqwestError(reqwest::Error),
    InvalidFunctionCallResponse(serde_json::Error),
}

impl From<reqwest::Error> for RpcError {
    fn from(error: reqwest::Error) -> Self {
        RpcError::ReqwestError(error)
    }
}

#[derive(Serialize)]
struct JsonRequest {
    jsonrpc: String,
    method: String,
    params: Value,
    id: String,
}

#[derive(Deserialize)]
struct JsonResponse {
    // id: String,
    // jsonrpc: String,
    result: Option<Value>,
    // error: Option<Value>,
}

#[derive(Deserialize)]
struct FunctionCallResponse {
    // block_hash: String,
    // block_height: u64,
    result: Option<Vec<u8>>,
    // error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PairBalanceUpdate {
    pub account_id: String,
    pub token_id: String,
    pub balance: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub rpcs: Vec<String>,
    pub concurrency: usize,
    pub bearer_token: Option<String>,
    pub timeout: Duration,
}

impl RpcConfig {
    pub fn from_env() -> Self {
        let config = RpcConfig {
            rpcs: env::var("RPCS")
                .expect("Missing env RPCS")
                .split(",")
                .map(|s| s.to_string())
                .collect(),
            concurrency: env::var("RPC_CONCURRENCY")
                .unwrap_or("100".to_string())
                .parse()
                .unwrap(),
            bearer_token: env::var("RPC_BEARER_TOKEN").ok(),
            timeout: env::var("RPC_TIMEOUT")
                .map(|s| Duration::from_millis(s.parse().unwrap()))
                .unwrap_or(RPC_TIMEOUT),
        };
        assert!(config.concurrency > 0);
        assert!(config.rpcs.len() > 0);
        config
    }
}

pub async fn get_ft_balances(
    pairs: &[String],
    block_height: Option<BlockHeight>,
    rpc_config: &RpcConfig,
) -> Result<Vec<PairBalanceUpdate>, RpcError> {
    let mut balances = Vec::new();
    if pairs.is_empty() {
        return Ok(balances);
    }
    let start = std::time::Instant::now();
    let client = Client::new();
    let (tx, mut rx) = mpsc::channel::<Result<PairBalanceUpdate, RpcError>>(rpc_config.concurrency);
    let rpcs = &rpc_config.rpcs;

    for (i, pair) in pairs.iter().enumerate() {
        let client = client.clone();
        let tx = tx.clone();
        let (token_id, account_id) = pair.split_once(':').unwrap();
        let rpcs = rpcs.clone();
        let token_id = token_id.to_string();
        let account_id = account_id.to_string();
        let bearer_token = rpc_config.bearer_token.clone();
        let timeout = rpc_config.timeout;

        // Spawn a new asynchronous task for each request
        task::spawn(async move {
            let mut index = i;
            let mut iterations = rpcs.len();
            let res = loop {
                let url = &rpcs[index % rpcs.len()];
                index += 1;
                let res = get_ft_balance(
                    &client,
                    &url,
                    &token_id,
                    &account_id,
                    block_height,
                    &bearer_token,
                    timeout,
                )
                .await;

                match res {
                    Ok(balance) => {
                        break Ok(PairBalanceUpdate {
                            account_id: account_id.clone(),
                            token_id: token_id.clone(),
                            balance,
                        });
                    }
                    Err(e) => {
                        tracing::warn!(target: TARGET_RPC, "RPC Error: {:?}", e);
                        // Need to retry this task
                        iterations -= 1;
                        if iterations == 0 {
                            break Err(e);
                        }
                    }
                }
            };
            tx.send(res).await.expect("Failed to send balance");
        });
    }

    // Close the sender to ensure the loop below exits once all tasks are completed
    drop(tx);

    let mut errors = Vec::new();
    // Wait for all tasks to complete
    while let Some(res) = rx.recv().await {
        match res {
            Ok(pair) => balances.push(pair),
            Err(e) => {
                errors.push(e);
            }
        }
    }
    let duration = start.elapsed().as_millis();

    tracing::debug!(target: TARGET_RPC, "Query {}ms: get_ft_balances {} pairs",
        duration,
        pairs.len());

    if let Some(err) = errors.pop() {
        return Err(err);
    }

    Ok(balances)
}

async fn get_ft_balance(
    client: &Client,
    url: &String,
    token_id: &String,
    account_id: &String,
    block_height: Option<BlockHeight>,
    bearer_token: &Option<String>,
    timeout: Duration,
) -> Result<Option<String>, RpcError> {
    let mut params = json!({
        "request_type": "call_function",
        "account_id": token_id,
        "method_name": "ft_balance_of",
        "args_base64": BASE64_STANDARD.encode(format!("{{\"account_id\": \"{}\"}}", account_id)),
    });
    if let Some(block_height) = block_height {
        params["block_id"] = json!(block_height);
    } else {
        params["finality"] = json!("final");
    }
    let request = JsonRequest {
        jsonrpc: "2.0".to_string(),
        method: "query".to_string(),
        params,
        id: "0".to_string(),
    };
    let mut response = client.post(url);
    if let Some(bearer) = bearer_token {
        response = response.bearer_auth(bearer);
    }
    let response = response.json(&request).timeout(timeout).send().await?;
    let response = response.json::<JsonResponse>().await?;
    let balance = if let Some(res) = response.result {
        let fc: FunctionCallResponse =
            serde_json::from_value(res).map_err(|e| RpcError::InvalidFunctionCallResponse(e))?;
        fc.result.and_then(|result| {
            let balance: Option<String> = serde_json::from_slice(&result).ok();
            let parsed_balance: Option<u128> = balance.and_then(|s| s.parse().ok());
            parsed_balance.map(|b| b.to_string())
        })
    } else {
        None
    };
    Ok(balance)
}
