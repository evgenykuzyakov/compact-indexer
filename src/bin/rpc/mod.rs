use base64::prelude::*;
use near_indexer::near_primitives::serialize::dec_format;
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
const RPC_ERROR_UNKNOWN_BLOCK: &str = "UNKNOWN_BLOCK";

#[derive(Debug)]
pub enum RpcError {
    ReqwestError(reqwest::Error),
    InvalidFunctionCallResponse(serde_json::Error),
    InvalidAccountStateResponse(serde_json::Error),
    UnknownBlock,
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
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcErrorCause {
    // info: Option<Value>,
    name: Option<String>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    cause: Option<JsonRpcErrorCause>,
    // code: i64,
    // data: Option<String>,
}

#[derive(Deserialize)]
struct FunctionCallResponse {
    // block_hash: String,
    // block_height: u64,
    result: Option<Vec<u8>>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct AccountStateResponse {
    #[serde(with = "dec_format")]
    amount: u128,
    #[serde(with = "dec_format")]
    locked: u128,
    storage_usage: u64,
    // error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub rpcs: Vec<String>,
    pub concurrency: usize,
    pub bearer_token: Option<String>,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub enum RpcTask {
    FtPair {
        block_height: Option<BlockHeight>,
        account_id: String,
        token_id: String,
    },
    AccountState {
        block_height: Option<BlockHeight>,
        account_id: String,
    },
}

#[derive(Debug, Clone)]
pub struct RpcFtPairResult {
    pub balance: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcAccountStateResult {
    #[serde(with = "dec_format")]
    #[serde(rename = "b")]
    pub balance: u128,
    #[serde(with = "dec_format")]
    #[serde(rename = "l")]
    pub locked: u128,
    #[serde(rename = "s")]
    pub storage_bytes: u64,
}

#[derive(Debug, Clone)]
pub enum RpcTaskResult {
    FtPair(RpcFtPairResult),
    AccountState(RpcAccountStateResult),
}

impl RpcTaskResult {
    pub fn unwrap_as_ft_pair(&self) -> &RpcFtPairResult {
        match &self {
            RpcTaskResult::FtPair(r) => r,
            _ => panic!("Not FtPair"),
        }
    }

    pub fn unwrap_as_account_state(&self) -> &RpcAccountStateResult {
        match &self {
            RpcTaskResult::AccountState(r) => r,
            _ => panic!("Not AccountState"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RpcResultPair {
    pub task: RpcTask,
    pub result: Option<RpcTaskResult>,
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

pub async fn fetch_from_rpc(
    tasks: &[RpcTask],
    rpc_config: &RpcConfig,
) -> Result<Vec<RpcResultPair>, RpcError> {
    let mut results = Vec::new();
    if tasks.is_empty() {
        return Ok(results);
    }
    let start = std::time::Instant::now();
    let client = Client::new();
    let (tx, mut rx) = mpsc::channel::<Result<RpcResultPair, RpcError>>(rpc_config.concurrency);
    let rpcs = &rpc_config.rpcs;

    for (i, task) in tasks.iter().enumerate() {
        let client = client.clone();
        let tx = tx.clone();
        let rpcs = rpcs.clone();
        let task = task.clone();
        let bearer_token = rpc_config.bearer_token.clone();
        let timeout = rpc_config.timeout;

        // Spawn a new asynchronous task for each request
        task::spawn(async move {
            let mut index = i;
            let mut iterations = rpcs.len();
            let mut sleep = Duration::from_millis(100);
            let res = loop {
                let url = &rpcs[index % rpcs.len()];
                index += 1;
                let res = execute_task(&client, &url, &task, &bearer_token, timeout).await;

                match res {
                    Ok(result) => {
                        break Ok(RpcResultPair { task, result });
                    }
                    Err(e) => {
                        tracing::warn!(target: TARGET_RPC, "RPC Error: {:?}", e);
                        // Need to retry this task
                        iterations -= 1;
                        if iterations == 0 {
                            break Err(e);
                        }
                        tokio::time::sleep(sleep).await;
                        sleep *= 2;
                    }
                }
            };
            tx.send(res).await.expect("Failed to send task result");
        });
    }

    // Close the sender to ensure the loop below exits once all tasks are completed
    drop(tx);

    let mut errors = Vec::new();
    // Wait for all tasks to complete
    while let Some(res) = rx.recv().await {
        match res {
            Ok(pair) => results.push(pair),
            Err(e) => {
                errors.push(e);
            }
        }
    }
    let duration = start.elapsed().as_millis();

    tracing::debug!(target: TARGET_RPC, "Query {}ms: fetch_from_rpc {} tasks",
        duration,
        tasks.len());

    if let Some(err) = errors.pop() {
        return Err(err);
    }

    Ok(results)
}

async fn execute_task(
    client: &Client,
    url: &String,
    task: &RpcTask,
    bearer_token: &Option<String>,
    timeout: Duration,
) -> Result<Option<RpcTaskResult>, RpcError> {
    match task {
        RpcTask::FtPair {
            block_height,
            account_id,
            token_id,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "call_function",
                    "account_id": token_id,
                    "method_name": "ft_balance_of",
                    "args_base64": BASE64_STANDARD.encode(format!("{{\"account_id\": \"{}\"}}", account_id)),
                }),
                client,
                url,
                block_height,
                bearer_token,
                timeout,
            ).await?;
            match value {
                Some(value) => parse_ft_balance(value),
                None => Ok(None),
            }
        }
        RpcTask::AccountState {
            block_height,
            account_id,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "view_account",
                    "account_id": account_id,
                }),
                client,
                url,
                block_height,
                bearer_token,
                timeout,
            )
            .await?;
            match value {
                Some(value) => parse_account_state(value),
                None => Ok(None),
            }
        }
    }
}

async fn rpc_json_request(
    mut params: Value,
    client: &Client,
    url: &String,
    block_height: &Option<BlockHeight>,
    bearer_token: &Option<String>,
    timeout: Duration,
) -> Result<Option<Value>, RpcError> {
    if let Some(block_height) = block_height {
        params["block_id"] = json!(*block_height);
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
    if let Some(error) = response.error {
        if let Some(cause) = error.cause {
            if cause.name == Some(RPC_ERROR_UNKNOWN_BLOCK.to_string()) {
                return Err(RpcError::UnknownBlock);
            }
        }
    }

    Ok(response.result)
}

fn parse_account_state(result: Value) -> Result<Option<RpcTaskResult>, RpcError> {
    let account_state: AccountStateResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidAccountStateResponse(e))?;
    Ok(Some(RpcTaskResult::AccountState(RpcAccountStateResult {
        balance: account_state.amount,
        locked: account_state.locked,
        storage_bytes: account_state.storage_usage,
    })))
}

fn parse_ft_balance(result: Value) -> Result<Option<RpcTaskResult>, RpcError> {
    let fc: FunctionCallResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidFunctionCallResponse(e))?;
    if let Some(error) = fc.error {
        tracing::debug!(target: TARGET_RPC, "FCR Error: {}", error);
    }
    Ok(fc.result.and_then(|result| {
        let balance: Option<String> = serde_json::from_slice(&result).ok();
        let parsed_balance = balance.and_then(|s| s.parse().ok());
        parsed_balance.map(|b| RpcTaskResult::FtPair(RpcFtPairResult { balance: b }))
    }))
}
