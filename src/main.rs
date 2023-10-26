mod retriable;
mod utils;

use crate::utils::*;
use clickhouse::{Client, Row};
use dotenv::dotenv;
use near_indexer::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView,
    ReceiptEnumView, ReceiptView,
};
use std::convert::TryFrom;
use std::env;
use tracing_subscriber::EnvFilter;

use near_indexer::near_primitives::hash::CryptoHash;
use serde::Serialize;

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}

const PROJECT_ID: &str = "compact_indexer";

mod receipt_status {
    pub const FAILURE: &str = "FAILURE";
    pub const SUCCESS: &str = "SUCCESS";
}

mod action_kind {
    pub const CREATE_ACCOUNT: &str = "CREATE_ACCOUNT";
    pub const DEPLOY_CONTRACT: &str = "DEPLOY_CONTRACT";
    pub const FUNCTION_CALL: &str = "FUNCTION_CALL";
    pub const TRANSFER: &str = "TRANSFER";
    pub const STAKE: &str = "STAKE";
    pub const ADD_KEY: &str = "ADD_KEY";
    pub const DELETE_KEY: &str = "DELETE_KEY";
    pub const DELETE_ACCOUNT: &str = "DELETE_ACCOUNT";
    pub const DELEGATE: &str = "DELEGATE";
}

#[derive(Row, Serialize)]
pub struct ActionRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub receipt_index: u16,
    pub action_index: u8,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: &'static str,
    pub action: &'static str,
    pub contract_hash: Option<String>,
    pub public_key: Option<String>,
    pub access_key_contract_id: Option<String>,
    pub deposit: Option<u128>,
    pub gas_price: u128,
    pub attached_gas: Option<u64>,
    pub gas_burnt: u64,
    pub tokens_burnt: u128,
    pub method_name: Option<String>,
    pub args_account_id: Option<String>,
    pub args_receiver_id: Option<String>,
    pub args_sender_id: Option<String>,
    pub args_token_id: Option<String>,
    pub args_amount: Option<u128>,
    pub return_value_int: Option<u128>,
}

fn main() {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let home_dir = std::path::PathBuf::from(near_indexer::get_default_home());

    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,compact_indexer=info",
    );

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    tracing::log::info!(
        target: PROJECT_ID,
        "Starting indexer",
    );

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    match command {
        "init" => {
            let config_args = near_indexer::InitConfigArgs {
                chain_id: None,
                account_id: None,
                test_seed: None,
                num_shards: 4,
                fast: false,
                genesis: None,
                download_genesis: false,
                download_genesis_url: None,
                download_records_url: None,
                download_config: true,
                download_config_url: Some("https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/mainnet/config.json".to_string()),
                boot_nodes: None,
                max_gas_burnt_view: None
            };
            near_indexer::indexer_init_configs(&home_dir, config_args).unwrap();
        }
        "run" => {
            let client = establish_connection();
            let indexer_config = near_indexer::IndexerConfig {
                home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
                validate_genesis: false,
            };
            let sys = actix::System::new();
            sys.block_on(async move {
                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                listen_blocks(stream, client).await;

                actix::System::current().stop();
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `init` or `run` arg"),
    }
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    client: Client,
) {
    while let Some(streamer_message) = stream.recv().await {
        extract_info(client.clone(), streamer_message)
            .await
            .unwrap();
    }
}

async fn extract_info(client: Client, msg: near_indexer::StreamerMessage) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;

    let mut insert = client.insert("actions")?;
    let mut receipt_index: u16 = 0;
    for shard in msg.shards {
        for outcome in shard.receipt_execution_outcomes {
            let ReceiptView {
                predecessor_id,
                receiver_id: account_id,
                receipt_id,
                receipt,
            } = outcome.receipt;
            let predecessor_id = predecessor_id.to_string();
            let account_id = account_id.to_string();
            let receipt_id = receipt_id.to_string();
            let ExecutionOutcomeView {
                status: execution_status,
                gas_burnt,
                tokens_burnt,
                ..
            } = outcome.execution_outcome.outcome;
            let status = match &execution_status {
                ExecutionStatusView::Unknown => receipt_status::FAILURE,
                ExecutionStatusView::Failure(_) => receipt_status::FAILURE,
                ExecutionStatusView::SuccessValue(_) => receipt_status::SUCCESS,
                ExecutionStatusView::SuccessReceiptId(_) => receipt_status::SUCCESS,
            };
            let return_value_int = extract_return_value_int(execution_status);
            match receipt {
                ReceiptEnumView::Action {
                    actions, gas_price, ..
                } => {
                    for (action_index, action) in actions.into_iter().enumerate() {
                        let action_index =
                            u8::try_from(action_index).expect("Action index overflow");
                        let args_data = extract_args_data(&action);
                        let row = ActionRow {
                            block_height,
                            block_hash: block_hash.clone(),
                            block_timestamp,
                            receipt_id: receipt_id.clone(),
                            receipt_index,
                            action_index,
                            predecessor_id: predecessor_id.clone(),
                            account_id: account_id.clone(),
                            status,
                            action: match action {
                                ActionView::CreateAccount => action_kind::CREATE_ACCOUNT,
                                ActionView::DeployContract { .. } => action_kind::DEPLOY_CONTRACT,
                                ActionView::FunctionCall { .. } => action_kind::FUNCTION_CALL,
                                ActionView::Transfer { .. } => action_kind::TRANSFER,
                                ActionView::Stake { .. } => action_kind::STAKE,
                                ActionView::AddKey { .. } => action_kind::ADD_KEY,
                                ActionView::DeleteKey { .. } => action_kind::DELETE_KEY,
                                ActionView::DeleteAccount { .. } => action_kind::DELETE_ACCOUNT,
                                ActionView::Delegate { .. } => action_kind::DELEGATE,
                            },
                            contract_hash: match &action {
                                ActionView::DeployContract { code } => {
                                    Some(CryptoHash::hash_bytes(&code).to_string())
                                }
                                _ => None,
                            },
                            public_key: match &action {
                                ActionView::AddKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                ActionView::DeleteKey { public_key, .. } => {
                                    Some(public_key.to_string())
                                }
                                _ => None,
                            },
                            access_key_contract_id: match &action {
                                ActionView::AddKey { access_key, .. } => {
                                    match &access_key.permission {
                                        AccessKeyPermissionView::FunctionCall {
                                            receiver_id,
                                            ..
                                        } => Some(receiver_id.to_string()),
                                        _ => None,
                                    }
                                }
                                _ => None,
                            },
                            deposit: match &action {
                                ActionView::Transfer { deposit, .. } => Some(*deposit),
                                ActionView::Stake { stake, .. } => Some(*stake),
                                ActionView::FunctionCall { deposit, .. } => Some(*deposit),
                                _ => None,
                            },
                            gas_price,
                            attached_gas: match &action {
                                ActionView::FunctionCall { gas, .. } => Some(*gas),
                                _ => None,
                            },
                            gas_burnt,
                            tokens_burnt,
                            method_name: match &action {
                                ActionView::FunctionCall { method_name, .. } => {
                                    Some(method_name.to_string())
                                }
                                _ => None,
                            },
                            args_account_id: args_data.as_ref().and_then(|args| {
                                args.account_id
                                    .as_ref()
                                    .map(|account_id| account_id.to_string())
                            }),
                            args_receiver_id: args_data.as_ref().and_then(|args| {
                                args.receiver_id
                                    .as_ref()
                                    .map(|receiver_id| receiver_id.to_string())
                            }),
                            args_sender_id: args_data.as_ref().and_then(|args| {
                                args.sender_id
                                    .as_ref()
                                    .map(|sender_id| sender_id.to_string())
                            }),
                            args_token_id: args_data
                                .as_ref()
                                .and_then(|args| args.token_id.clone()),
                            args_amount: args_data.as_ref().and_then(|args| {
                                args.amount.as_ref().and_then(|amount| amount.parse().ok())
                            }),
                            return_value_int,
                        };
                        insert.write(&row).await?;
                    }
                }
                ReceiptEnumView::Data { .. } => {}
            }
            receipt_index = receipt_index
                .checked_add(1)
                .expect("Receipt index overflow");
        }
    }

    insert.end().await?;

    Ok(())
}
