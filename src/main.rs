mod utils;

use crate::utils::*;
use clickhouse::{Client, Row};
use dotenv::dotenv;
use near_indexer::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView,
    ReceiptEnumView, ReceiptView,
};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::convert::TryFrom;
use std::env;
use tracing_subscriber::EnvFilter;

use near_indexer::near_primitives::hash::CryptoHash;
use serde::Serialize;

use std::time::Duration;
use tokio_retry::{strategy::ExponentialBackoff, Retry};

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}

const PROJECT_ID: &str = "compact_indexer";
const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ReceiptStatus {
    Failure = 1,
    Success = 2,
}

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ActionKind {
    CreateAccount = 1,
    DeployContract = 2,
    FunctionCall = 3,
    Transfer = 4,
    Stake = 5,
    AddKey = 6,
    DeleteKey = 7,
    DeleteAccount = 8,
    Delegate = 9,
}

#[derive(Row, Serialize)]
pub struct ActionRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub receipt_index: u16,
    pub action_index: u8,
    pub signer_id: String,
    pub signer_public_key: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ReceiptStatus,
    pub action: ActionKind,
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
    pub args_new_account_id: Option<String>,
    pub args_owner_id: Option<String>,
    pub args_receiver_id: Option<String>,
    pub args_sender_id: Option<String>,
    pub args_token_id: Option<String>,
    pub args_amount: Option<u128>,
    pub args_balance: Option<u128>,
    pub args_nft_contract_id: Option<String>,
    pub args_nft_token_id: Option<String>,
    pub args_utm_source: Option<String>,
    pub args_utm_medium: Option<String>,
    pub args_utm_campaign: Option<String>,
    pub args_utm_term: Option<String>,
    pub args_utm_content: Option<String>,
    pub return_value_int: Option<u128>,
}

#[derive(Row, Serialize)]
pub struct EventRow {
    pub block_height: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub receipt_index: u16,
    pub log_index: u16,
    pub signer_id: String,
    pub signer_public_key: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub status: ReceiptStatus,

    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
    pub data_account_id: Option<String>,
    pub data_owner_id: Option<String>,
    pub data_old_owner_id: Option<String>,
    pub data_new_owner_id: Option<String>,
    pub data_liquidation_account_id: Option<String>,
    pub data_authorized_id: Option<String>,
    pub data_token_ids: Vec<String>,
    pub data_token_id: Option<String>,
    pub data_position: Option<String>,
    pub data_amount: Option<u128>,
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

    tracing::log::info!(target: PROJECT_ID, "Starting indexer",);

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    match command {
        "run" => {
            let client = establish_connection();
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
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
        _ => panic!("You have to pass `run` arg"),
    }
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    client: Client,
) {
    while let Some(streamer_message) = stream.recv().await {
        extract_info(&client, streamer_message).await.unwrap();
    }
}

async fn extract_info(client: &Client, msg: near_indexer::StreamerMessage) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let block_hash = msg.block.header.hash.to_string();
    let block_timestamp = msg.block.header.timestamp_nanosec;

    let mut rows = vec![];
    let mut events = vec![];

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
                logs,
                ..
            } = outcome.execution_outcome.outcome;
            let status = match &execution_status {
                ExecutionStatusView::Unknown => ReceiptStatus::Failure,
                ExecutionStatusView::Failure(_) => ReceiptStatus::Failure,
                ExecutionStatusView::SuccessValue(_) => ReceiptStatus::Success,
                ExecutionStatusView::SuccessReceiptId(_) => ReceiptStatus::Success,
            };
            let return_value_int = extract_return_value_int(execution_status);
            match receipt {
                ReceiptEnumView::Action {
                    signer_id,
                    signer_public_key,
                    actions,
                    gas_price,
                    ..
                } => {
                    for (log_index, log) in logs.into_iter().enumerate() {
                        if log.starts_with(EVENT_LOG_PREFIX) {
                            let log_index = u16::try_from(log_index).expect("Log index overflow");
                            let event = parse_event(&log.as_str()[EVENT_LOG_PREFIX.len()..]);
                            if let Some(event) = event {
                                events.push(EventRow {
                                    block_height,
                                    block_hash: block_hash.clone(),
                                    block_timestamp,
                                    receipt_id: receipt_id.clone(),
                                    receipt_index,
                                    log_index,
                                    signer_id: signer_id.to_string(),
                                    signer_public_key: signer_public_key.to_string(),
                                    predecessor_id: predecessor_id.clone(),
                                    account_id: account_id.clone(),
                                    status,

                                    version: event.version,
                                    standard: event.standard,
                                    event: event.event,
                                    data_account_id: event.data.as_ref().and_then(|data| {
                                        data.account_id
                                            .as_ref()
                                            .map(|account_id| account_id.to_string())
                                    }),
                                    data_owner_id: event.data.as_ref().and_then(|data| {
                                        data.owner_id.as_ref().map(|owner_id| owner_id.to_string())
                                    }),
                                    data_old_owner_id: event.data.as_ref().and_then(|data| {
                                        data.old_owner_id
                                            .as_ref()
                                            .map(|old_owner_id| old_owner_id.to_string())
                                    }),
                                    data_new_owner_id: event.data.as_ref().and_then(|data| {
                                        data.new_owner_id
                                            .as_ref()
                                            .map(|new_owner_id| new_owner_id.to_string())
                                    }),
                                    data_liquidation_account_id: event.data.as_ref().and_then(
                                        |data| {
                                            data.liquidation_account_id.as_ref().map(
                                                |liquidation_account_id| {
                                                    liquidation_account_id.to_string()
                                                },
                                            )
                                        },
                                    ),
                                    data_authorized_id: event.data.as_ref().and_then(|data| {
                                        data.authorized_id
                                            .as_ref()
                                            .map(|authorized_id| authorized_id.to_string())
                                    }),
                                    data_token_ids: event
                                        .data
                                        .as_ref()
                                        .and_then(|data| data.token_ids.clone())
                                        .unwrap_or_default(),
                                    data_token_id: event
                                        .data
                                        .as_ref()
                                        .and_then(|data| data.token_id.clone()),
                                    data_position: event
                                        .data
                                        .as_ref()
                                        .and_then(|data| data.position.clone()),
                                    data_amount: event.data.as_ref().and_then(|data| {
                                        data.amount.as_ref().and_then(|amount| amount.parse().ok())
                                    }),
                                });
                            }
                        }
                    }

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
                            signer_id: signer_id.to_string(),
                            signer_public_key: signer_public_key.to_string(),
                            predecessor_id: predecessor_id.clone(),
                            account_id: account_id.clone(),
                            status,
                            action: match action {
                                ActionView::CreateAccount => ActionKind::CreateAccount,
                                ActionView::DeployContract { .. } => ActionKind::DeployContract,
                                ActionView::FunctionCall { .. } => ActionKind::FunctionCall,
                                ActionView::Transfer { .. } => ActionKind::Transfer,
                                ActionView::Stake { .. } => ActionKind::Stake,
                                ActionView::AddKey { .. } => ActionKind::AddKey,
                                ActionView::DeleteKey { .. } => ActionKind::DeleteKey,
                                ActionView::DeleteAccount { .. } => ActionKind::DeleteAccount,
                                ActionView::Delegate { .. } => ActionKind::Delegate,
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
                            args_new_account_id: args_data.as_ref().and_then(|args| {
                                args.args_new_account_id
                                    .as_ref()
                                    .map(|new_account_id| new_account_id.to_string())
                            }),
                            args_owner_id: args_data.as_ref().and_then(|args| {
                                args.args_owner_id
                                    .as_ref()
                                    .map(|owner_id| owner_id.to_string())
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
                            args_balance: args_data.as_ref().and_then(|args| {
                                args.balance
                                    .as_ref()
                                    .and_then(|balance| balance.parse().ok())
                            }),
                            args_nft_contract_id: args_data.as_ref().and_then(|args| {
                                args.nft_contract_id
                                    .as_ref()
                                    .map(|nft_contract_id| nft_contract_id.to_string())
                            }),
                            args_nft_token_id: args_data.as_ref().and_then(|args| {
                                args.nft_token_id
                                    .as_ref()
                                    .map(|nft_token_id| nft_token_id.to_string())
                            }),
                            args_utm_source: args_data.as_ref().and_then(|args| {
                                args.utm_source
                                    .as_ref()
                                    .map(|utm_source| utm_source.to_string())
                            }),
                            args_utm_medium: args_data.as_ref().and_then(|args| {
                                args.utm_medium
                                    .as_ref()
                                    .map(|utm_medium| utm_medium.to_string())
                            }),
                            args_utm_campaign: args_data.as_ref().and_then(|args| {
                                args.utm_campaign
                                    .as_ref()
                                    .map(|utm_campaign| utm_campaign.to_string())
                            }),
                            args_utm_term: args_data.as_ref().and_then(|args| {
                                args.utm_term.as_ref().map(|utm_term| utm_term.to_string())
                            }),
                            args_utm_content: args_data.as_ref().and_then(|args| {
                                args.utm_content
                                    .as_ref()
                                    .map(|utm_content| utm_content.to_string())
                            }),
                            return_value_int,
                        };
                        rows.push(row);
                    }
                }
                ReceiptEnumView::Data { .. } => {}
            }
            receipt_index = receipt_index
                .checked_add(1)
                .expect("Receipt index overflow");
        }
    }

    if !rows.is_empty() {
        insert_rows_with_retry(client, &rows, "actions").await?;
    }
    if !events.is_empty() {
        insert_rows_with_retry(client, &events, "events").await?;
    }
    if block_height % 100 == 0 {
        tracing::log::info!(target: PROJECT_ID, "{}#: Inserted {} actions rows", block_height, rows.len());
        tracing::log::info!(target: PROJECT_ID, "{}#: Inserted {} events rows", block_height, events.len());
    }
    Ok(())
}

async fn insert_rows_with_retry<T>(
    client: &Client,
    rows: &Vec<T>,
    table: &str,
) -> clickhouse::error::Result<()>
where
    T: Row + Serialize,
{
    let strategy = ExponentialBackoff::from_millis(100).max_delay(Duration::from_secs(30));
    let retry_future = Retry::spawn(strategy, || async {
        let res = || async {
            let mut insert = client.insert(table)?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        };
        match res().await {
            Ok(_) => Ok(()),
            Err(err) => {
                tracing::log::error!(target: PROJECT_ID, "Error inserting rows into \"{}\": {}", table, err);
                Err(err)
            }
        }
    });

    retry_future.await
}
