mod utils;

use clickhouse::{Client, Row};
use fastnear_primitives::near_primitives::views::{
    AccessKeyPermissionView, ActionView, ExecutionOutcomeView, ExecutionStatusView,
    ReceiptEnumView, ReceiptView,
};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::env;
pub use utils::extract_rows;
use utils::*;

use serde::Serialize;

use fastnear_primitives::block_with_tx_hash::BlockWithTxHashes;
use fastnear_primitives::near_primitives::types::BlockHeight;
use std::convert::TryFrom;
use std::time::Duration;
use tokio_retry::{strategy::ExponentialBackoff, Retry};

const CLICKHOUSE_TARGET: &str = "clickhouse";
const EVENT_LOG_PREFIX: &str = "EVENT_JSON:";

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum ReceiptStatus {
    Failure = 1,
    Success = 2,
}

#[derive(Copy, Clone, Debug, Serialize_repr, Deserialize_repr, PartialEq)]
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
    DeployGlobalContract = 10,
    DeployGlobalContractByAccountId = 11,
    UseGlobalContract = 12,
    UseGlobalContractByAccountId = 13,
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

pub struct ClickDB {
    pub client: Client,
    pub actions: Vec<ActionRow>,
    pub events: Vec<EventRow>,
    pub min_batch: usize,
}

impl ClickDB {
    pub fn new(min_batch: usize) -> Self {
        Self {
            client: establish_connection(),
            actions: Vec::new(),
            events: Vec::new(),
            min_batch,
        }
    }

    pub async fn commit(&mut self) -> clickhouse::error::Result<()> {
        self.commit_actions().await?;
        self.commit_events().await?;
        Ok(())
    }

    pub async fn commit_actions(&mut self) -> clickhouse::error::Result<()> {
        insert_rows_with_retry(&self.client, &self.actions, "actions").await?;
        self.actions.clear();
        Ok(())
    }

    pub async fn commit_events(&mut self) -> clickhouse::error::Result<()> {
        insert_rows_with_retry(&self.client, &self.events, "events").await?;
        self.events.clear();
        Ok(())
    }

    pub async fn last_block_height(&self) -> clickhouse::error::Result<Option<BlockHeight>> {
        let block_height = self
            .client
            .query("SELECT max(block_height) FROM actions")
            .fetch_one::<u64>()
            .await?;
        Ok(Some(block_height))
    }
}

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}

pub async fn extract_info(db: &mut ClickDB, msg: BlockWithTxHashes) -> anyhow::Result<()> {
    let block_height = msg.block.header.height;
    let (actions, events) = extract_rows(msg);
    db.actions.extend(actions);
    db.events.extend(events);

    if block_height % 1000 == 0 {
        tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} actions and {} events", block_height, db.actions.len(), db.events.len());
    }
    if db.actions.len() >= db.min_batch {
        db.commit_actions().await?;
    }
    if db.events.len() >= db.min_batch {
        db.commit_events().await?;
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
                tracing::log::error!(target: CLICKHOUSE_TARGET, "Error inserting rows into \"{}\": {}", table, err);
                Err(err)
            }
        }
    });

    retry_future.await
}
