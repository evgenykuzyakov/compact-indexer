use near_crypto::{PublicKey, Signature};
use near_indexer::near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_indexer::near_primitives::challenge::ChallengesResult;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::serialize::dec_format;
use near_indexer::near_primitives::types::{
    AccountId, Balance, BlockHeight, Gas, NumBlocks, ProtocolVersion, ShardId, StateRoot,
    StorageUsage, StoreKey, StoreValue,
};
use near_indexer::near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_indexer::near_primitives::views::AccessKeyView;
use near_indexer::near_primitives::{borsh, views};
use serde_with::base64::Base64;
use serde_with::serde_as;

#[derive(Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XBlock {
    pub block: XBlockView,
    pub shards: Vec<XIndexerShard>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, BorshSerialize, BorshDeserialize)]
pub struct XBlockView {
    pub author: AccountId,
    pub header: XBlockHeaderView,
    pub chunks: Vec<XChunkHeaderView>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct XBlockHeaderView {
    pub height: BlockHeight,
    pub prev_height: Option<BlockHeight>,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub block_body_hash: Option<CryptoHash>,
    pub chunk_receipts_root: CryptoHash,
    pub chunk_headers_root: CryptoHash,
    pub chunk_tx_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub chunks_included: u64,
    pub challenges_root: CryptoHash,
    /// Legacy json number. Should not be used.
    pub timestamp: u64,
    #[serde(with = "dec_format")]
    pub timestamp_nanosec: u64,
    pub random_value: CryptoHash,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub chunk_mask: Vec<bool>,
    #[serde(with = "dec_format")]
    pub gas_price: Balance,
    pub block_ordinal: Option<NumBlocks>,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub rent_paid: Balance,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "dec_format")]
    pub total_supply: Balance,
    pub challenges_result: ChallengesResult,
    pub last_final_block: CryptoHash,
    pub last_ds_final_block: CryptoHash,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
    pub epoch_sync_data_hash: Option<CryptoHash>,
    pub approvals: Vec<Option<Signature>>,
    pub signature: Signature,
    pub latest_protocol_version: ProtocolVersion,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct XChunkHeaderView {
    pub chunk_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub outcome_root: CryptoHash,
    pub prev_state_root: StateRoot,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    pub height_included: BlockHeight,
    pub shard_id: ShardId,
    pub gas_used: Gas,
    pub gas_limit: Gas,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub rent_paid: Balance,
    /// TODO(2271): deprecated.
    #[serde(with = "dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "dec_format")]
    pub balance_burnt: Balance,
    pub outgoing_receipts_root: CryptoHash,
    pub tx_root: CryptoHash,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub signature: Signature,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XIndexerShard {
    pub shard_id: ShardId,
    pub chunk: Option<XIndexerChunkView>,
    pub receipt_execution_outcomes: Vec<XIndexerExecutionOutcomeWithReceipt>,
    pub state_changes: Vec<XStateChangeWithCauseView>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XStateChangeWithCauseView {
    pub cause: XStateChangeCauseView,
    #[serde(flatten)]
    pub value: XStateChangeValueView,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum XStateChangeCauseView {
    NotWritableToDisk,
    InitialState,
    TransactionProcessing { tx_hash: CryptoHash },
    ActionReceiptProcessingStarted { receipt_hash: CryptoHash },
    ActionReceiptGasReward { receipt_hash: CryptoHash },
    ReceiptProcessing { receipt_hash: CryptoHash },
    PostponedReceipt { receipt_hash: CryptoHash },
    UpdatedDelayedReceipts,
    ValidatorAccountsUpdate,
    Migration,
    Resharding,
}

#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "change")]
pub enum XStateChangeValueView {
    AccountUpdate {
        account_id: AccountId,
        #[serde(flatten)]
        account: XAccountView,
    },
    AccountDeletion {
        account_id: AccountId,
    },
    AccessKeyUpdate {
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKeyView,
    },
    AccessKeyDeletion {
        account_id: AccountId,
        public_key: PublicKey,
    },
    DataUpdate {
        account_id: AccountId,
        #[serde(rename = "key_base64")]
        key: StoreKey,
        #[serde(rename = "value_base64")]
        value: StoreValue,
    },
    DataDeletion {
        account_id: AccountId,
        #[serde(rename = "key_base64")]
        key: StoreKey,
    },
    ContractCodeUpdate {
        account_id: AccountId,
        #[serde(rename = "code_base64")]
        #[serde_as(as = "Base64")]
        code: Vec<u8>,
    },
    ContractCodeDeletion {
        account_id: AccountId,
    },
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Eq,
    PartialEq,
    Clone,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct XAccountView {
    #[serde(with = "dec_format")]
    pub amount: Balance,
    #[serde(with = "dec_format")]
    pub locked: Balance,
    pub code_hash: CryptoHash,
    pub storage_usage: StorageUsage,
    /// TODO(2271): deprecated.
    #[serde(default)]
    pub storage_paid_at: BlockHeight,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XIndexerChunkView {
    pub author: AccountId,
    pub header: XChunkHeaderView,
    pub transactions: Vec<XIndexerTransactionWithOutcome>,
    pub receipts: Vec<views::ReceiptView>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XIndexerTransactionWithOutcome {
    pub transaction: views::SignedTransactionView,
    pub outcome: XIndexerExecutionOutcomeWithOptionalReceipt,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XIndexerExecutionOutcomeWithOptionalReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: Option<views::ReceiptView>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, BorshSerialize, BorshDeserialize)]
pub struct XIndexerExecutionOutcomeWithReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: views::ReceiptView,
}
