use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::types::ShardId;
use near_indexer::near_primitives::views::{
    BlockView, ExecutionOutcomeWithIdView, ReceiptView, StateChangeWithCauseView,
};
use near_indexer::{
    IndexerChunkView, IndexerExecutionOutcomeWithReceipt, IndexerShard, StreamerMessage,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BlockWithTxHashes {
    pub block: BlockView,
    pub shards: Vec<IndexerShardWithTxHashes>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerShardWithTxHashes {
    pub shard_id: ShardId,
    pub chunk: Option<IndexerChunkView>,
    pub receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceiptAndTxHash>,
    pub state_changes: Vec<StateChangeWithCauseView>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IndexerExecutionOutcomeWithReceiptAndTxHash {
    pub execution_outcome: ExecutionOutcomeWithIdView,
    pub receipt: ReceiptView,
    pub tx_hash: Option<CryptoHash>,
}

impl From<StreamerMessage> for BlockWithTxHashes {
    fn from(streamer_message: StreamerMessage) -> Self {
        Self {
            block: streamer_message.block,
            shards: streamer_message
                .shards
                .into_iter()
                .map(
                    |IndexerShard {
                         shard_id,
                         chunk,
                         receipt_execution_outcomes,
                         state_changes,
                     }| IndexerShardWithTxHashes {
                        shard_id,
                        chunk,
                        receipt_execution_outcomes: receipt_execution_outcomes
                            .into_iter()
                            .map(
                                |IndexerExecutionOutcomeWithReceipt {
                                     execution_outcome,
                                     receipt,
                                 }| {
                                    IndexerExecutionOutcomeWithReceiptAndTxHash {
                                        execution_outcome,
                                        receipt,
                                        tx_hash: None,
                                    }
                                },
                            )
                            .collect(),
                        state_changes,
                    },
                )
                .collect(),
        }
    }
}
