# Compact indexer

Indexes NEAR blockchain actions and store them in a ClickHouse DB.

## Create ClickHouse table

```sql
-- This is a ClickHouse table.
-- receipt_index is an index of a receipt within a block. It's used for deduplication of actions.
CREATE TABLE default.actions
(
    block_height UInt64,
    block_hash String,
    block_timestamp DateTime64(9, 'UTC'),
    receipt_id String,
    receipt_index UInt16,
    action_index UInt8,
    signer_id String,
    signer_public_key String,
    predecessor_id String,
    account_id String,
    status Enum('FAILURE', 'SUCCESS'),
    action Enum('CREATE_ACCOUNT', 'DEPLOY_CONTRACT', 'FUNCTION_CALL', 'TRANSFER', 'STAKE', 'ADD_KEY', 'DELETE_KEY', 'DELETE_ACCOUNT', 'DELEGATE'),
    contract_hash Nullable(String),
    public_key Nullable(String),
    access_key_contract_id Nullable(String),
    deposit Nullable(UInt128),
    gas_price UInt128,
    attached_gas Nullable(UInt64),
    gas_burnt UInt64,
    tokens_burnt UInt128,
    method_name Nullable(String),
    args_account_id Nullable(String),
    args_new_account_id Nullable(String),
    args_owner_id Nullable(String),
    args_receiver_id Nullable(String),
    args_sender_id Nullable(String),
    args_token_id Nullable(String),
    args_amount Nullable(UInt128),
    args_balance Nullable(UInt128),
    args_nft_contract_id Nullable(String),
    args_nft_token_id Nullable(String),
    args_utm_source Nullable(String),
    args_utm_medium Nullable(String),
    args_utm_campaign Nullable(String),
    args_utm_term Nullable(String),
    args_utm_content Nullable(String),
    return_value_int Nullable(UInt128),
)
    ENGINE = ReplacingMergeTree()
PRIMARY KEY (account_id, block_timestamp)
ORDER BY (account_id, block_timestamp, receipt_index, action_index)

CREATE TABLE default.events
(
    block_height UInt64,
    block_hash String,
    block_timestamp DateTime64(9, 'UTC'),
    receipt_id String,
    receipt_index UInt16,
    log_index UInt16,
    signer_id String,
    signer_public_key String,
    predecessor_id String,
    account_id String,
    status Enum('FAILURE', 'SUCCESS'),
    
    version Nullable(String),
    standard Nullable(String),
    event Nullable(String),
    data_account_id Nullable(String),
    data_owner_id Nullable(String),
    data_old_owner_id Nullable(String),
    data_new_owner_id Nullable(String),
    data_liquidation_account_id Nullable(String),
    data_authorized_id Nullable(String),
    data_token_ids Array(String),
    data_token_id Nullable(String),
    data_position Nullable(String),
    data_amount Nullable(UInt128),
)
    ENGINE = ReplacingMergeTree()
PRIMARY KEY (account_id, block_timestamp)
ORDER BY (account_id, block_timestamp, receipt_index, log_index)
```

## To run

Create `.env` file and fill details:
```
DATABASE_URL=https://FOOBAR.clickhouse.cloud:8443
DATABASE_USER=default
DATABASE_PASSWORD=PASSWORD
DATABASE_DATABASE=default
```

Follow a NEAR RPC node setup instructions to get a node running.

```bash
cargo build --release
./target/release/compact-indexer run
```
