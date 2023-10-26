# Compact indexer

Indexes NEAR blockchain actions and store them in a ClickHouse DB.

## Create ClickHouse table

```sql
-- This is a ClickHouse table.
-- receipt_index is an index of a receipt within a block. It doesn't have significance.
CREATE TABLE default.actions
(
    block_height UInt64,
    block_hash String,
    block_timestamp DateTime64(9),
    receipt_id String,
    receipt_index UInt16,
    action_index UInt8,
    predecessor_id String,
    account_id String,
    status Enum('FAILURE', 'SUCCESS'),
    action Enum('CREATE_ACCOUNT', 'DEPLOY_CONTRACT', 'FUNCTION_CALL', 'TRANSFER', 'STAKE', 'ADD_KEY', 'DELETE_KEY', 'DELETE_ACCOUNT', 'DELEGATE'),
    contract_hash Nullable(String),
    public_key Nullable(String),
    access_key_contract_id Nullable(String),
    action_deposit Nullable(UInt128),
    gas_price UInt128,
    attached_gas Nullable(UInt64),
    gas_burnt UInt64,
    tokens_burnt UInt128,
    method_name Nullable(String),
    args_account_id Nullable(String),
    args_receiver_id Nullable(String),
    args_sender_id Nullable(String),
    args_token_id Nullable(String),
    args_amount Nullable(UInt128),
    return_value_int Nullable(UInt128),
)
    ENGINE = MergeTree()
PRIMARY KEY (account_id, block_timestamp)
ORDER BY (account_id, block_timestamp, receipt_index, action_index)
```

## To run

Create `.env` file and add:
```
DATABASE_URL=tcp://default:PASSWORD@HOST:9440/defaut?secure=true
```

```bash
cargo build --release
./target/release/compact-indexer init
./target/release/compact-indexer run
```
