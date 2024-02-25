# Compact indexer

Indexes NEAR blockchain actions and store them in a ClickHouse DB.

## Create ClickHouse table

```sql
-- This is a ClickHouse table.
CREATE TABLE near.repl_actions_tx on CLUSTER cluster1
(
    block_height UInt64 COMMENT 'Block height',
    block_hash String COMMENT 'Block hash',
    block_timestamp DateTime64(9, 'UTC') COMMENT 'Block timestamp in UTC',
    transaction_hash String COMMENT 'Transaction hash',
    receipt_id String COMMENT 'Receipt hash',
    receipt_index UInt16 COMMENT 'Index of the receipt that appears in the block across all shards',
    action_index UInt8 COMMENT 'Index of the actions within the receipt',
    signer_id String COMMENT 'The account ID of the transaction signer',
    signer_public_key String COMMENT 'The public key of the transaction signer',
    predecessor_id String COMMENT 'The account ID of the receipt predecessor',
    account_id String COMMENT 'The account ID of where the receipt is executed',
    status Enum('FAILURE', 'SUCCESS') COMMENT 'The status of the receipt execution, either SUCCESS or FAILURE',
    action Enum('CREATE_ACCOUNT', 'DEPLOY_CONTRACT', 'FUNCTION_CALL', 'TRANSFER', 'STAKE', 'ADD_KEY', 'DELETE_KEY', 'DELETE_ACCOUNT', 'DELEGATE') COMMENT 'The action type',
    contract_hash Nullable(String) COMMENT 'The hash of the contract if the action is DEPLOY_CONTRACT',
    public_key Nullable(String) COMMENT 'The public key used in the action if the action is ADD_KEY or DELETE_KEY',
    access_key_contract_id Nullable(String) COMMENT 'The contract ID of the limited access key if the action is ADD_KEY and not a full access key',
    deposit Nullable(UInt128) COMMENT 'The amount of attached deposit in yoctoNEAR if the action is FUNCTION_CALL, STAKE or TRANSFER',
    gas_price UInt128 COMMENT 'The gas price in yoctoNEAR for the receipt',
    attached_gas Nullable(UInt64) COMMENT 'The amount of attached gas if the action is FUNCTION_CALL',
    gas_burnt UInt64 COMMENT 'The amount of burnt gas for the execution of the whole receipt',
    tokens_burnt UInt128 COMMENT 'The amount of tokens in yoctoNEAR burnt for the execution of the whole receipt',
    method_name Nullable(String) COMMENT 'The method name if the action is FUNCTION_CALL',
    args_account_id Nullable(String) COMMENT '`account_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_new_account_id Nullable(String) COMMENT '`new_account_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_owner_id Nullable(String) COMMENT '`owner_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_receiver_id Nullable(String) COMMENT '`receiver_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_sender_id Nullable(String) COMMENT '`sender_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_token_id Nullable(String) COMMENT '`token_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_amount Nullable(UInt128) COMMENT '`amount` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_balance Nullable(UInt128) COMMENT '`balance` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_nft_contract_id Nullable(String) COMMENT '`nft_contract_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_nft_token_id Nullable(String) COMMENT '`nft_token_id` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_utm_source Nullable(String) COMMENT '`_utm_source` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_utm_medium Nullable(String) COMMENT '`_utm_medium` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_utm_campaign Nullable(String) COMMENT '`_utm_campaign` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_utm_term Nullable(String) COMMENT '`_utm_term` argument from the JSON arguments if the action is FUNCTION_CALL',
    args_utm_content Nullable(String) COMMENT '`_utm_content` argument from the JSON arguments if the action is FUNCTION_CALL',
    return_value_int Nullable(UInt128) COMMENT 'The parsed integer string from the returned value of the FUNCTION_CALL action',

    INDEX block_height_minmax_idx block_height TYPE minmax GRANULARITY 1,
    INDEX account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX signer_id_bloom_index signer_id TYPE bloom_filter() GRANULARITY 1,
    INDEX block_hash_bloom_index block_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX transaction_hash_bloom_index transaction_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX receipt_hash_bloom_index receipt_hash TYPE bloom_filter() GRANULARITY 1,
    INDEX public_key_bloom_index public_key TYPE bloom_filter() GRANULARITY 1,
    INDEX predecessor_id_bloom_index predecessor_id TYPE bloom_filter() GRANULARITY 1,
    INDEX method_name_index method_name TYPE set(0) GRANULARITY 1,
    INDEX args_account_id_bloom_index args_account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX args_new_account_id_bloom_index args_new_account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX args_owner_id_bloom_index args_owner_id TYPE bloom_filter() GRANULARITY 1,
    INDEX args_receiver_id_bloom_index args_receiver_id TYPE bloom_filter() GRANULARITY 1,
    INDEX args_sender_id_bloom_index args_sender_id TYPE bloom_filter() GRANULARITY 1,
)
    ENGINE = ReplicatedReplacingMergeTree
PRIMARY KEY (block_timestamp, account_id)
ORDER BY (block_timestamp, account_id, receipt_index, action_index)
                               
CREATE TABLE actions_tx AS near.repl_actions_tx
ENGINE = Distributed(cluster1, near, repl_actions_tx)

CREATE TABLE near.repl_events ON CLUSTER cluster1
(
    block_height UInt64 COMMENT 'Block height',
    block_hash String COMMENT 'Block hash',
    block_timestamp DateTime64(9, 'UTC') COMMENT 'Block timestamp in UTC',
    transaction_hash String COMMENT 'Transaction hash',
    receipt_id String COMMENT 'Receipt hash',
    receipt_index UInt16 COMMENT 'Index of the receipt that appears in the block across all shards',
    log_index UInt16 COMMENT 'Index of the log within the receipt',
    signer_id String COMMENT 'The account ID of the transaction signer',
    signer_public_key String COMMENT 'The public key of the transaction signer',
    predecessor_id String COMMENT 'The account ID of the receipt predecessor',
    account_id String COMMENT 'The account ID of where the receipt is executed',
    status Enum('FAILURE', 'SUCCESS') COMMENT 'The status of the receipt execution, either SUCCESS or FAILURE',
    
    version Nullable(String) COMMENT '`version` field from the JSON event',
    standard Nullable(String) COMMENT '`standard` field from the JSON event',
    event Nullable(String) COMMENT '`event` field from the JSON event',
    data_account_id Nullable(String) COMMENT '`account_id` field from the first data object in the JSON event',
    data_owner_id Nullable(String) COMMENT '`owner_id` field from the first data object in the JSON event',
    data_old_owner_id Nullable(String) COMMENT '`old_owner_id` field from the first data object in the JSON event',
    data_new_owner_id Nullable(String) COMMENT '`new_owner_id` field from the first data object in the JSON event',
    data_liquidation_account_id Nullable(String) COMMENT '`liquidation_account_id` field from the first data object in the JSON event',
    data_authorized_id Nullable(String) COMMENT '`authorized_id` field from the first data object in the JSON event',
    data_token_ids Array(String) COMMENT '`token_ids` field from the first data object in the JSON event',
    data_token_id Nullable(String) COMMENT '`token_id` field from the first data object in the JSON event',
    data_position Nullable(String) COMMENT '`position` field from the first data object in the JSON event',
    data_amount Nullable(UInt128) COMMENT '`amount` field from the first data object in the JSON event',

    INDEX block_height_minmax_idx block_height TYPE minmax GRANULARITY 1,
    INDEX account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX event_set_index event TYPE set(0) GRANULARITY 1,
    INDEX data_account_id_bloom_index data_account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX data_owner_id_bloom_index data_owner_id TYPE bloom_filter() GRANULARITY 1,
    INDEX data_old_owner_id_bloom_index data_old_owner_id TYPE bloom_filter() GRANULARITY 1,
    INDEX data_new_owner_id_bloom_index data_new_owner_id TYPE bloom_filter() GRANULARITY 1,
    )
ENGINE = ReplicatedReplacingMergeTree
PRIMARY KEY (block_timestamp, account_id)
ORDER BY (block_timestamp, account_id, receipt_index, log_index)

CREATE TABLE events AS near.repl_events
ENGINE = Distributed(cluster1, near, repl_events)
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
