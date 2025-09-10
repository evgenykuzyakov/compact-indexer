# Compact indexer

Indexes NEAR blockchain actions and store them in a ClickHouse DB.

## Getting started

Clickhouse instructions taken from https://clickhouse.com/docs/en/getting-started/quick-start

The first command will create a binary, and it's best to navigate to the root directory of this project.

Run this command to build the binary:

    curl https://clickhouse.com/ | sh

Now run the binary passing "server" and our development config:

    ./clickhouse server --config-file clickhouse-info/config/config.xml

(**Ctrl + C** to exit)

Access the basic terminal client:

    ./clickhouse client

(**Ctrl + D** to exit)

Create two new databases (using the `:)` prompt in the `client`):

    CREATE DATABASE near_mainnet_compact;

Show it for a sanity check:

    SHOW DATABASES

This project utilizes efficiency gains from:
https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication

Next, you'll need to install `zookeeper`, from Apache.

Mac (OS X):

    brew install zookeeper

Linux:

    sudo apt-get install zookeeperd

After installing zookeeper, it instructs you to run commands to start the service, so make sure to follow those by copy/pasting the command(s).

The database schema is in the `clickhouse-info/db-schemas` folder. Exit the Clickhouse client with Ctrl + D. Apply the schema (execute SQL commands) to the new database.

    ./clickhouse client -d near_mainnet_compact --queries-file clickhouse-info/db-schemas/actions-events.sql

## To run

Copy the `.env.template` into `.env` and modify the environment variables:

    cp .env.template .env

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
