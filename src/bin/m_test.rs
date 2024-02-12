use clickhouse::{Client, Row};
use dotenv::dotenv;
use serde::Serialize;
use std::env;
use tracing_subscriber::EnvFilter;

use std::time::Duration;
use tokio::time;
use tokio_retry::{strategy::ExponentialBackoff, Retry};

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}

const PROJECT_ID: &str = "compact_indexer";

/*
DROP TABLE IF EXISTS default.time

CREATE TABLE default.time
(
    date UInt64,
    yo String
)
    ENGINE = ReplacingMergeTree()
PRIMARY KEY (yo, date)
ORDER BY (yo, date)
*/

#[derive(Row, Serialize)]
pub struct ActionRow {
    pub date: u64,
    pub yo: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

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
    let mut interval = time::interval(Duration::from_secs(1));
    let client = establish_connection();

    loop {
        interval.tick().await;
        let now = std::time::SystemTime::now();
        let since_the_epoch = now.duration_since(std::time::UNIX_EPOCH)?;
        let timestamp = since_the_epoch.as_secs();

        let rows = vec![ActionRow {
            date: timestamp,
            yo: "yo".to_string(),
        }];
        insert_rows_with_retry(&client, &rows).await?;
        tracing::log::info!(target: PROJECT_ID, "Inserted {} rows", rows.len());
    }
}

async fn insert_rows_with_retry(
    client: &Client,
    rows: &Vec<ActionRow>,
) -> clickhouse::error::Result<()> {
    let strategy = ExponentialBackoff::from_millis(100).max_delay(Duration::from_secs(10));
    let retry_future = Retry::spawn(strategy, || async {
        let mut insert = client.insert("time")?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await
    });

    retry_future.await
}
