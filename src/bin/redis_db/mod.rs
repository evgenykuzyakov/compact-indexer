mod stream;

use stream::*;

use itertools::Itertools;
use redis::aio::Connection;
use redis::Client;
use std::env;

pub struct RedisDB {
    pub client: Client,
    pub connection: Connection,
}

#[allow(dead_code)]
impl RedisDB {
    pub async fn new() -> Self {
        let client = Client::open(env::var("REDIS_URL").expect("Missing REDIS_URL env var"))
            .expect("Failed to connect to Redis");
        let connection = client
            .get_async_connection()
            .await
            .expect("Failed to on Redis connection");
        Self { client, connection }
    }

    pub async fn reconnect(&mut self) -> redis::RedisResult<()> {
        self.connection = self.client.get_async_connection().await?;
        Ok(())
    }
}

#[allow(dead_code)]
impl RedisDB {
    pub async fn set(&mut self, key: &str, value: &str) -> redis::RedisResult<String> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async(&mut self.connection)
            .await
    }

    pub async fn get(&mut self, key: &str) -> redis::RedisResult<String> {
        redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.connection)
            .await
    }

    pub async fn xadd(
        &mut self,
        key: &str,
        id: &str,
        data: &[(String, String)],
        max_len: Option<usize>,
    ) -> redis::RedisResult<String> {
        if let Some(max_len) = max_len {
            redis::cmd("XADD")
                .arg(key)
                .arg("MAXLEN")
                .arg("~")
                .arg(max_len)
                .arg(id)
                .arg(data)
                .query_async(&mut self.connection)
                .await
        } else {
            redis::cmd("XADD")
                .arg(key)
                .arg(id)
                .arg(data)
                .query_async(&mut self.connection)
                .await
        }
    }

    pub async fn xread(
        &mut self,
        count: usize,
        key: &str,
        id: &str,
    ) -> redis::RedisResult<Vec<(String, Vec<(String, String)>)>> {
        let streams: Vec<Stream> = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(0)
            .arg("STREAMS")
            .arg(key)
            .arg(id)
            .query_async(&mut self.connection)
            .await?;
        // Taking the first stream
        let stream = streams.into_iter().next().unwrap();
        Ok(stream
            .entries
            .into_iter()
            .map(|entry| {
                let id = entry.id().unwrap();
                let key_values = entry
                    .key_values
                    .into_iter()
                    .map(|v| redis::from_redis_value::<String>(&v).unwrap())
                    .tuples()
                    .collect();
                (id, key_values)
            })
            .collect())
    }

    pub async fn last_id(&mut self, key: &str) -> redis::RedisResult<Option<String>> {
        let entries: Vec<Entry> = redis::cmd("XREVRANGE")
            .arg(key)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut self.connection)
            .await?;
        Ok(entries.first().map(|e| e.id().unwrap()))
    }
}
