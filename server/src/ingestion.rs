/* Copyright 2022 The ModelarDB Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Support for data ingestion from an MQTT broker. To use the ingestor, run the `ingestor.start()`
//! method. This method first creates the client. Then it uses the created client to connect to the
//! broker and subscribe to the specified topics. The connection's message stream is then looped
//! over to ingest the messages that are published to the topics.

use std::{process, time::Duration};

use futures::{executor::block_on, stream::StreamExt};
use paho_mqtt as mqtt;
use paho_mqtt::{AsyncClient, AsyncReceiver, Message};
use tracing::{error, info};

use crate::storage::StorageEngine;

/// A single MQTT client that can subscribe to `broker` and ingest messages from `topics`. Note that
/// after creation, the ingestor needs to be started to ingest messages.
pub struct Ingestor {
    /// URI for the MQTT broker.
    broker: String,
    /// ID that is used to uniquely identify the ingestor as a client to the MQTT broker.
    client_id: String,
    /// Specific topics that should be subscribed to. Use `[*]` to subscribe to all topics.
    topics: Vec<String>,
    /// The quality of service for each subscribed-to topic.
    qos: Vec<i32>,
}

impl Ingestor {
    /// Create a new ingestor. Note that `topics` and `qos` must be of the same length.
    pub fn try_new(
        broker: String,
        client_id: String,
        topics: Vec<String>,
        qos: Vec<i32>,
    ) -> Result<Self, String> {
        if topics.len() != qos.len() {
            Err("The number of topics must be the same as the number of QoS specifiers.".to_string())
        } else {
            Ok(Self {
                broker,
                client_id,
                topics,
                qos,
            })
        }
    }

    /// Create a broker client, subscribe to the topics, and start ingesting messages.
    pub fn start(self, mut storage_engine: StorageEngine) {
        info!("Creating MQTT broker client with id '{}'.", self.client_id);
        let mut client = self.create_client();

        if let Err(err) = block_on(async {
            let mut stream = self.subscribe_to_broker(&mut client).await;

            info!("Waiting for messages...");
            Self::ingest_messages(&mut stream, &mut storage_engine).await;

            Ok::<(), mqtt::Error>(())
        }) {
            error!("{}", err);
        }
    }

    /// Create a broker client with the ingestors broker URI and client ID.
    fn create_client(&self) -> AsyncClient {
        let create_options = mqtt::CreateOptionsBuilder::new()
            .server_uri(self.broker.clone())
            .client_id(self.client_id.clone())
            .finalize();

        mqtt::AsyncClient::new(create_options).unwrap_or_else(|e| {
            error!("Error creating the client: {:?}", e);
            process::exit(1);
        })
    }

    /// Make the connection to the broker and subscribe to the specified topics.
    async fn subscribe_to_broker(
        &self,
        client: &mut AsyncClient,
    ) -> AsyncReceiver<Option<Message>> {
        // Get the message stream before connecting since messages can arrive as soon as the connection
        // is made. A buffer size of 25 is used since it is the standard in paho_mqtt examples.
        let mut stream = client.get_stream(25);

        // Define the last will and testament message to notify other clients about disconnect.
        let lwt = mqtt::Message::new("modelardbd_lwt", "ModelarDB lost connection", mqtt::QOS_1);

        let connect_options = mqtt::ConnectOptionsBuilder::new()
            // An interval of 30 seconds is used since it is the standard in paho_mqtt examples.
            .keep_alive_interval(Duration::from_secs(30))
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        info!("Connecting to MQTT broker with URI '{}'.", self.broker);
        client.connect(connect_options).await;

        info!("Subscribing to topics: {:?}", self.topics);
        client.subscribe_many(self.topics.as_slice(), self.qos.as_slice()).await;

        stream
    }

    /// Ingest the published messages in a loop until the connection to the MQTT broker is lost.
    async fn ingest_messages(
        stream: &mut AsyncReceiver<Option<Message>>,
        storage_engine: &mut StorageEngine,
    ) {
        // While the message stream returns the next message in the stream, ingest the messages.
        while let Some(msg_opt) = stream.next().await {
            if let Some(msg) = msg_opt {
                storage_engine.insert_message(msg);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_create_ingestor() {
        let ingestor = Ingestor::try_new(
            "broker".to_string(),
            "client".to_string(),
            vec!["topic_1".to_string(), "topic_2".to_string()],
            vec![1, 1],
        );

        assert!(ingestor.is_ok())
    }

    #[test]
    fn test_topics_and_qos_cannot_be_different_length() {
        let ingestor = Ingestor::try_new(
            "broker".to_string(),
            "client".to_string(),
            vec!["topic_1".to_string(), "topic_2".to_string()],
            vec![1, 1, 1],
        );

        assert!(ingestor.is_err())
    }
}
