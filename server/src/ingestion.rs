//! Module containing support for data ingestion from an MQTT broker.
//!
//! The connection settings of the ingestor are controlled through the broker, client, topics,
//! and qos fields. Note that the client ID should be unique.
//!
//! To use the ingestor, first create the client, then use the created client to connect to the
//! the broker and subscribe to the specific topics. Connecting returns a message stream that
//! can be looped over to ingest the messages that are published to the topics.
//!

/* Copyright 2021 The MiniModelarDB Contributors
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
use futures::{executor::block_on, stream::StreamExt};
use paho_mqtt as mqtt;
use std::{process, time::Duration};
use paho_mqtt::{AsyncClient, Message, Receiver};

pub struct Ingestor {
    broker: &'static str,
    client: &'static str,
    topics: &'static [&'static str],
    qos: [i32],
}

// TODO: Add debug logging with tracer log.
impl Ingestor {
    /// Create a broker client with the specified Ingestor fields.
    pub fn create_client(self) -> AsyncClient {
        let create_options = mqtt::CreateOptionsBuilder::new()
            .server_uri(self.broker)
            .client_id(self.client)
            .finalize();

        let mut client = mqtt::AsyncClient::new(create_options).unwrap_or_else(|e| {
            println!("Error creating the client: {:?}", e);
            process::exit(1);
        });

        client
    }

    /// Make the connection to the broker and subscribe to the specified topics.
    pub fn subscribe_to_broker(self, client: &mut AsyncClient) -> async_channel::Receiver<Option<Message>> {
        // Get message stream before connecting.
        let mut stream = client.get_stream(25);

        // Define last will and testament message to notify other clients about disconnect.
        let lwt = mqtt::Message::new("mdb_lwt", "ModelarDB lost connection", mqtt::QOS_1);

        let connect_options = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(30))
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        client.connect(connect_options).await?;
        client.subscribe_many(self.topics, &self.qos).await?;

        stream
    }
}