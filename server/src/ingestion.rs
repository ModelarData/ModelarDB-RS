/* Copyright 2022 The MiniModelarDB Contributors
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

//! Support for data ingestion from an MQTT broker.
//!
//! To use the ingestor, run the "ingestor.start()" function. This function first creates the client.
//! Then it uses the created client to connect to the broker and subscribe to the specified topics.
//! The connection message stream is then looped over to ingest the messages that are published to the topics.

use futures::{executor::block_on, stream::StreamExt};
use paho_mqtt as mqtt;
use paho_mqtt::{AsyncClient, AsyncReceiver, Message};
use std::{process, time::Duration};

/// A single MQTT client that can subscribe to the specified broker and ingest messages from the
/// specified topics. Note that after creation, the ingestor needs to be started to ingest messages.
pub struct Ingestor {
    /// Server URI for the MQTT broker.
    pub broker: &'static str,
    /// ID that is used to uniquely identify the ingestor as a client to the MQTT broker.
    pub client_id: &'static str,
    /// Specific topics that should be subscribed to. Use "\[*]" to subscribe to all topics.
    pub topics: &'static [&'static str],
    /// The quality of service for each subscribed-to topic. Should be of same length as `topics` field.
    pub qos: &'static [i32],
}

impl Ingestor {
    /// Create a broker client, subscribe to the topics and start ingesting messages.
    pub fn start(self) {
        println!("Creating MQTT broker client.");
        let mut client = self.create_client();

        if let Err(err) = block_on(async {
            let mut stream = self.subscribe_to_broker(&mut client).await;

            println!("Waiting for messages...");
            ingest_messages(&mut stream, &mut client).await;

            Ok::<(), mqtt::Error>(())
        }) {
            eprintln!("{}", err);
        }
    }

    /// Create a broker client with the given broker URI and client ID.
    fn create_client(&self) -> AsyncClient {
        let create_options = mqtt::CreateOptionsBuilder::new()
            .server_uri(self.broker)
            .client_id(self.client_id)
            .finalize();

        mqtt::AsyncClient::new(create_options).unwrap_or_else(|e| {
            eprintln!("Error creating the client: {:?}", e);
            process::exit(1);
        })
    }

    /// Make the connection to the broker and subscribe to the specified topics.
    async fn subscribe_to_broker(&self, client: &mut AsyncClient) -> AsyncReceiver<Option<Message>> {
        // Get message stream before connecting since messages can arrive as soon as the connection is made.
        let mut stream = client.get_stream(25);

        // Define last will and testament message to notify other clients about disconnect.
        let lwt = mqtt::Message::new("mdb_lwt", "ModelarDB lost connection", mqtt::QOS_1);

        let connect_options = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(30))
            .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        println!("Connecting to MQTT broker.");
        client.connect(connect_options).await;

        println!("Subscribing to topics: {:?}", topics);
        client.subscribe_many(self.topics, self.qos).await;

        stream
    }
}

// TODO: Send the messages to the storage engine when they are retrieved.
/// Ingest the published messages in a loop until connection is lost.
async fn ingest_messages(stream: &mut AsyncReceiver<Option<Message>>, client: &mut AsyncClient) {
    // While the message stream resolves to the next item in the stream, ingest the messages.
    while let Some(msg_opt) = stream.next().await {
        if let Some(msg) = msg_opt {
            println!("{}", msg);
        } else {
            // A "None" means we were disconnected. Try to reconnect...
            println!("Lost connection. Attempting reconnect.");
            while let Err(err) = client.reconnect().await {
                eprintln!("Error reconnecting: {}", err);
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    }
}
