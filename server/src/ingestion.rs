//! Module containing support for data ingestion from an MQTT broker.
//!
//! The connection settings of the ingestor are controlled through the broker, client_id, topics,
//! and qos fields. Note that the client ID should be unique.
//!
//! To use the ingestor, run the "ingestor.start()" function. This functions first creates the client.
//! Then it uses the created client to connect to the broker and subscribe to the specified topics.
//! The connection message stream is then looped over to ingest the messages that are published to the topics.
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
use paho_mqtt::{AsyncClient, AsyncReceiver, Message};
use std::{process, time::Duration};

pub struct Ingestor {
    pub broker: &'static str,
    pub client_id: &'static str,
    pub topics: &'static [&'static str],
    pub qos: &'static [i32],
}

impl Ingestor {
    /// Create a broker client, subscribe to the topics and start ingesting messages.
    ///
    /// # Arguments
    /// * `compress_callback` - Function called on the messages when a batch of messages has been
    /// ingested into memory. Due to possible memory limitations, the messages can also be supplied
    /// to the compress callback through a file path.
    pub fn start(self, compress_callback: fn(msg: Message)) {
        println!("Creating MQTT broker client.");
        let mut client = create_client(self.broker, self.client_id);

        if let Err(err) = block_on(async {
            let mut stream = subscribe_to_broker(&mut client, self.topics, self.qos).await;

            println!("Waiting for messages...");
            ingest_messages(&mut stream, &mut client, compress_callback).await;

            Ok::<(), mqtt::Error>(())
        }) {
            eprintln!("{}", err);
        }
    }
}

/** Private Functions **/
/// Create a broker client with the given broker URI and client ID.
fn create_client(broker: &str, client_id: &str) -> AsyncClient {
    let create_options = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker)
        .client_id(client_id)
        .finalize();

    let mut client = mqtt::AsyncClient::new(create_options).unwrap_or_else(|e| {
        eprintln!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    client
}

/// Make the connection to the broker and subscribe to the specified topics.
async fn subscribe_to_broker(
    client: &mut AsyncClient,
    topics: &[&str],
    qos: &[i32],
) -> AsyncReceiver<Option<Message>> {
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
    client.subscribe_many(topics, qos).await;

    stream
}

// TODO: Send the messages to the storage engine when they are retrieved.
/// Ingest the published messages in a loop until connection is lost.
async fn ingest_messages(
    stream: &mut AsyncReceiver<Option<Message>>,
    client: &mut AsyncClient,
    compress_callback: fn(msg: Message),
) {
    // While the message stream resolves to the next item in the stream, ingest the messages.
    while let Some(msg_opt) = stream.next().await {
        if let Some(msg) = msg_opt {
            compress_callback(msg)
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
