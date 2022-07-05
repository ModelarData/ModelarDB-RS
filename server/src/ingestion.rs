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

pub struct Ingestor {

}