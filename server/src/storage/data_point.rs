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

//! Support for a single internal data point. Note that this struct is mainly
//! used when transitioning from a raw message into the in-memory representation.

use crate::storage::{MetaData, Timestamp, Value};
use paho_mqtt::Message;

#[derive(Debug)]
pub struct DataPoint {
    pub timestamp: Timestamp,
    pub value: Value,
    pub metadata: MetaData,
}

impl DataPoint {
    /// Given a raw MQTT message, extract the message components and return them as a data point.
    pub fn from_message(message: &Message) -> Result<Self, String> {
        let payload = message.payload_str();

        if payload.chars().next().unwrap() != '[' || payload.chars().last().unwrap() != ']' {
            Err("The message does not have the correct format.".to_string())
        } else {
            let first_last_off: &str = &payload[1..payload.len() - 1];
            let timestamp_value: Vec<&str> = first_last_off.split(", ").collect();

            if timestamp_value.len() != 2 {
                Err("The message can only contain a timestamp and a single value.".to_string())
            } else {
                if let Ok(timestamp) = timestamp_value[0].parse::<Timestamp>() {
                    if let Ok(value) = timestamp_value[1].parse::<Value>() {
                        Ok(Self {
                            timestamp,
                            value,
                            metadata: vec![message.topic().to_string().replace("/", "-")],
                        })
                    } else {
                        Err("Value could not be parsed.".to_string())
                    }
                } else {
                    Err("Timestamp could not be parsed.".to_string())
                }
            }
        }
    }

    // TODO: Currently the only information we have to uniquely identify a sensor is the topic.
    //       If this changes, change this function.
    /// Generates an unique key for a time series based on the information in the message.
    pub fn generate_unique_key(&self) -> String {
        self.metadata.join("-")
    }
}

// TODO: Test for getting a data point from a message.
