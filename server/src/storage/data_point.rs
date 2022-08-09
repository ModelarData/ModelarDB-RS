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

//! A single data point. Note that this struct is mainly used when converting from a raw MQTT
//! message to Apache Arrow.

use std::fmt;
use std::fmt::Formatter;
use paho_mqtt::Message;

use crate::types::{Timestamp, Value};

pub struct DataPoint {
    pub timestamp: Timestamp,
    pub value: Value,
    pub metadata: Vec<String>,
}

impl DataPoint {
    /// Given a raw MQTT message, extract the message components and return them as a data point.
    /// MQTT messages with a payload format of "[timestamp, value]" are expected.
    pub fn from_message(message: &Message) -> Result<Self, String> {
        let payload = message.payload_str();

        if payload.is_empty() {
            Err("The message is empty.".to_owned())
        } else {
            if payload.chars().next().unwrap() != '[' || payload.chars().last().unwrap() != ']' {
                Err("The message does not have the correct format.".to_owned())
            } else {
                let first_last_off: &str = &payload[1..payload.len() - 1];
                let timestamp_value: Vec<&str> = first_last_off.split(", ").collect();

                if timestamp_value.len() != 2 {
                    Err("The message can only contain a timestamp and a single value.".to_owned())
                } else {
                    if let Ok(timestamp) = timestamp_value[0].parse::<Timestamp>() {
                        if let Ok(value) = timestamp_value[1].parse::<Value>() {
                            Ok(Self {
                                timestamp,
                                value,
                                metadata: vec![message.topic().to_owned().replace("/", "-")],
                            })
                        } else {
                            Err("Value could not be parsed.".to_owned())
                        }
                    } else {
                        Err("Timestamp could not be parsed.".to_owned())
                    }
                }
            }
        }
    }

    // TODO: Change this when we have more information to identify a time series.
    /// Generate a unique key for a time series based on the information in the message.
    pub fn generate_unique_key(&self) -> String {
        self.metadata.join("-")
    }
}

impl fmt::Display for DataPoint {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "(Timestamp: {}, Value: {})", self.timestamp, self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_get_data_point_from_valid_message() {
        let message = get_message_with_payload("[1657878396943245, 30]");
        let result = DataPoint::from_message(&message);

        assert!(result.is_ok());

        let data_point = result.unwrap();
        assert_eq!(data_point.timestamp, 1657878396943245);
        assert_eq!(data_point.value, 30 as f32);
        assert_eq!(data_point.metadata, vec!["ModelarDB-test".to_owned()])
    }

    #[test]
    fn test_cannot_get_data_point_from_empty_message() {
        let message = get_message_with_payload("");
        let result = DataPoint::from_message(&message);

        assert!(result.is_err())
    }

    #[test]
    fn test_cannot_get_data_point_from_message_with_invalid_format() {
        let message = get_message_with_payload("1657878396943245, 30");
        let result = DataPoint::from_message(&message);

        assert!(result.is_err())
    }

    #[test]
    fn test_cannot_get_data_point_from_message_with_multiple_values() {
        let message = get_message_with_payload("[1657878396943245, 30, 40]");
        let result = DataPoint::from_message(&message);

        assert!(result.is_err())
    }

    #[test]
    fn test_cannot_get_data_point_from_message_with_invalid_timestamp() {
        let message = get_message_with_payload("[invalid, 30]");
        let result = DataPoint::from_message(&message);

        assert!(result.is_err())
    }

    #[test]
    fn test_cannot_get_data_point_from_message_with_invalid_value() {
        let message = get_message_with_payload("[1657878396943245, invalid]");
        let result = DataPoint::from_message(&message);

        assert!(result.is_err())
    }

    fn get_message_with_payload(payload: &str) -> Message {
        Message::new("ModelarDB/test", payload, 1)
    }
}
