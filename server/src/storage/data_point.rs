use paho_mqtt::Message;
use crate::storage::{MetaData, Timestamp, Value};

#[derive(Debug)]
pub struct DataPoint {
    pub timestamp: Timestamp,
    pub value: Value,
    pub metadata: MetaData,
}

impl DataPoint {
    /// Given a raw MQTT message, extract the message components and return them as a data point.
    pub fn from_message(message: &Message) -> DataPoint {
        let message_payload = message.payload_str();
        let first_last_off: &str = &message_payload[1..message_payload.len() - 1];

        let timestamp_value: Vec<&str> = first_last_off.split(", ").collect();
        let timestamp = timestamp_value[0].parse::<Timestamp>().unwrap();
        let value = timestamp_value[1].parse::<Value>().unwrap();

        DataPoint {
            timestamp,
            value,
            metadata: vec![message.topic().to_string()],
        }
    }

    // TODO: Currently the only information we have to uniquely identify a sensor is the ID. If this changes, change this function.
    /// Generates an unique key for a time series based on the information in the message.
    pub fn generate_unique_key(&self) -> String {
        self.metadata.join("-")
    }
}
