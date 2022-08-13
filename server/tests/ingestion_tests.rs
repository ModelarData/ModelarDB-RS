use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use datafusion::arrow::array;

use rand::Rng;
use datafusion::arrow::array::{Float32Array, PrimitiveArray, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimestampMillisecondType};
use datafusion::arrow::datatypes::TimeUnit::Millisecond;
use datafusion::arrow::record_batch::RecordBatch;
use proptest::collection::vec;

#[test]
fn test_ingest_message_into_storage_engine() {
    // TODO: Send a single message to do_put.
    // TODO: Assert that a new segment has been created in the storage engine.
}

#[test]
fn test_can_ingest_multiple_time_series_into_storage_engine() {
    // TODO: Send multiple messages from multiple different time series to do_put.
    // TODO: Assert that multiple new segments have been created in the storage engine.
}

#[test]
fn test_can_ingest_full_segment() {
    // TODO: Send BUILDER_CAPACITY messages from the same time series to do_put.
    // TODO: Assert that the full segment has been made available for compression.
}

#[test]
fn test_cannot_ingest_invalid_message() {
    // TODO: Send a single message to do put with the wrong schema.
    // TODO: Assert that the storage engine is empty.
}

#[test]
fn test_can_compress_ingested_segment() {
    // TODO: Send BUILDER_CAPACITY messages from the same time series to do_put.
    // TODO: Assert that the segment is compressed.
}

#[test]
fn test_can_compress_multiple_ingested_segments() {
    // TODO: Send BUILDER_CAPACITY messages from multiple time series to do_put.
    // TODO: Assert that all segments have been compressed.
}

#[test]
fn test_can_query_ingested_uncompressed_data() {
    // TODO: Send a single message to do_put.
    // TODO: Use the query engine to query from the time series the message belongs to.
}

#[test]
fn test_can_query_ingested_compressed_data() {
    // TODO: Send BUILDER_CAPACITY messages from the same time series to do_put.
    // TODO: Use the query engine to query from the time series the messages belong to.
}

#[test]
fn test_can_query_ingested_uncompressed_and_compressed_data() {
    // TODO: Send BUILDER_CAPACITY + 1 messages from the same time series to do_put.
    // TODO: Use the query engine to query from the time series the messages belong to.
    // TODO: Ensure that the uncompressed message is part of the query result.
}

/// Generate `count` random messages for the time series referenced by `key` and send it to the
/// ModelarDB server with Arrow Flight through the `do_put()` endpoint.
fn send_messages_to_server(key: String) {

}

/// Generate a [`RecordBatch`] with the current timestamp, a random value, and `key`.
fn generate_random_message(key: String) -> RecordBatch {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    let value = rand::thread_rng().gen_range(0..100) as f32;

    let message_schema = Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
        Field::new("value", DataType::Float32, false),
        Field::new("key",DataType::Utf8, false),
    ]);

    RecordBatch::try_new(
        Arc::new(message_schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
            Arc::new(Float32Array::from(vec![value])),
            Arc::new(StringArray::from(vec![key])),
        ]
    ).unwrap()
}