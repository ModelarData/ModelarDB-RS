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

//! Compress time series

use std::sync::Arc;

use datafusion::arrow::array::{BinaryBuilder, Float32Builder, UInt8Array, UInt8Builder};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

use crate::models;
use crate::models::{gorilla::Gorilla, pmcmean::PMCMean, swing::Swing, ErrorBound};
use crate::types::{
    ArrowTimestamp, ArrowValue, Timestamp, TimestampArray, TimestampBuilder, Value, ValueArray,
    ValueBuilder,
};

pub fn compress(error_bound: &ErrorBound, record_batch: &RecordBatch) -> RecordBatch {
    // Extract the timestamps and values to compress.
    let uncompressed_timestamps = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampArray>()
        .unwrap();
    let uncompressed_values = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<ValueArray>()
        .unwrap();

    // num_rows is allocated to not reallocate as one model is created per data
    // point in the worst cast, however, usually significantly fewer is used.
    let num_rows = record_batch.num_rows();
    let mut compressed_record_batch_builder = CompressedRecordBatchBuilder::new(num_rows);

    // Compress the timestamps and values.
    let mut current_index = 0;

    while current_index < num_rows {
        let compressed_segment_builder = build_next_segment(
            current_index,
            num_rows,
            error_bound,
            &uncompressed_timestamps,
            &uncompressed_values,
        );

        let compressed_segment_builder_length = compressed_segment_builder.length;

        compressed_segment_builder.finish(
            current_index,
            &uncompressed_timestamps,
            &uncompressed_values,
            &mut compressed_record_batch_builder,
        );
        current_index += compressed_segment_builder_length;
    }
    compressed_record_batch_builder.finnish()
}

fn build_next_segment(
    mut current_index: usize,
    end_index: usize,
    error_bound: &ErrorBound,
    timestamps: &TimestampArray,
    values: &ValueArray,
) -> CompressedSegmentBuilder {
    let mut compressed_segment_builder = CompressedSegmentBuilder::new(error_bound.clone());
    while compressed_segment_builder.can_fit_more() && current_index < end_index {
        let timestamp = timestamps.value(current_index);
        let value = values.value(current_index);
        compressed_segment_builder.fit_data_point(timestamp, value);
        current_index += 1;
    }
    compressed_segment_builder
}

struct CompressedSegmentBuilder {
    length: usize,
    pmc_mean: PMCMean,
    pmc_mean_has_fit_all: bool,
    swing: Swing,
    swing_has_fit_all: bool,
    gorilla: Gorilla,
    gorilla_maximum_length: usize, // TODO: remove temporary maximum length.
}

impl CompressedSegmentBuilder {
    fn new(error_bound: ErrorBound) -> Self {
        Self {
            length: 0,
            pmc_mean: PMCMean::new(error_bound),
            pmc_mean_has_fit_all: true,
            swing: Swing::new(error_bound),
            swing_has_fit_all: true,
            gorilla: Gorilla::new(),
            gorilla_maximum_length: 50, // Temporary maximum length.
        }
    }

    fn fit_data_point(&mut self, timestamp: Timestamp, value: Value) {
        self.pmc_mean_has_fit_all = self.pmc_mean_has_fit_all && self.pmc_mean.fit_value(value);
        self.swing_has_fit_all =
            self.swing_has_fit_all && self.swing.fit_data_point(timestamp, value);
        self.gorilla.compress_value(value); // Gorilla is lossless.
        self.length += 1;
    }

    fn can_fit_more(&self) -> bool {
        self.pmc_mean_has_fit_all
            || self.swing_has_fit_all
            || self.length < self.gorilla_maximum_length
    }

    fn finish(
        self,
        start_index: usize,
        uncompressed_timestamps: &TimestampArray,
        uncompressed_values: &ValueArray,
        compressed_record_batch_builder: &mut CompressedRecordBatchBuilder,
    ) {
        let end_index = start_index + self.length;

        // Timestamps
        let start_time = uncompressed_timestamps.value(start_index);
        let end_time = uncompressed_timestamps.value(end_index - 1);
        let timestamps = &[]; // TODO: compress irregular timestamps.

        // The model that uses the fewest number of bytes per value is stored.
        let (model_type_id, min_value, max_value, values) = models::select_model(
            self.pmc_mean,
            self.swing,
            self.gorilla,
            &uncompressed_values.values()[start_index..end_index],
        );

        // TODO: compute and store the actual error.
        let error = 10.0;

        compressed_record_batch_builder.append_segment(
            model_type_id,
            timestamps,
            start_time,
            end_time,
            &values,
            min_value,
            max_value,
            error,
        );
    }
}

struct CompressedRecordBatchBuilder {
    model_type_ids: UInt8Builder,
    timestamps: BinaryBuilder,
    start_times: TimestampBuilder,
    end_times: TimestampBuilder,
    values: BinaryBuilder,
    min_values: ValueBuilder,
    max_values: ValueBuilder,
    error: Float32Builder,
}

impl CompressedRecordBatchBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            model_type_ids: UInt8Builder::new(capacity),
            timestamps: BinaryBuilder::new(capacity),
            start_times: TimestampBuilder::new(capacity),
            end_times: TimestampBuilder::new(capacity),
            values: BinaryBuilder::new(capacity),
            min_values: ValueBuilder::new(capacity),
            max_values: ValueBuilder::new(capacity),
            error: Float32Builder::new(capacity),
        }
    }

    fn append_segment(
        &mut self,
        model_type_id: u8,
        timestamps: &[u8],
        start_time: Timestamp,
        end_time: Timestamp,
        values: &[u8],
        min_value: Value,
        max_value: Value,
        error: f32,
    ) {
        // unwrap() is used as append_value() never returns Error for
        // PrimitiveBuilder.
        self.model_type_ids.append_value(model_type_id).unwrap();
        self.timestamps.append_value(timestamps).unwrap();
        self.start_times.append_value(start_time).unwrap();
        self.end_times.append_value(end_time).unwrap();
        self.values.append_value(values).unwrap();
        self.min_values.append_value(min_value).unwrap();
        self.max_values.append_value(max_value).unwrap();
        self.error.append_value(error).unwrap();
    }

    fn finnish(mut self) -> RecordBatch {
        RecordBatch::try_new(
            CompressedRecordBatchBuilder::get_compressed_segment_schema_ref(),
            vec![
                Arc::new(self.model_type_ids.finish()),
                Arc::new(self.timestamps.finish()),
                Arc::new(self.start_times.finish()),
                Arc::new(self.end_times.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.min_values.finish()),
                Arc::new(self.max_values.finish()),
                Arc::new(self.error.finish()),
            ],
        )
        .unwrap()
    }

    fn get_compressed_segment_schema_ref() -> SchemaRef {
        // TODO: share with time_series.rs when refactoring `StorageEngine`.
        Arc::new(Schema::new(vec![
            Field::new("model_type_id", DataType::UInt8, false),
            Field::new("timestamps", DataType::Binary, false),
            Field::new("start_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("end_time", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", DataType::Binary, false),
            Field::new("min_value", ArrowValue::DATA_TYPE, false),
            Field::new("max_value", ArrowValue::DATA_TYPE, false),
            Field::new("error", DataType::Float32, false),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{collection, prop_assert, prop_assert_eq, prop_assume, proptest};

    // Tests compress.
    #[test]
    fn test_compress_empty_time_series() {
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let uncompressed_data = create_uncompressed_record_batch(&[], &[]);
        let compressed_data = compress(&error_bound, &uncompressed_data);
        assert_eq!(0, compressed_data.num_rows())
    }

    #[test]
    fn test_compress_regular_constant_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![10.0; timestamps.len()];
        let uncompressed_record_batch = create_uncompressed_record_batch(&timestamps, &values);
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch = compress(&error_bound, &uncompressed_record_batch);
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_record_batch,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_compress_regular_almost_constant_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![10.1, 10.0, 10.2, 10.2, 10.0, 10.1, 10.0, 10.0, 10.0];
        let uncompressed_record_batch = create_uncompressed_record_batch(&timestamps, &values);
        let error_bound = ErrorBound::try_new(5.0).unwrap();
        let compressed_record_batch = compress(&error_bound, &uncompressed_record_batch);
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_record_batch,
            &compressed_record_batch,
            &[models::PMC_MEAN_ID],
        )
    }

    #[test]
    fn test_compress_regular_linear_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = Vec::from_iter((10..100).step_by(10).map(|v| v as f32));
        let uncompressed_record_batch = create_uncompressed_record_batch(&timestamps, &values);
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch = compress(&error_bound, &uncompressed_record_batch);
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_record_batch,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_compress_regular_almost_linear_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![10.0, 20.0, 30.1, 40.8, 51.0, 60.2, 70.1, 80.7, 90.4];
        let uncompressed_record_batch = create_uncompressed_record_batch(&timestamps, &values);
        let error_bound = ErrorBound::try_new(5.0).unwrap();
        let compressed_record_batch = compress(&error_bound, &uncompressed_record_batch);
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_record_batch,
            &compressed_record_batch,
            &[models::SWING_ID],
        )
    }

    #[test]
    fn test_compress_regular_random_time_series() {
        let timestamps = Vec::from_iter((100..1000).step_by(100));
        let values = vec![7.47, 13.34, 14.50, 4.88, 7.84, 6.69, 8.63, 5.109, 2.16];
        let uncompressed_record_batch = create_uncompressed_record_batch(&timestamps, &values);
        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch = compress(&error_bound, &uncompressed_record_batch);
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_record_batch,
            &compressed_record_batch,
            &[models::GORILLA_ID],
        )
    }

    #[test]
    fn test_compress_regular_random_linear_constant_time_series() {
        let mut constant = vec![10.0; 100];
        let mut linear = Vec::from_iter((10..1000).step_by(10).map(|v| v as f32));
        let mut random = vec![7.47, 13.34, 14.50, 4.88, 7.84, 6.69, 8.63, 5.109, 2.16];

        let mut values = vec![];
        values.append(&mut random);
        values.append(&mut linear);
        values.append(&mut constant);

        // TODO: make all tests use an auto generated set of timestamps based on the length of values.
        let timestamps = Vec::from_iter((100..(values.len() + 1) as i64 * 100).step_by(100));
        let uncompressed_record_batch = create_uncompressed_record_batch(&timestamps, &values);

        let error_bound = ErrorBound::try_new(0.0).unwrap();
        let compressed_record_batch = compress(&error_bound, &uncompressed_record_batch);
        assert_compressed_record_batch_with_segments_from_regular_time_series(
            &uncompressed_record_batch,
            &compressed_record_batch,
            &[models::GORILLA_ID, models::SWING_ID, models::PMC_MEAN_ID],
        )
    }

    fn create_uncompressed_record_batch(timestamps: &[Timestamp], values: &[Value]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("timestamps", ArrowTimestamp::DATA_TYPE, false),
            Field::new("values", ArrowValue::DATA_TYPE, false),
        ]);

        let mut timestamps_builder = TimestampBuilder::new(timestamps.len());
        timestamps_builder.append_slice(timestamps).unwrap();
        let mut values_builder = ValueBuilder::new(values.len());
        values_builder.append_slice(values).unwrap();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(timestamps_builder.finish()),
                Arc::new(values_builder.finish()),
            ],
        )
        .unwrap()
    }

    fn assert_compressed_record_batch_with_segments_from_regular_time_series(
        uncompressed_record_batch: &RecordBatch,
        compressed_record_batch: &RecordBatch,
        expected_model_type_ids: &[u8],
    ) {
        let uncompressed_timestamps = uncompressed_record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampArray>()
            .unwrap();
        let sampling_interval = uncompressed_timestamps.value(1) - uncompressed_timestamps.value(0);

        assert_eq!(
            expected_model_type_ids.len(),
            compressed_record_batch.num_rows()
        );

        let mut total_compressed_length = 0;
        for segment in 0..expected_model_type_ids.len() {
            let expected_model_type_id = expected_model_type_ids[segment];
            let model_type_id = compressed_record_batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(segment);
            assert_eq!(expected_model_type_id, model_type_id);

            let start_time = compressed_record_batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampArray>()
                .unwrap()
                .value(segment);
            let end_time = compressed_record_batch
                .column(3)
                .as_any()
                .downcast_ref::<TimestampArray>()
                .unwrap()
                .value(segment);
            total_compressed_length +=
                models::length(start_time, end_time, sampling_interval as i32);
        }
        assert_eq!(
            uncompressed_record_batch.num_rows(),
            total_compressed_length
        );
    }
}
