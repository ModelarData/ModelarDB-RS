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

//! Support for managing all uncompressed data that is ingested into the storage engine.

use std::collections::{HashMap, VecDeque};
use paho_mqtt::Message;
use tracing::{info, info_span};
use crate::storage::data_point::DataPoint;
use crate::storage::segment::{FinishedSegment, SegmentBuilder};

// Note that the capacity has to be a multiple of 64 bytes to avoid the actual capacity
// being larger due to internal alignment when allocating memory for the builders.
const BUILDER_CAPACITY: usize = 64;
const UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES: usize = 5000;

/// Converts raw MQTT messages to uncompressed data points and stores uncompressed data points
/// temporarily in an in-memory buffer that spills to Apache Parquet files. When finished the data
/// is made available for compression.
struct UncompressedDataManager {
    /// Path to the folder containing all uncompressed data managed by the storage engine.
    storage_folder_path: String,
    /// The uncompressed segments while they are being built.
    uncompressed_data: HashMap<String, SegmentBuilder>,
    /// Prioritized queue of finished segments that are ready for compression.
    finished_queue: VecDeque<FinishedSegment>,
    /// How many bytes of memory that are left for storing uncompressed segments.
    uncompressed_remaining_memory_in_bytes: usize,
}

impl UncompressedDataManager {
    pub fn new(storage_folder_path: String) -> Self {
        Self {
            storage_folder_path,
            // TODO: Maybe create with estimated capacity to avoid reallocation.
            uncompressed_data: HashMap::new(),
            finished_queue: VecDeque::new(),
            uncompressed_remaining_memory_in_bytes: UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES,
        }
    }

    /// Parse `message` and insert it into the in-memory buffer. Return Ok if the message was
    /// successfully inserted, otherwise return Err.
    pub fn insert_message(&mut self, message: Message) -> Result<(), String> {
        let data_point = DataPoint::from_message(&message)?;
        let key = data_point.generate_unique_key();
        let _span = info_span!("insert_message", key = key.clone()).entered();

        info!("Inserting data point '{}' into segment.", data_point);

        if let Some(segment) = self.uncompressed_data.get_mut(&key) {
            info!("Found existing segment.");

            segment.insert_data(&data_point);

            if segment.is_full() {
                info!("Segment is full, moving it to the queue of finished segments.");

                // Since this is only reachable if the segment exists in the HashMap, unwrap is safe to use.
                let full_segment = self.uncompressed_data.remove(&key).unwrap();
                self.enqueue_segment(key, full_segment)
            }
        } else {
            info!("Could not find segment. Creating segment.");

            // If there is not enough memory for a new segment, spill a finished segment.
            if SegmentBuilder::get_memory_size() > self.uncompressed_remaining_memory_in_bytes {
                self.spill_finished_segment();
            }

            // Create a new segment and reduce the remaining amount of reserved memory by its size.
            let mut segment = SegmentBuilder::new();
            self.uncompressed_remaining_memory_in_bytes -= SegmentBuilder::get_memory_size();

            info!(
                "Created segment. Remaining reserved bytes: {}.",
                self.uncompressed_remaining_memory_in_bytes
            );

            segment.insert_data(&data_point);
            self.uncompressed_data.insert(key, segment);
        }

        Ok(())
    }

    /// Remove the oldest finished segment from the queue and return it. Return `None` if the queue
    /// of finished segments is empty.
    pub fn get_finished_segment(&mut self) -> Option<FinishedSegment> {
        if let Some(finished_segment) = self.finished_queue.pop_front() {
            // Add the memory size of the removed finished segment back to the remaining bytes.
            self.uncompressed_remaining_memory_in_bytes +=
                finished_segment.uncompressed_segment.get_memory_size();

            Some(finished_segment)
        } else {
            None
        }
    }

    /// Move `segment_builder` to the queue of finished segments.
    fn enqueue_segment(&mut self, key: String, segment_builder: SegmentBuilder) {
        let finished_segment = FinishedSegment {
            key,
            uncompressed_segment: Box::new(segment_builder),
        };

        self.finished_queue.push_back(finished_segment);
    }

    /// Spill the first in-memory finished segment in the queue of finished segments. If no
    /// in-memory finished segments could be found, panic.
    fn spill_finished_segment(&mut self) {
        info!("Not enough memory to create segment. Spilling an already finished segment.");

        // Iterate through the finished segments to find a segment that is in memory.
        for finished in self.finished_queue.iter_mut() {
            if let Ok(path) = finished.spill_to_apache_parquet(self.storage_folder_path.clone()) {
                // Add the size of the segment back to the remaining reserved bytes.
                self.uncompressed_remaining_memory_in_bytes += SegmentBuilder::get_memory_size();

                info!(
                    "Spilled the segment to '{}'. Remaining reserved bytes: {}.",
                    path, self.uncompressed_remaining_memory_in_bytes
                );
                return ();
            }
        }

        // TODO: All uncompressed and compressed data should be saved to disk first.
        // If not able to find any in-memory finished segments, we should panic.
        panic!("Not enough reserved memory to hold all necessary segment builders.");
    }
}
