/* Copyright 2023 The ModelarDB Contributors
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

//! Functions for collecting and using command line arguments in both the server and manager.

use std::sync::Arc;

use object_store::{aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, ObjectStore};

/// Collect the command line arguments that this program was started with.
pub fn collect_command_line_arguments(maximum_arguments: usize) -> Vec<String> {
    let mut args = std::env::args();
    args.next(); // Skip the executable.

    // Collect at most the maximum number of command line arguments plus one. The plus one argument
    // is collected to trigger the default pattern when parsing the command line arguments with
    // pattern matching, making it possible to handle errors caused by too many arguments.
    args.by_ref().take(maximum_arguments + 1).collect()
}

/// Create an [`ObjectStore`] that represents the remote path in `argument`.
pub fn argument_to_remote_object_store(argument: &str) -> Result<Arc<dyn ObjectStore>, String> {
    match argument.split_once("://") {
        Some(("s3", bucket_name)) => {
            let object_store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket_name)
                .build()
                .map_err(|error| error.to_string())?;

            Ok(Arc::new(object_store))
        }
        Some(("azureblobstorage", container_name)) => {
            let object_store = MicrosoftAzureBuilder::from_env()
                .with_container_name(container_name)
                .build()
                .map_err(|error| error.to_string())?;

            Ok(Arc::new(object_store))
        }
        _ => Err(
            "Remote data folder must be s3://bucket-name or azureblobstorage://container-name."
                .to_owned(),
        ),
    }
}
