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

//! Functionality for validating connections to remote data folders.

use std::str::FromStr;
use std::sync::Arc;

use object_store::{path::Path, ObjectStore};

/// The object stores that are currently supported as remote data folders.
#[derive(PartialEq, Eq)]
pub enum RemoteDataFolderType {
    S3,
    AzureBlobStorage,
}

impl FromStr for RemoteDataFolderType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "s3" => Ok(RemoteDataFolderType::S3),
            "azureblobstorage" => Ok(RemoteDataFolderType::AzureBlobStorage),
            _ => Err(format!(
                "'{}' is not a valid value for RemoteDataFolderType.",
                value
            )),
        }
    }
}

/// Extract the remote data folder type from the arguments and validate that the remote data folder can be
/// accessed. If the remote data folder cannot be accessed, return the error that occurred as a [`String`].
pub async fn validate_remote_data_folder_from_argument(
    argument: &str,
    remote_data_folder: &Arc<dyn ObjectStore>,
) -> Result<(), String> {
    // unwrap() is safe since if there is a remote data folder, there is always a valid third argument.
    let object_store_type = argument.split_once("://").unwrap().0;
    let remote_data_folder_type = RemoteDataFolderType::from_str(object_store_type).unwrap();

    validate_remote_data_folder(remote_data_folder_type, remote_data_folder).await
}

/// Validate that the remote data folder can be accessed. If the remote data folder cannot be
/// accessed, return the error that occurred as a [`String`].
pub async fn validate_remote_data_folder(
    remote_data_folder_type: RemoteDataFolderType,
    remote_data_folder: &Arc<dyn ObjectStore>,
) -> Result<(), String> {
    // Check that the connection is valid by attempting to retrieve a file that does not exist.
    match remote_data_folder.get(&Path::from("")).await {
        Ok(_) => Ok(()),
        Err(error) => match error {
            object_store::Error::NotFound { .. } => {
                // Note that for Azure Blob Storage the same error is returned if the object was not
                // found due to the object not existing and due to the container not existing.
                if remote_data_folder_type == RemoteDataFolderType::AzureBlobStorage {
                    Ok(())
                } else {
                    Err(error.to_string())
                }
            }
            _ => Err(error.to_string()),
        },
    }
}
