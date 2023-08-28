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
//! Functionality for validating remote data folders extracted from arguments is also provided.

use std::env;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use object_store::{aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, path::Path, ObjectStore};
use tonic::Status;

const REMOTE_DATA_FOLDER_ERROR: &str =
    "Remote data folder must be s3://bucket-name or azureblobstorage://container-name.";

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
        _ => Err(REMOTE_DATA_FOLDER_ERROR.to_owned()),
    }
}

/// Create a vector of bytes that represents the connection information to the remote path in `argument`.
pub fn argument_to_connection_info(argument: &str) -> Result<Vec<u8>, String> {
    match argument.split_once("://") {
        Some(("s3", bucket_name)) => {
            let endpoint = env::var("AWS_ENDPOINT").map_err(|error| error.to_string())?;
            let access_key_id = env::var("AWS_ACCESS_KEY_ID").map_err(|error| error.to_string())?;
            let secret_access_key =
                env::var("AWS_SECRET_ACCESS_KEY").map_err(|error| error.to_string())?;

            let credentials = [
                endpoint.as_str(),
                bucket_name,
                access_key_id.as_str(),
                secret_access_key.as_str(),
            ];

            Ok(credentials
                .iter()
                .flat_map(|credential| encode_argument(credential))
                .collect())
        }
        Some(("azureblobstorage", container_name)) => {
            let account =
                env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|error| error.to_string())?;
            let access_key =
                env::var("AZURE_STORAGE_ACCESS_KEY").map_err(|error| error.to_string())?;

            let credentials = [account.as_str(), access_key.as_str(), container_name];

            Ok(credentials
                .iter()
                .flat_map(|credential| encode_argument(credential))
                .collect())
        }
        _ => Err(REMOTE_DATA_FOLDER_ERROR.to_owned()),
    }
}

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
    if let Some(split_argument) = argument.split_once("://") {
        let object_store_type = split_argument.0;
        let remote_data_folder_type = RemoteDataFolderType::from_str(object_store_type)?;

        validate_remote_data_folder(remote_data_folder_type, remote_data_folder).await
    } else {
        Err(format!(
            "Remote data folder argument '{argument}' is invalid."
        ))
    }
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

/// Convert the given `argument` into padded bytes that contain the length of the byte
/// representation of `argument` together with the byte representation. The length is exactly two
/// bytes long.
pub fn encode_argument(argument: &str) -> Vec<u8> {
    let argument_bytes: Vec<u8> = argument.as_bytes().into();
    let mut argument_size_bytes = vec![0; 2];

    // unwrap() is safe since the buffer is in memory and no I/O errors can occur.
    argument_size_bytes
        .write(&argument_bytes.len().to_be_bytes())
        .unwrap();

    [
        &argument_size_bytes[(argument_size_bytes.len() - 2)..],
        argument_bytes.as_slice(),
    ]
    .concat()
}

/// Assumes `data` is a slice containing one or more arguments with the following format:
/// size of argument (2 bytes) followed by the argument (size bytes). Returns a tuple containing
/// the first argument and `data` with the extracted argument's bytes removed.
pub fn extract_argument(data: &[u8]) -> Result<(&str, &[u8]), Status> {
    let size_bytes: [u8; 2] = data[..2]
        .try_into()
        .map_err(|_| Status::internal("Size of argument is not 2 bytes."))?;

    let size = u16::from_be_bytes(size_bytes) as usize;

    let argument = std::str::from_utf8(&data[2..(size + 2)])
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let remaining_bytes = &data[(size + 2)..];

    Ok((argument, remaining_bytes))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode_argument() {
        let encoded_argument = encode_argument("test");
        let (argument_size_bytes, argument_bytes) = encoded_argument.split_at(2);

        assert_eq!(argument_size_bytes, [0, 4]);
        assert_eq!(argument_bytes, b"test");
    }

    #[test]
    fn test_extract_arguments() {
        let encoded_argument_1 = encode_argument("argument_1");
        let encoded_argument_2 = encode_argument("argument_2");

        let data = [encoded_argument_1.as_slice(), encoded_argument_2.as_slice()].concat();

        let (argument_1, offset_data) = extract_argument(data.as_slice()).unwrap();
        let (argument_2, _offset_data) = extract_argument(offset_data).unwrap();

        assert_eq!(argument_1, "argument_1");
        assert_eq!(argument_2, "argument_2");
    }
}
