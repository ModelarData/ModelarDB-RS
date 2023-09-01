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
use uuid::Uuid;

/// Error to emit when an unknown remote data folder type is used.
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
                "s3",
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

            let credentials = [
                "azureblobstorage",
                account.as_str(),
                access_key.as_str(),
                container_name,
            ];

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

/// Validate that the remote data folder can be accessed. If the remote data folder cannot be
/// accessed, return the error that occurred as a [`String`].
pub async fn validate_remote_data_folder(
    remote_data_folder: &Arc<dyn ObjectStore>,
) -> Result<(), String> {
    let invalid_path = Uuid::new_v4().to_string();

    // Check that the connection is valid by attempting to retrieve a file that does not exist.
    match remote_data_folder.get(&Path::from(invalid_path)).await {
        Ok(_) => Ok(()),
        Err(error) => match error {
            object_store::Error::NotFound { .. } => {
                let error = error.to_string();

                // BlobNotFound and NoSuchKey errors are only returned if the object store
                // connection is valid but the path does not exist.
                if error.contains("BlobNotFound") || error.contains("NoSuchKey") {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            _ => Err(error.to_string()),
        },
    }
}

/// Parse the arguments in `data` and return the resulting remote object store. If `data` does not
/// contain valid connection information or the type of the new remote object store is not "s3" or
/// "azureblobstorage", [`Status`] is returned.
pub async fn parse_object_store_arguments(data: &[u8]) -> Result<Arc<dyn ObjectStore>, Status> {
    let (object_store_type, offset_data) = decode_argument(data)?;

    match object_store_type {
        "s3" => Ok(parse_s3_arguments(offset_data).await),
        "azureblobstorage" => Ok(parse_azure_blob_storage_arguments(offset_data).await),
        _ => Err(Status::unimplemented(format!(
            "{object_store_type} is currently not supported."
        ))),
    }?
}

/// Parse the arguments in `data` and return an [`Amazon S3`](object_store::aws::AmazonS3) object
/// store if `data` contains the necessary arguments. If `data` is missing arguments or if the
/// created [`Amazon S3`](object_store::aws::AmazonS3) object store connection is invalid,
/// [`Status`] is returned.
pub async fn parse_s3_arguments(data: &[u8]) -> Result<Arc<dyn ObjectStore>, Status> {
    let (endpoint, offset_data) = decode_argument(data)?;
    let (bucket_name, offset_data) = decode_argument(offset_data)?;
    let (access_key_id, offset_data) = decode_argument(offset_data)?;
    let (secret_access_key, _offset_data) = decode_argument(offset_data)?;

    let s3: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_region("")
            .with_allow_http(true)
            .with_endpoint(endpoint)
            .with_bucket_name(bucket_name)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .build()
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
    );

    validate_remote_data_folder(&s3)
        .await
        .map_err(Status::invalid_argument)?;

    Ok(s3)
}

/// Parse the arguments in `data` and return an [`Azure Blob Storage`](object_store::azure::MicrosoftAzure)
/// object store if `data` contains the necessary arguments. If `data` is missing arguments or if the created
/// [`Azure Blob Storage`](object_store::azure::MicrosoftAzure) object store connection is invalid,
/// [`Status`] is returned.
pub async fn parse_azure_blob_storage_arguments(
    data: &[u8],
) -> Result<Arc<dyn ObjectStore>, Status> {
    let (account, offset_data) = decode_argument(data)?;
    let (access_key, offset_data) = decode_argument(offset_data)?;
    let (container_name, _offset_data) = decode_argument(offset_data)?;

    let azure_blob_storage: Arc<dyn ObjectStore> = Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(account)
            .with_access_key(access_key)
            .with_container_name(container_name)
            .build()
            .map_err(|error| Status::invalid_argument(error.to_string()))?,
    );

    validate_remote_data_folder(&azure_blob_storage)
        .await
        .map_err(Status::invalid_argument)?;

    Ok(azure_blob_storage)
}

/// Convert the given `argument` into bytes that contain the length of the byte representation of
/// `argument` together with the byte representation. The length is exactly two bytes long.
pub fn encode_argument(argument: &str) -> Vec<u8> {
    let argument_bytes: Vec<u8> = argument.as_bytes().into();
    let mut argument_size_bytes = vec![0; 2];

    // unwrap() is safe since the buffer is in memory and no I/O errors can occur.
    argument_size_bytes
        .write_all(&argument_bytes.len().to_be_bytes())
        .unwrap();

    [
        &argument_size_bytes[(argument_size_bytes.len() - 2)..],
        argument_bytes.as_slice(),
    ]
    .concat()
}

/// Return a tuple containing the first argument and `data` with the extracted argument's bytes
/// removed. It is assumed that `data` is a slice containing one or more arguments with the
/// following format: size of argument (2 bytes) followed by the argument (size bytes).
pub fn decode_argument(data: &[u8]) -> Result<(&str, &[u8]), Status> {
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
    use proptest::proptest;

    #[test]
    fn test_s3_argument_to_connection_info() {
        env::set_var("AWS_ENDPOINT", "test_endpoint");
        env::set_var("AWS_ACCESS_KEY_ID", "test_access_key_id");
        env::set_var("AWS_SECRET_ACCESS_KEY", "test_secret_access_key");

        let connection_info = argument_to_connection_info("s3://test_bucket_name").unwrap();

        let (object_store_type, offset_data) = decode_argument(&connection_info).unwrap();
        assert_eq!(object_store_type, "s3");

        let (endpoint, offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(endpoint, "test_endpoint");

        let (bucket_name, offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(bucket_name, "test_bucket_name");

        let (access_key_id, offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(access_key_id, "test_access_key_id");

        let (secret_access_key, _offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(secret_access_key, "test_secret_access_key")
    }

    #[test]
    fn test_azureblobstorage_argument_to_connection_info() {
        env::set_var("AZURE_STORAGE_ACCOUNT_NAME", "test_storage_account_name");
        env::set_var("AZURE_STORAGE_ACCESS_KEY", "test_storage_access_key");

        let connection_info =
            argument_to_connection_info("azureblobstorage://test_container_name").unwrap();

        let (object_store_type, offset_data) = decode_argument(&connection_info).unwrap();
        assert_eq!(object_store_type, "azureblobstorage");

        let (account, offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(account, "test_storage_account_name");

        let (access_key, offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(access_key, "test_storage_access_key");

        let (container_name, _offset_data) = decode_argument(offset_data).unwrap();
        assert_eq!(container_name, "test_container_name");
    }

    #[test]
    fn test_invalid_argument_to_connection_info() {
        assert!(argument_to_connection_info("googlecloudstorage://test").is_err());
    }

    proptest! {
        #[test]
        fn test_encode_decode_argument(
            argument_1 in "[A-Za-zÀ-ȕ0-9(),-_., ]",
            argument_2 in "[A-Za-zÀ-ȕ0-9(),-_., ]"
        ) {
            let encoded_argument_1 = encode_argument(&argument_1);
            let encoded_argument_2 = encode_argument(&argument_2);

            let data = [encoded_argument_1.as_slice(), encoded_argument_2.as_slice()].concat();

            let (decoded_argument_1, offset_data) = decode_argument(data.as_slice()).unwrap();
            let (decoded_argument_2, _offset_data) = decode_argument(offset_data).unwrap();

            assert_eq!(decoded_argument_1, argument_1);
            assert_eq!(decoded_argument_2, argument_2);
        }
    }
}
