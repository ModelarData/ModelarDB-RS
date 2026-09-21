/* Copyright 2026 The ModelarDB Contributors
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

//! Functions for creating object stores used throughout ModelarDB for testing purposes. All return
//! `Box<dyn ObjectStore>` to match the output of [`object_store::parse_url_opts()`].

use std::collections::HashMap;
use std::path::Path as StdPath;

use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use url::Url;

use crate::BUCKET_AND_CONTAINER_NAME;

/// Return an [`InMemory`] [`ObjectStore`] for testing.
pub fn in_memory_object_store() -> Box<dyn ObjectStore> {
    Box::new(InMemory::new())
}

/// Return a [`LocalFileSystem`] [`ObjectStore`] for testing.
pub fn local_file_system_object_store(object_store_path: &StdPath) -> Box<dyn ObjectStore> {
    let local_file_system = LocalFileSystem::new_with_prefix(object_store_path).unwrap();
    Box::new(local_file_system)
}

/// Return an [`AmazonS3`](object_store::aws::AmazonS3) [`ObjectStore`] for testing.
pub fn s3_object_store() -> Box<dyn ObjectStore> {
    let location = format!("s3://{BUCKET_AND_CONTAINER_NAME}");
    let url = Url::parse(&location).unwrap();

    let storage_options = HashMap::from([
        ("aws_access_key_id".to_owned(), "minioadmin".to_owned()),
        ("aws_secret_access_key".to_owned(), "minioadmin".to_owned()),
        (
            "aws_endpoint_url".to_owned(),
            "http://localhost:9000".to_owned(),
        ),
        (
            "aws_bucket_name".to_owned(),
            BUCKET_AND_CONTAINER_NAME.to_owned(),
        ),
        ("aws_allow_http".to_owned(), "true".to_owned()),
    ]);

    let (boxed_amazon_s3, _path) = object_store::parse_url_opts(&url, &storage_options).unwrap();
    boxed_amazon_s3
}

/// Return a [`MicrosoftAzure`](object_store::azure::MicrosoftAzure) [`ObjectStore`] for testing.
pub fn azure_object_store() -> Box<dyn ObjectStore> {
    let location = format!("az://{BUCKET_AND_CONTAINER_NAME}");
    let url = Url::parse(&location).unwrap();

    let storage_options = HashMap::from([
        ("azure_storage_account_name".to_owned(), "devstoreaccount1".to_owned()),
        ("azure_storage_account_key".to_owned(), "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned()),
        ("azure_container_name".to_owned(), BUCKET_AND_CONTAINER_NAME.to_owned()),
        ("azure_storage_use_emulator".to_owned(), "true".to_owned()),
    ]);

    let (boxed_microsoft_azure, _path) =
        object_store::parse_url_opts(&url, &storage_options).unwrap();
    boxed_microsoft_azure
}
