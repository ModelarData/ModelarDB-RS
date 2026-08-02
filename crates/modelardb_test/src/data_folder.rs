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

//! Functions for creating data folders used throughout ModelarDB for testing purposes.

use std::path::Path as StdPath;

use modelardb_storage::data_folder::DataFolder;

use crate::BUCKET_AND_CONTAINER_NAME;

/// Return a [`DataFolder`] storing data in memory for testing.
pub async fn in_memory_data_folder() -> DataFolder {
    DataFolder::open_memory().await.unwrap()
}

/// Return a [`DataFolder`] storing data on local disk for testing.
pub async fn local_file_system_data_folder(data_folder_path: &StdPath) -> DataFolder {
    DataFolder::open_local(data_folder_path).await.unwrap()
}

/// Return a [`DataFolder`] storing data in an AWS3 compatible object store for testing using Minio.
pub async fn aws3_data_folder() -> DataFolder {
    DataFolder::open_s3(
        "http://localhost:9000".to_owned(),
        BUCKET_AND_CONTAINER_NAME.to_owned(),
        "minioadmin".to_owned(),
        "minioadmin".to_owned(),
    )
    .await
    .unwrap()
}

/// Return a [`DataFolder`] storing data in Microsoft Azure for testing using Azurite.
pub async fn azure_data_folder() -> DataFolder {
    DataFolder::open_azure(
        "devstoreaccount1".to_owned(),
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
            .to_owned(),
        BUCKET_AND_CONTAINER_NAME.to_owned(),
        true,
    )
    .await
    .unwrap()
}
