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

//! Interface to connect to and interact with the manager, used if the server is started with a
//! manager and needs to interact with it to initialize the Delta Lake.

use tonic::metadata::MetadataMap;

use crate::error::{ModelarDbServerError, Result};

/// Manages metadata related to the manager and provides functionality for interacting with the manager.
#[derive(Clone, Debug, PartialEq)]
pub struct Manager {
    /// Key received from the manager when registering, used to validate future requests that are
    /// only allowed to come from the manager.
    key: String,
}

impl Manager {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    /// Validate the request by checking that the key in the request metadata matches the key of the
    /// manager. If the request is valid, return [`Ok`], otherwise return [`ModelarDbServerError`].
    pub fn validate_request(&self, request_metadata: &MetadataMap) -> Result<()> {
        let request_key =
            request_metadata
                .get("x-manager-key")
                .ok_or(ModelarDbServerError::InvalidState(
                    "Missing manager key.".to_owned(),
                ))?;

        if &self.key != request_key {
            Err(ModelarDbServerError::InvalidState(format!(
                "Manager key '{request_key:?}' is invalid.",
            )))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use uuid::Uuid;

    // Tests for validate_request().
    #[tokio::test]
    async fn test_validate_request() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();
        request_metadata.append("x-manager-key", manager.key.parse().unwrap());

        assert!(manager.validate_request(&request_metadata).is_ok());
    }

    #[tokio::test]
    async fn test_validate_request_without_key() {
        let manager = create_manager();
        let request_metadata = MetadataMap::new();

        let result = manager.validate_request(&request_metadata);

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid State Error: Missing manager key."
        );
    }

    #[tokio::test]
    async fn test_validate_request_with_invalid_key() {
        let manager = create_manager();
        let mut request_metadata = MetadataMap::new();

        let key = Uuid::new_v4().to_string();
        request_metadata.append("x-manager-key", key.parse().unwrap());

        let result = manager.validate_request(&request_metadata);

        assert_eq!(
            result.unwrap_err().to_string(),
            format!("Invalid State Error: Manager key '\"{key}\"' is invalid.")
        );
    }

    fn create_manager() -> Manager {
        Manager::new(Uuid::new_v4().to_string())
    }
}
