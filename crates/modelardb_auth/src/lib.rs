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

//! Types to support authentication and authorization in ModelarDB.

use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

pub mod authenticator;

/// The permission required to perform an operation in a ModelarDB server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl Permission {
    /// Return [`true`] if `granted` satisfies this required permission using the hierarchy
    /// Admin ⊇ Write ⊇ Read, otherwise [`false`] is returned.
    pub fn is_satisfied_by(&self, granted: &Permission) -> bool {
        matches!(
            (self, granted),
            (Permission::Read, _)
                | (Permission::Write, Permission::Write | Permission::Admin)
                | (Permission::Admin, Permission::Admin)
        )
    }
}

/// Tonic interceptor that attaches a bearer token to every outgoing request when one is present.
#[derive(Clone)]
pub struct BearerInterceptor {
    /// The value of the `authorization` header to attach. This is either `Bearer <token>` or
    /// [`None`] if no token has been provided.
    pub maybe_authorization: Option<AsciiMetadataValue>,
}

impl BearerInterceptor {
    /// Create a new [`BearerInterceptor`] that attaches the bearer token in `maybe_token` to every
    /// outgoing request. If `maybe_token` is [`None`], no token is attached. If `maybe_token` is
    /// [`Some`] but the token is an invalid ASCII string, [`Status`] is returned.
    pub fn try_new(maybe_token: Option<&str>) -> Result<Self, Status> {
        let maybe_authorization = maybe_token
            .map(|token| {
                format!("Bearer {token}")
                    .parse::<AsciiMetadataValue>()
                    .map_err(|error| Status::invalid_argument(format!("Invalid token: {error}.")))
            })
            .transpose()?;

        Ok(Self {
            maybe_authorization,
        })
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(authorization) = &self.maybe_authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_satisfied_by_read() {
        assert!(Permission::Read.is_satisfied_by(&Permission::Read));
    }

    #[test]
    fn test_read_satisfied_by_write() {
        assert!(Permission::Read.is_satisfied_by(&Permission::Write));
    }

    #[test]
    fn test_read_satisfied_by_admin() {
        assert!(Permission::Read.is_satisfied_by(&Permission::Admin));
    }

    #[test]
    fn test_write_not_satisfied_by_read() {
        assert!(!Permission::Write.is_satisfied_by(&Permission::Read));
    }

    #[test]
    fn test_write_satisfied_by_write() {
        assert!(Permission::Write.is_satisfied_by(&Permission::Write));
    }

    #[test]
    fn test_write_satisfied_by_admin() {
        assert!(Permission::Write.is_satisfied_by(&Permission::Admin));
    }

    #[test]
    fn test_admin_not_satisfied_by_read() {
        assert!(!Permission::Admin.is_satisfied_by(&Permission::Read));
    }

    #[test]
    fn test_admin_not_satisfied_by_write() {
        assert!(!Permission::Admin.is_satisfied_by(&Permission::Write));
    }

    #[test]
    fn test_admin_satisfied_by_admin() {
        assert!(Permission::Admin.is_satisfied_by(&Permission::Admin));
    }
}
