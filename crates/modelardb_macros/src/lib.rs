/* Copyright 2025 The ModelarDB Contributors
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

//! The procedural macros used throughout ModelarDB.

use proc_macro2::TokenStream;
use proc_macro2::TokenTree::Ident;
use proc_macro2::Ident as IdentStruct;
use quote::quote;

/// Bucket and container name used by Minio and Azurite
const BUCKET_AND_CONTAINER_NAME: &str = "modelardb";

#[proc_macro_attribute]
pub fn object_store_test(args: proc_macro::TokenStream, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Convert args and input to proc_macro2 so they can be used with quote!.
    let _args: TokenStream = args.into();
    let mut input: TokenStream = input.into();

    // TODO: Updated the code to replace all object_stores with any permutation of object_stores.
    let mut input_iter = input.clone().into_iter();
    input_iter.next();
    input_iter.next();

    let function_name = if let Some(Ident(ident)) = input_iter.next() {
        ident
    } else {
        // TODO: Compile error?
        panic!("Must be applied to an async function with one parameter of type &dyn ObjectStore");
    };
    let function_name_in_memory = add_suffix_to_ident(&function_name, "_memory");
    let function_name_local_file_system = add_suffix_to_ident(&function_name, "_local_file_system");
    let function_name_aws3 = add_suffix_to_ident(&function_name, "_aws3");
    let function_name_azure = add_suffix_to_ident(&function_name, "_azure");

    let tokens = quote! {
        #[tokio::test]
        async fn #function_name_in_memory() {
            let object_store = InMemory::new();
            #function_name(&object_store).await;
        }

        #[tokio::test]
        async fn #function_name_local_file_system() {
            let temp_dir = tempfile::tempdir().unwrap();
            let object_store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
            #function_name(&object_store).await;
        }

        #[tokio::test]
        async fn #function_name_aws3() {
            let storage_options = HashMap::from([
                ("aws_access_key_id".to_owned(), "minioadmin".to_owned()),
                ("aws_secret_access_key".to_owned(), "minioadmin".to_owned()),
                ("aws_endpoint_url".to_owned(), "http://localhost:9000".to_owned()),
                ("aws_bucket_name".to_owned(), #BUCKET_AND_CONTAINER_NAME.to_owned()),
                ("aws_s3_allow_unsafe_rename".to_owned(), "true".to_owned()),
            ]);

            // Build the Amazon S3 object store with the given storage options manually to allow http.
            let location = format!("s3://{}", #BUCKET_AND_CONTAINER_NAME);
            let url = Url::parse(&location).unwrap();

            let object_store = storage_options
                .iter()
                .fold(
                    AmazonS3Builder::new()
                        .with_url(url.to_string())
                        .with_allow_http(true),
                    |builder, (key, value)| match key.parse() {
                        Ok(k) => builder.with_config(k, value),
                        Err(_) => builder,
                    },
                )
                .build().unwrap();

            #function_name(&object_store).await;
        }

        #[tokio::test]
        async fn #function_name_azure() {
            let location = format!("az://{}", #BUCKET_AND_CONTAINER_NAME);
            let url = Url::parse(&location).unwrap();

            let storage_options = HashMap::from([
                ("azure_storage_account_name".to_owned(), "devstoreaccount1".to_owned()),
                ("azure_storage_account_key".to_owned(), "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned()),
                ("azure_container_name".to_owned(), #BUCKET_AND_CONTAINER_NAME.to_owned()),
            ]);
            let (object_store, _path) = object_store::parse_url_opts(&url, &storage_options).unwrap();

            #function_name(&object_store).await;
        }
    };

    input.extend(tokens);
    input.into()
}

/// Adds `suffix` to `ident` without updating the [`Span`] associated with `ident`. The [`Span`] is
/// not updated as there seems to be no method for constructing a new [`Span`].
fn add_suffix_to_ident(ident: &IdentStruct, suffix: &str) -> IdentStruct {
    // The existing span is reused as there seems to no method for constructing one.
    let span = ident.span();

    // Span.source_text(); is not used as its comment say it is for diagnostics only.
    let ident_strint = ident.to_string();

    IdentStruct::new(&format!("{ident_strint}{suffix}"), span)
}
