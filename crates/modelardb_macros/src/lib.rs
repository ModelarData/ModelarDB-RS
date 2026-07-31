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

mod error;

use proc_macro2::Group as GroupStruct;
use proc_macro2::Ident as IdentStruct;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;
use proc_macro2::TokenTree::Group;
use proc_macro2::TokenTree::Ident;
use proc_macro2::TokenTree::Punct;
use quote::format_ident;
use quote::quote;

use crate::error::ModelarDbMacrosError;
use crate::error::Result;

/// Bucket and container name used by Minio and Azurite
const BUCKET_AND_CONTAINER_NAME: &str = "modelardb";

#[proc_macro_attribute]
pub fn object_store_test(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Convert args and input to proc_macro2 so they can be used with quote!.
    let _args: TokenStream = args.into();
    let mut input: TokenStream = input.into();

    let (function_name, group) =
        next_ident_and_group(input.clone()).expect("Assumes the input would contain a Group.");
    let object_store_parameter_count = object_store_count(group.clone())
        .expect("Assumes the input would contain &dyn ObjectStore parameters.");
    let function_name_object_store_test = format_ident!("{}_object_store_test", function_name);

    // Build the async tokio::test function that will call the function annotated with this macro
    // using all the combinations of supported object stores. The function is build inside out to
    // make correct nesting using curly braces simpler.
    let argument_names: Vec<_> = (0..object_store_parameter_count)
        .map(|ospc| format_ident!("os{}", ospc))
        .collect();

    let mut tokio_test_function = quote! {
        #function_name(#(#argument_names),*);
    };

    for argument_name in argument_names.iter().rev() {
        tokio_test_function = quote! {
            for #argument_name in object_stores {
                #tokio_test_function
            }
        };
    }

    tokio_test_function = quote! {
        #[tokio::test]
        async fn #function_name_object_store_test() {
            let object_stores = &[in_memory_object_store(), local_file_system_object_store(), aws3_object_store(), azure_object_store()];
            #tokio_test_function
        }
    };

    let tokens = quote! {
        fn in_memory_object_store() -> Box<dyn object_store::ObjectStore> {
            Box::new(InMemory::new())
        }

        fn local_file_system_object_store() -> Box<dyn object_store::ObjectStore> {
            let temp_dir = tempfile::tempdir().unwrap();
            let local_file_system = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
            Box::new(local_file_system)
        }

        fn aws3_object_store() -> Box<dyn object_store::ObjectStore> {
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

            let amazon_s3 = storage_options
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
            Box::new(amazon_s3)
        }

        fn azure_object_store() -> Box<dyn object_store::ObjectStore> {
            let location = format!("az://{}", #BUCKET_AND_CONTAINER_NAME);
            let url = Url::parse(&location).unwrap();

            let storage_options = HashMap::from([
                ("azure_storage_account_name".to_owned(), "devstoreaccount1".to_owned()),
                ("azure_storage_account_key".to_owned(), "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned()),
                ("azure_container_name".to_owned(), #BUCKET_AND_CONTAINER_NAME.to_owned()),
                ("azure_storage_use_emulator".to_owned(), "true".to_owned()),
            ]);
            let (boxed_microsoft_azure, _path) = object_store::parse_url_opts(&url, &storage_options).unwrap();
            boxed_microsoft_azure
        }

        #tokio_test_function
    };

    println!("{}", tokens);

    input.extend(tokens);
    input.into()
}

/// Return the next pair of adjacent [`IdentStruct`] and [`GroupStruct`] tokens from `input` or
/// `None` if no adjacent [`IdentStruct`] and [`GroupStruct`] token exist in the rest of `input`.
fn next_ident_and_group(input: TokenStream) -> Option<(IdentStruct, GroupStruct)> {
    let mut adjacent_ident = None;
    for token in input {
        if let Group(group) = &token
            && let Some(ident) = adjacent_ident
        {
            // Clone is needed to avoid a partial move error with the next if let.
            return Some((ident, group.clone()));
        } else if let Ident(ident) = token {
            adjacent_ident = Some(ident);
        } else {
            adjacent_ident = None;
        }
    }
    None
}

/// Returns the number of `name: &dyn ObjectStore` arguments in the group. `The return type is `u16`
/// as `rustc` returns an error if a function or method have more than 65,535 parameters at the time
/// of writing. An [`ModelarDbMacrosError`] is returned if `group` contain anything but multiple
/// instances of ``name: &dyn ObjectStore`.
fn object_store_count(group: GroupStruct) -> Result<u16> {
    let mut token_peekable_iterator = group.stream().into_iter().peekable();

    let mut object_store_count = 0;
    loop {
        expect_parameter_object_store(&mut token_peekable_iterator)?;
        object_store_count += 1;

        // Skip the comma between parameters
        token_peekable_iterator.next();

        // End loop when iterator is empty, peek is used to not consume a token.
        if let None = token_peekable_iterator.peek() {
            break;
        }
    }

    Ok(object_store_count)
}

/// Returns [`Ok`] if the next five tokens `token_iterator` returns is `name: &dyn ObjectStore`,
/// otherwise a [`ModelarDbMacrosError`] is returned.
fn expect_parameter_object_store(
    token_iterator: &mut impl Iterator<Item = TokenTree>,
) -> Result<()> {
    // The contents of the first Ident token cannot checked as it is the parameter name.
    expect_ident_without_contents(token_iterator)?;
    expect_punct_with_contents(token_iterator, ':')?;
    expect_punct_with_contents(token_iterator, '&')?;
    expect_ident_with_contents(token_iterator, "dyn")?;
    expect_ident_with_contents(token_iterator, "ObjectStore")?;
    Ok(())
}

/// Return [`Ok`] if the next [`TokenTree`] from `token_iterator` is an [`Ident`], otherwise a [`ModelarDbMacrosError] is returned.
fn expect_ident_without_contents(
    token_iterator: &mut impl Iterator<Item = TokenTree>,
) -> Result<()> {
    let error_message = match token_iterator.next() {
        Some(Ident(_token)) => return Ok(()),
        Some(token) => format!("Expected Ident, found {}.", token),
        None => format!("Expected Ident, found an empty iterator."),
    };
    Err(ModelarDbMacrosError::Parse(error_message))
}

/// Return [`Ok`] if the next [`TokenTree`] from `token_iterator` is an [`Ident`] that contains `content`, otherwise a [`ModelarDbMacrosError] is returned.
fn expect_ident_with_contents(
    token_iterator: &mut impl Iterator<Item = TokenTree>,
    contents: &str,
) -> Result<()> {
    let error_message = match token_iterator.next() {
        Some(Ident(token)) if token.to_string() == contents => return Ok(()),
        Some(token) => format!("Expected Ident with {}, found {}.", contents, token),
        None => format!("Expected Ident with {}, found an empty iterator.", contents),
    };
    Err(ModelarDbMacrosError::Parse(error_message))
}

/// Return [`Ok`] if the next [`TokenTree`] from `token_iterator` is an [`Punct`] that contains `content`, otherwise a [`ModelarDbMacrosError] is returned.
fn expect_punct_with_contents(
    token_iterator: &mut impl Iterator<Item = TokenTree>,
    contents: char,
) -> Result<()> {
    let error_message = match token_iterator.next() {
        Some(Punct(token)) if token.as_char() == contents => return Ok(()),
        Some(token) => format!("Expected Punct with {}, found {}.", contents, token),
        None => format!("Expected Punct with {}, found an empty iterator.", contents),
    };
    Err(ModelarDbMacrosError::Parse(error_message))
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
