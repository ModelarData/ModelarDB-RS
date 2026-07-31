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

use itertools::Itertools;
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

    // Build the async tokio::test functions that will call the function annotated with this macro
    // using all the combinations of supported object stores. A separate function is created for
    // each permutation of ObjectStores instead of a single function with nested loops to make it
    // simpler to see which combination of object stores fail a test.
    let object_store_idents = &[
        format_ident!("in_memory_object_store"),
        format_ident!("local_file_system_object_store"),
        format_ident!("aws3_object_store"),
        format_ident!("azure_object_store"),
    ];

    let object_store_ident_permutations_with_replacements = itertools::repeat_n(
        object_store_idents.into_iter(),
        object_store_parameter_count as usize,
    )
    .multi_cartesian_product();

    let mut tokio_test_functions = quote! {};
    let function_name_string = function_name.to_string();
    for object_store_idents in object_store_ident_permutations_with_replacements {
        // The name of all tokio test functions use function_test as a prefix so cargo test will
        // execute them all if is called with functions_name as its argument as it runs tests
        // containing its argument in their names. The name of all the object stores used are append
        // to make it easy to see which object stores caused the test to fail.
        //
        // Each part is separated by two underscores to make the name more readable and to decrease
        // the chance that the name will conflict with the name of a user-defined test since they
        // should not use two underscores.
        //
        // The name is crated manually because quote!()'s * syntax adds spaces rustc cannot handle and
        // format_ident!() cannot be used as the number of ObjectStore parameters is not static.
        let mut tokio_test_function_name = String::new();
        tokio_test_function_name.push_str(&function_name_string);
        for object_store_ident in &object_store_idents {
            tokio_test_function_name.push_str("__");
            tokio_test_function_name.push_str(&object_store_ident.to_string());
        }
        let tokio_test_function_name_ident = format_ident!("{}", tokio_test_function_name);

        tokio_test_functions = quote! {
            #tokio_test_functions

            #[tokio::test]
            async fn #tokio_test_function_name_ident() {
                #function_name(#(&modelardb_test::object_store::#object_store_idents()),*).await
            }
        };
    }

    input.extend(tokio_test_functions);
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
