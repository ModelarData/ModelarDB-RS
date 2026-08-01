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
use proc_macro::Group as GroupStruct;
use proc_macro::Ident as IdentStruct;
use proc_macro::TokenStream;
use proc_macro::TokenTree;
use proc_macro::TokenTree::Group;
use proc_macro::TokenTree::Ident;
use proc_macro::TokenTree::Punct;

use crate::error::ModelarDbMacrosError;
use crate::error::Result;

#[proc_macro_attribute]
pub fn object_store_test(
    _args: proc_macro::TokenStream,
    mut input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Extract and check the required parts of the annotated function. The input stream must be
    // cloned as it must be read as part of this macro but also extended with the generated code.
    let (function_name_ident, function_parameters_group) =
        next_ident_and_group(input.clone()).expect("Assumes input is a function with parameters.");
    let object_store_parameter_count = object_store_count(function_parameters_group)
        .expect("Assumes the function only has &dyn ObjectStore parameters.");

    // Build the async tokio::test functions that will call the annotated function using all the
    // combinations of supported object stores. A separate function is created for each permutation
    // of object stores instead of a single function with nested loops to make it simpler to see
    // which combination of object stores fail a test. The name for the generated function start
    // with the name of the annotated function so they can all by run with cargo test annotated
    // function name and uses a combination of one and two underscores to make it more readable and
    // to avoid conflicts with user code as normal function names should not use two underscores.
    let function_name = function_name_ident.to_string();

    let object_store_names = &[
        "in_memory_object_store",
        "local_file_system_object_store",
        "aws3_object_store",
        "azure_object_store",
    ];

    let object_store_names_permutations_with_replacements = itertools::repeat_n(
        object_store_names.into_iter(),
        object_store_parameter_count as usize,
    )
    .multi_cartesian_product();

    let mut implementation = String::new();
    for object_store_names in object_store_names_permutations_with_replacements {
        let name = object_store_names.clone().into_iter().join("__");
        let arguments = object_store_names
            .into_iter()
            .map(|osn| format!("&modelardb_test::object_store::{}()", osn))
            .join(", ");

        implementation.push_str(&format!(
            "
            #[tokio::test]
            async fn {function_name}__{name}() {{
                {function_name}({arguments}).await
            }}
        "
        ));
    }

    // Append the generated functions to the existing token stream.
    let implementation_tokens = implementation
        .parse::<TokenStream>()
        .expect("object_store_test generated invalid tokens.");
    input.extend(implementation_tokens);

    input
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
fn object_store_count(function_arguments_group: GroupStruct) -> Result<u16> {
    let mut token_peekable_iterator = function_arguments_group.stream().into_iter().peekable();

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
