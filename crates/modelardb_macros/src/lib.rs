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

//! The procedural macros used throughout ModelarDB. The procedural macros purposely do not use
//! crates designed to simplify writing procedural macros like `syn`, `proc_macro2`, and `quote`, as
//! they made the code more complex when evaluated. The macros have no automatic tests as using
//! [`proc_macro`] outside procedural macros makes the compiler panic.

mod error;

use std::fmt::{self, Display, Formatter};

use itertools::Itertools;
use proc_macro::TokenTree::{Group, Ident, Punct};
use proc_macro::{Group as GroupStruct, Ident as IdentStruct, TokenStream, TokenTree};

use crate::error::ModelarDbMacrosError;
use crate::error::Result;

/// Specify the type of parameters a macro supports.
#[derive(Clone, Copy)]
enum ParameterType {
    /// The annotated function must only contain `&DataFolder` parameters.
    DataFolder,
    /// The annotated function must only contain `&dyn ObjectStore` parameters.
    ObjectStore,
}

impl Display for ParameterType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ParameterType::DataFolder => write!(f, "&DataFolder"),
            ParameterType::ObjectStore => write!(f, "&dyn ObjectStore"),
        }
    }
}

/// A function that will create an argument to be passed to a function as a borrow.
struct BorrowedArgument {
    /// Name of the function.
    name: String,
    /// Module containing the function.
    module: String,
    /// Specify if the function must be called with a path.
    need_path: bool,
    /// Specify if the function must be called with .await.
    need_await: bool,
}

impl BorrowedArgument {
    fn new(name: &str, module: &str, need_path: bool, need_await: bool) -> Self {
        BorrowedArgument {
            name: name.to_owned(),
            module: module.to_owned(),
            need_path,
            need_await,
        }
    }
}

/// Macro for generating test functions that use all permutations with replacements of `DataFolder`
/// The macro must be placed on an `async` function without `#[test]` or `#[tokio::test]` that only
/// has `&DataFolder` parameters. It will generate one `#[tokio::test]` function for each
/// permutation with replacement of `DataFolder` configurations that call the annotated function.
///
/// ```ignore
/// // This doc test is not tested as procedural macros cannot be used in their own crates.
/// use modelardb_macros::data_folder_test;
///
/// #[data_folder_test]
/// async fn test_data_folder_drop_table(data_folder: &DataFolder) {
///     data_folder.drop_table("table_name").await.unwrap();
/// }
/// ```.
#[proc_macro_attribute]
pub fn data_folder_test(
    _args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Extract and check the required parts of the annotated function. The input stream must be
    // cloned as it must be read as part of this macro but also extended with the generated code.
    let (function_name, data_folder_parameter_count) =
        function_name_and_checked_parameter_count(input.clone(), ParameterType::DataFolder);

    // Generate the async tokio::test functions that will call the annotated function using all
    // combinations of data folder configurations. A separate function is created for each
    // permutation of data folder configurations instead of a single function with nested loops to
    // make it simpler to see which combination of data folder configurations fail a test. The name
    // for the generated function start with the name of the annotated function followed by the name
    // of each data folder configuration used with each part of the name separated by two
    // underscores. By using the annotated function name as a prefix, all the generated tests can be
    // run with `cargo test annotated_function_name`. The names of the data folder configurations
    // used are included to make it simple to identify which data folder configurations causes a
    // test to fail. Finally, each part of the name is separated by two underscores to make it more
    // readable and to avoid conflicts with user code as function names should not use two
    // underscores.
    let data_folders = &[
        BorrowedArgument::new(
            "in_memory_data_folder",
            "modelardb_test::data_folder",
            false,
            true,
        ),
        BorrowedArgument::new(
            "local_file_system_data_folder",
            "modelardb_test::data_folder",
            true,
            true,
        ),
        BorrowedArgument::new("s3_data_folder", "modelardb_test::data_folder", false, true),
        BorrowedArgument::new(
            "azure_data_folder",
            "modelardb_test::data_folder",
            false,
            true,
        ),
    ];

    // Create an iterator that produce all permutations with replacements of the items in
    // data_folders. First the code creates an iterator that repeats the data_folders iterator
    // data_folder_parameter_count times. Then these iterators are crossed together to produce each
    // permutation with replacements. This should be the same as data_folder_parameter_count nested
    // loops iterating over data_folders without the need to know the number of loops required
    // beforehand since the value of data_folder_parameter_count is not known at development time.
    let data_folder_permutations_with_replacements =
        itertools::repeat_n(data_folders.iter(), data_folder_parameter_count as usize)
            .multi_cartesian_product();

    let mut code = String::new();
    for data_folder_permutation in data_folder_permutations_with_replacements {
        code.push_str(&generate_test_function(
            &function_name,
            &data_folder_permutation,
        ));
    }

    // Append the generated functions to the existing token stream.
    append_code_to_token_stream(input, code)
}

/// Macro for generating test functions that use all permutations with replacements of object
/// stores. The macro must be placed on an `async` function without `#[test]` or `#[tokio::test]`
/// that only has `&dyn ObjectStore` parameters. It will generate one `#[tokio::test]` function for
/// each permutation with replacement of supported object stores that call the annotated function.
///
/// ```ignore
/// // This doc test is not tested as procedural macros cannot be used in their own crates.
/// use modelardb_macros::object_store_test;
///
/// use futures::StreamExt;
/// use object_store::ObjectStore;
///
/// #[object_store_test]
/// async fn test_object_store_list(object_store: &dyn ObjectStore) {
///     let mut files = object_store.list(None);
///     while let Some(f) = files.next().await {
///        f.unwrap();
///     }
/// }
/// ```.
#[proc_macro_attribute]
pub fn object_store_test(
    _args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // See the comments in data_folder_test as it follows the same structure as object_store_test.
    let (function_name, object_store_parameter_count) =
        function_name_and_checked_parameter_count(input.clone(), ParameterType::ObjectStore);

    let object_stores = &[
        BorrowedArgument::new(
            "in_memory_object_store",
            "modelardb_test::object_store",
            false,
            false,
        ),
        BorrowedArgument::new(
            "local_file_system_object_store",
            "modelardb_test::object_store",
            true,
            false,
        ),
        BorrowedArgument::new(
            "s3_object_store",
            "modelardb_test::object_store",
            false,
            false,
        ),
        BorrowedArgument::new(
            "azure_object_store",
            "modelardb_test::object_store",
            false,
            false,
        ),
    ];

    let object_store_permutations_with_replacements =
        itertools::repeat_n(object_stores.iter(), object_store_parameter_count as usize)
            .multi_cartesian_product();

    let mut code = String::new();
    for object_store_permutation in object_store_permutations_with_replacements {
        code.push_str(&generate_test_function(
            &function_name,
            &object_store_permutation,
        ));
    }

    append_code_to_token_stream(input, code)
}

/// Extracts the function name from `input`, checks that all its parameters are of type
/// `ParameterType`, and returns the number of parameters. The number of parameters is returned as a
/// `u16` as `rustc` returns an error if a function or method has more than 65,535 parameters at the
/// time of writing. A [`ModelarDbMacrosError`] is returned if `input` is not a function that only
/// accepts parameters of `parameter_type`.
fn function_name_and_checked_parameter_count(
    input: TokenStream,
    parameter_type: ParameterType,
) -> (String, u16) {
    let (function_name_ident, function_parameter_group) =
        next_ident_and_group(input.clone()).expect("Assumes input is a function with parameters.");
    let function_name = function_name_ident.to_string();
    let parameter_count = expect_parameter_type_and_count(function_parameter_group, parameter_type)
        .expect(&format!(
            "Assumes all of the function's parameters are of type {parameter_type}."
        ));
    (function_name, parameter_count)
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

/// Returns the number of `parameter_type` parameters in `function_parameter_group`. The return type
/// is `u16` as `rustc` returns an error if a function or method have more than 65,535 parameters at
/// the time of writing. A [`ModelarDbMacrosError`] is returned if `function_parameter_group`
/// contain anything but multiple instances of `parameter_type`.
fn expect_parameter_type_and_count(
    function_parameter_group: GroupStruct,
    parameter_type: ParameterType,
) -> Result<u16> {
    let mut token_peekable_iterator = function_parameter_group.stream().into_iter().peekable();

    let mut parameter_count = 0;
    loop {
        // Return an error if the next parameter does not match parameter_type.
        expect_parameter_type(&mut token_peekable_iterator, parameter_type)?;
        parameter_count += 1;

        // Skip the comma between parameters.
        token_peekable_iterator.next();

        // End loop when iterator is empty, peek is used in case the stream is not empty.
        if token_peekable_iterator.peek().is_none() {
            break;
        }
    }

    Ok(parameter_count)
}

/// Returns [`Ok`] if the next four tokens `token_iterator` returns is as expected according to
/// `parameter_type`, otherwise a [`ModelarDbMacrosError`] is returned.
fn expect_parameter_type(
    token_iterator: &mut impl Iterator<Item = TokenTree>,
    parameter_type: ParameterType,
) -> Result<()> {
    // The contents of the first Ident token cannot be checked as it is the parameter name.
    expect_ident_without_contents(token_iterator)?;
    expect_punct_with_contents(token_iterator, ':')?;
    expect_punct_with_contents(token_iterator, '&')?;
    match parameter_type {
        ParameterType::DataFolder => {
            expect_ident_with_contents(token_iterator, "DataFolder")?;
        }
        ParameterType::ObjectStore => {
            expect_ident_with_contents(token_iterator, "dyn")?;
            expect_ident_with_contents(token_iterator, "ObjectStore")?;
        }
    }
    Ok(())
}

/// Return [`Ok`] if the next [`TokenTree`] from `token_iterator` is a [`Ident`], otherwise a
/// [`ModelarDbMacrosError`] is returned.
fn expect_ident_without_contents(
    token_iterator: &mut impl Iterator<Item = TokenTree>,
) -> Result<()> {
    let error_message = match token_iterator.next() {
        Some(Ident(_token)) => return Ok(()),
        Some(token) => format!("Expected Ident, found {}.", token),
        None => "Expected Ident, found an empty iterator.".to_owned(),
    };
    Err(ModelarDbMacrosError::Parse(error_message))
}

/// Return [`Ok`] if the next [`TokenTree`] from `token_iterator` is an [`Ident`] that contains
/// `content`, otherwise a [`ModelarDbMacrosError`] is returned.
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

/// Return [`Ok`] if the next [`TokenTree`] from `token_iterator` is an [`Punct`] that contains
/// `content`, otherwise a [`ModelarDbMacrosError`] is returned.
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

/// Appends `code` to `input`.
fn append_code_to_token_stream(mut input: TokenStream, code: String) -> TokenStream {
    let implementation_tokens = code
        .parse::<TokenStream>()
        .expect("The macro generated invalid Rust code.");
    input.extend(implementation_tokens);
    input
}

/// Generate a call to `function_name` and pass it the `permutation` with replacements of arguments.
fn generate_test_function(function_name: &str, permutation: &[&BorrowedArgument]) -> String {
    let permutation_name = permutation.iter().map(|ba| &ba.name).join("__");

    let mut temp_dir_name_counter = 0;
    let temp_dirs = permutation
        .iter()
        .filter_map(|ba| {
            if ba.need_path {
                temp_dir_name_counter += 1;
                Some(format!(
                    "let temp_dir{temp_dir_name_counter} = tempfile::tempdir().unwrap();"
                ))
            } else {
                None
            }
        })
        .join(" ");

    temp_dir_name_counter = 0;
    let arguments = permutation
        .iter()
        .map(|ba| {
            let mut argument = String::new();
            argument.push_str(&format!("&{}::{}", ba.module, ba.name));
            if ba.need_path {
                temp_dir_name_counter += 1;
                argument.push_str(&format!("(temp_dir{temp_dir_name_counter}.path())"));
            } else {
                argument.push_str("()");
            }
            if ba.need_await {
                argument.push_str(".await")
            }
            argument
        })
        .join(", ");

    format!(
        "
            #[tokio::test]
            async fn {function_name}__{permutation_name}() {{
                {temp_dirs}
                {function_name}({arguments}).await
            }}
        "
    )
}
