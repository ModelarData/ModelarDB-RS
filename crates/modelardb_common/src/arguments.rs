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

use std::process;
use std::str;

/// Collect the command line arguments that this program was started with.
pub fn collect_command_line_arguments(maximum_arguments: usize) -> Vec<String> {
    let mut args = std::env::args();
    args.next(); // Skip the executable.

    // Collect at most the maximum number of command line arguments plus one. The plus one argument
    // is collected to trigger the default pattern when parsing the command line arguments with
    // pattern matching, making it possible to handle errors caused by too many arguments.
    args.by_ref().take(maximum_arguments + 1).collect()
}

/// Prints a usage message with `parameters` appended to the name of the binary executing this
/// function to stderr and exits with status code one to indicate that an error has occurred.
pub fn print_usage_and_exit_with_error(parameters: &str) -> ! {
    // The errors are consciously ignored as the program is terminating.
    let binary_path = std::env::current_exe().unwrap();
    let binary_name = binary_path.file_name().unwrap().to_str().unwrap();

    // Punctuation at the end does not seem to be common in the usage message of Unix tools.
    eprintln!("Usage: {binary_name} {parameters}");
    process::exit(1);
}
