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

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Rerun the build script if the proto definition changes.
    println!("cargo::rerun-if-changed=src/flight/protocol.proto");

    // Compile with the pure Rust protox compiler instead of protoc, then let prost-build generate
    // Rust from the resulting FileDescriptorSet.
    let file_descriptors = protox::compile(["src/flight/protocol.proto"], ["src/"])?;
    prost_build::compile_fds(file_descriptors)?;

    Ok(())
}
