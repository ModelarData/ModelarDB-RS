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

//! Implementation of functions that return information about the system.

use std::sync::LazyLock;

use sysinfo::System;

static SYSINFO_SYSTEM: LazyLock<System> = LazyLock::new(|| {
    let mut system = System::new();
    system.refresh_all();
    system
});

/// Return a percentage of the system's total memory.
pub fn percentage_of_total_memory(percentage: f64) -> u64 {
    ((SYSINFO_SYSTEM.total_memory() as f64 / 100.0) * percentage) as u64
}

/// Return the total amount of memory on the system.
pub fn total_memory() -> u64 {
    SYSINFO_SYSTEM.total_memory()
}
