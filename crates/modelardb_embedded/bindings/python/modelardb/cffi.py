# Copyright 2026 The ModelarDB Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pathlib

import cffi

MODELARDB_EMBEDDED_HEADER_NAME = "modelardb_embedded.h"


def _read_header() -> str:
    """Read the C header for modelardb_embedded and strip preprocessor
    directives and C++ guards that cffi cannot process.

    :raises FileNotFoundError: If the C header cannot be found.
    """
    # Attempt to load the header installed as part of the Python package.
    header_path = pathlib.Path(__file__).parent.resolve() / MODELARDB_EMBEDDED_HEADER_NAME

    if not header_path.exists():
        # Attempt to load the header from the development repository.
        header_path = pathlib.Path(__file__).parent.parent.parent.resolve() / "c" / MODELARDB_EMBEDDED_HEADER_NAME

    if not header_path.exists():
        raise FileNotFoundError(f"The C header {MODELARDB_EMBEDDED_HEADER_NAME} was not found.")

    content = header_path.read_text(encoding="UTF-8")

    # Remove preprocessor directives as cffi does not support them.
    content = "\n".join(line for line in content.splitlines() if not line.lstrip().startswith("#"))

    # Remove the extern "C" { ... } wrapper as it is C++ syntax, not C.
    content = content.replace('extern "C" {', "").replace('} // extern "C"', "")

    return content


ffi = cffi.FFI()
ffi.cdef(_read_header())
