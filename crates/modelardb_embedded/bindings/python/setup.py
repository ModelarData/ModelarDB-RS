# Copyright 2025 The ModelarDB Contributors
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

import os
import sys
import shutil
from pathlib import Path

from setuptools import setup, find_packages, Extension
from setuptools.command.sdist import sdist
from setuptools.command.build_ext import build_ext
from setuptools.command.bdist_wheel import bdist_wheel

CRATES_FOLDER_NAME = "crates"
CARGO_BUILD_NAME = "Cargo.toml"
CARGO_LOCK_NAME = "Cargo.lock"


def get_repository_root():
    return Path.cwd().parent.parent.parent.parent


def ignore_bindings_folder(path, content):
    bindings_folder_name = "bindings"
    if bindings_folder_name in content:
        return [bindings_folder_name]
    return []


def copy_file_to_cwd(source_folder: Path, file_name: Path):
    source_file_path = source_folder / file_name
    target_file_path = Path.cwd() / file_name
    shutil.copyfile(source_file_path, target_file_path)


def copy_src_if_repository():
    """Copies the Rust source code to the current directory if in the repository."""
    cwd = Path.cwd()

    target_crates_folder = cwd / CRATES_FOLDER_NAME
    if target_crates_folder.exists():
        return

    repository_root = get_repository_root()
    git_folder = repository_root / ".git"
    if git_folder.exists():
        source_crates_folder = repository_root / CRATES_FOLDER_NAME
        shutil.copytree(source_crates_folder, target_crates_folder,
                        ignore=ignore_bindings_folder)
        copy_file_to_cwd(repository_root, CARGO_BUILD_NAME)
        copy_file_to_cwd(repository_root, CARGO_LOCK_NAME)
        return

    raise FileNotFoundError("Failed to locate Rust source code.")


def delete_src_if_repository():
    """Deletes the Rust source code in the current directory if in the repository."""
    cwd = Path.cwd()

    crates_folder = cwd / CRATES_FOLDER_NAME
    cargo_build_file = cwd / CARGO_BUILD_NAME
    cargo_lock_file = cwd / CARGO_LOCK_NAME

    repository_root = get_repository_root()
    git_folder = repository_root / ".git"
    if git_folder.exists():
        # Errors are ignored to ensure all files are deleted.
        shutil.rmtree(crates_folder, ignore_errors=True)
        cargo_build_file.unlink(missing_ok=True)
        cargo_lock_file.unlink(missing_ok=True)


class RustSDist(sdist):
    def run(self):
        copy_src_if_repository()
        super().run()
        delete_src_if_repository()


class RustBDistWheel(bdist_wheel):
    def run(self):
        copy_src_if_repository()
        super().run()
        delete_src_if_repository()

    def get_tag(self):
        python, abi, plat = super().get_tag()
        # modelardb_embedded is a native library, not a Python extension.
        python, abi = 'py3', 'none'
        return python, abi, plat


class RustBuildExt(build_ext):
    def build_extension(self, ext):
        dependencies = ["cargo", "rustc", "protoc"]
        for dependency in dependencies:
            if not shutil.which(dependency):
                raise FileNotFoundError(f"Requires {', '.join(dependencies)}. Missing {dependency}")

        self.spawn(["cargo", "build", "--package", "modelardb_embedded", "--lib"])#, "--release"])
        shutil.move("target/debug/libmodelardb_embedded.so", self.get_ext_fullpath(ext.name))

    def get_ext_filename(self, ext_name):
        # Removes the CPython part of ext_name as the library is not linked to
        # CPython and it simplifies the code for loading the library in Python.
        filename = super().get_ext_filename(ext_name)
        start = filename.find(".")
        end = filename.rfind(".")
        return filename[:start] + filename[end:]


setup(
    packages=find_packages(exclude=("tests")),
    include_package_data=True,
    ext_modules=[Extension("modelardb_embedded", sources=[])],
    cmdclass={"sdist": RustSDist, "bdist_wheel": RustBDistWheel, "build_ext": RustBuildExt },
)
