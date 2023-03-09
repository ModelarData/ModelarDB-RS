FROM rust:1.67
# Copyright 2023 The ModelarDB Contributors
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

WORKDIR /usr/src/myapp

COPY . .

RUN apt-get update && apt-get -y install cmake protobuf-compiler

RUN cargo build

CMD ["target/debug/modelardbd", "edge", "data", "s3://modelardata"]
