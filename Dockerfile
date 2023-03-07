FROM rust:1.67

WORKDIR /usr/src/myapp

COPY . .

RUN apt-get update && apt-get -y install cmake protobuf-compiler

RUN cargo build

CMD ["target/debug/modelardbd", "edge", "data", "s3://modelardata"]
