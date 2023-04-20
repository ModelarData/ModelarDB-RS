## Storage Engine
The storage engine component is responsible for managing data points while converting them from their initial uncompressed 
state to their final compressed model-based state. Since data is compressed in segments, the storage engine is responsible 
for efficiently managing these segments until they can be compressed. When compressed, the storage engine manages the 
data until it can be saved to disk. The storage engine is designed to avoid unnecessary I/O by having an efficient 
in-memory data structure for uncompressed and compressed data, only using file buffers when necessary.

Beyond providing functionality for processing the ingested data, the storage engine is also responsible for providing
a consistent interface for all I/O operations. This includes both reading and writing files to local storage and
transferring compressed data to the remote object store.

### Uncompressed data

### Compressed data

### Data transfer
