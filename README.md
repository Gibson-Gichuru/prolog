# Prolog

This project is a distributed log system implemented in Go. It provides a gRPC-based API for producing and consuming log records, with features such as streaming, error handling, and efficient storage management.

## Features

- **gRPC API**: Supports producing and consuming log records via gRPC.
- **Streaming**: Enables streaming of log records for efficient data transfer.
- **Error Handling**: Implements custom error types for robust error reporting.
- **Efficient Storage**: Uses segmented storage with indexing for fast reads and writes.
- **Testing**: Comprehensive unit tests for all components.

## Project Structure

The project is organized as follows:

```
/api/v1/          # Protocol Buffers definitions and generated Go code for the gRPC API.
  log.proto       # The Protobuf definition for the log service.
  log.pb.go       # Generated Go code for Protobuf messages.
  log_grpc.pb.go  # Generated Go code for gRPC service.
  error.go        # Custom error types for the API.

/internal/log/     # Core log implementation.
  log.go          # Main log implementation with segmented storage.
  store.go        # Storage layer for log records.
  index.go        # Indexing layer for fast record lookups.
  segment.go      # Segment abstraction for log storage.
  config.go       # Configuration for log segments.
  store_test.go   # Unit tests for the storage layer.
  index_test.go   # Unit tests for the indexing layer.
  segment_test.go # Unit tests for the segment abstraction.
  log_test.go     # Unit tests for the main log implementation.

/internal/server/  # gRPC server implementation.
  server.go       # gRPC server for the log service.
  server_test.go  # Unit tests for the gRPC server.

/github/workflows/ # GitHub Actions for CI/CD.
  test.yml        # Workflow for running tests on push.

/Makefile          # Makefile for building, testing, and running the project.
```

## Setup

### Prerequisites

- Go 1.24 or later
- Protocol Buffers compiler (`protoc`)
- `protoc-gen-go` and `protoc-gen-go-grpc` plugins

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Gibson-Gichuru/prolog.git
   cd prolog
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Generate Protobuf code (if needed):
   ```bash
   protoc --go_out=. --go-grpc_out=. api/v1/log.proto
   ```

## Usage

### Build

To build the project, run:
```bash
make build
```

This will create an executable named `app`.

### Run

To start the server, run:
```bash
make run
```

The server will start and listen for gRPC requests.

### Test

To run the tests, use:
```bash
make test
```

This will execute all unit tests in the project.

## API Overview

The gRPC API provides the following methods:

- **Produce**: Appends a record to the log.
- **Consume**: Reads a record from the log by offset.
- **ProduceStream**: Streams records to the log.
- **ConsumeStream**: Streams records from the log starting at a given offset.

### Example Protobuf Messages

#### ProduceRequest
```proto
message ProduceRequest {
    Record record = 1;
}
```

#### ConsumeRequest
```proto
message ConsumeRequest {
    uint64 offset = 1;
}
```

#### Record
```proto
message Record {
    bytes value = 1;
    uint64 offset = 2;
}
```

## Testing

The project includes comprehensive unit tests for all components. Tests are located in the `internal/log` and `internal/server` directories. To run the tests, use:
```bash
make test
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes with clear messages.
4. Submit a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
