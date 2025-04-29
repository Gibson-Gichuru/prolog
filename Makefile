test:
	go test -v ./...

setup:
	go mod tidy
	go mod download
	go mod verify

gen:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/log.proto

build:
	go build -o app
run:
	go run main.go