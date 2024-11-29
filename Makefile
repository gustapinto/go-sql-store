test:
	go test -count=1 ./...

test/verbose:
	go test -v -count=1 ./...

run:
	go run ./cmd/main.go

build:
	go build -o ./bin/go-sql-store ./cmd/main.go
