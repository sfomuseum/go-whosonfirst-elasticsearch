cli:
	go build -mod vendor -o bin/es-whosonfirst-index cmd/es-whosonfirst-index/main.go
	go build -mod vendor -o bin/es2-whosonfirst-index cmd/es2-whosonfirst-index/main.go
