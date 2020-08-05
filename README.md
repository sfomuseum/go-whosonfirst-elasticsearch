# go-whosonfirst-elasticsearch

## Important

This is work in progress. Changes should be expected and documentation to follow.

## Tools

To build binary versions of these tools run the `cli` Makefile target. For example:

```
$> make cli
go build -mod vendor -o bin/es-whosonfirst-index cmd/es-whosonfirst-index/main.go
```

### es-whosonfirst-index

```
> bin/es-whosonfirst-index -h
  -append-spelunker-v1-properties
	...
  -elasticsearch-endpoint string
    			  ... (default "http://localhost:9200")
  -elasticsearch-index string
    		       ... (default "millsfield")
  -index-alt-files
	...
  -index-only-properties
	...
  -index-spelunker-v1
	...
  -indexer-uri string
    	       ... (default "repo://")
  -workers int
    	   ... (default 2)
```

## Elasticsearch

This code assumes Elasticsearch 7.x

## See also

* https://github.com/elastic/go-elasticsearch