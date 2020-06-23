# go-whosonfirst-elasticsearch

## Important

This is work in progress. Changes should be expected and documentation to follow.

## Tools

### es-whosonfirst-index

```
> go run -mod vendor cmd/es-whosonfirst-index/main.go -h
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