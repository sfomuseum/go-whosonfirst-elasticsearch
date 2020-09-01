# go-whosonfirst-elasticsearch

## Important

This is work in progress. Changes should be expected and comprehensive documentation to follow.

## Tools

To build binary versions of these tools run the `cli` Makefile target. For example:

```
$> make cli
go build -mod vendor -o bin/es-whosonfirst-index cmd/es-whosonfirst-index/main.go
```

### es-whosonfirst-index

```
> ./bin/es-whosonfirst-index -h
  -append-spelunker-v1-properties
    	Append and index auto-generated Whos On First Spelunker properties.
  -elasticsearch-endpoint string
    	A fully-qualified Elasticsearch endpoint. (default "http://localhost:9200")
  -elasticsearch-index string
    	A valid Elasticsearch index. (default "millsfield")
  -index-alt-files
    	Index alternate geometries.
  -index-only-properties
    	Only index GeoJSON Feature properties (not geometries).
  -index-spelunker-v1
    	Index GeoJSON Feature properties inclusive of auto-generated Whos On First Spelunker properties.
  -indexer-uri string
    	A valid whosonfirst/go-whosonfirst-index URI string. (default "repo://")
  -workers int
    	The number of concurrent workers to index data using. Default is the value of runtime.NumCPU().
```	

For example:

```
$> bin/es-whosonfirst-index \
	-index-spelunker-v1
	-elasticsearch-index whosonfirst \
	/usr/local/data/whosonfirst-data-admin-ca
```

### Known-knowns

#### index-spelunker-v1

* Support for generating `date:` properties derived from `edtf:` property values is currently not available. This is currently blocked on the lack of a Go language `Extended DateTime Format` parser.

## Elasticsearch

This code assumes Elasticsearch 7.x

## See also

* https://github.com/elastic/go-elasticsearch
* https://github.com/whosonfirst/go-whosonfirst-index