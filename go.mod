module github.com/sfomuseum/go-whosonfirst-elasticsearch

go 1.16

// Note that elastic/go-elasticsearch/v7 v7.13.0 is the last version known to work with AWS
// Elasticsearch instances. v7.14.0 and higher will fail with this error message:
// "the client noticed that the server is not a supported distribution of Elasticsearch"
// Good times...

require (
	github.com/cenkalti/backoff/v4 v4.1.2
	github.com/elastic/go-elasticsearch/v7 v7.13.0
	github.com/sfomuseum/go-edtf v0.3.1
	github.com/sfomuseum/go-flags v0.8.2
	github.com/tidwall/gjson v1.12.0
	github.com/tidwall/sjson v1.2.3
	github.com/whosonfirst/go-whosonfirst-iterate-git/v2 v2.0.0
	github.com/whosonfirst/go-whosonfirst-iterate/v2 v2.0.1
	github.com/whosonfirst/go-whosonfirst-placetypes v0.3.0
	github.com/whosonfirst/go-whosonfirst-uri v1.1.0
	gopkg.in/olivere/elastic.v3 v3.0.75
)
