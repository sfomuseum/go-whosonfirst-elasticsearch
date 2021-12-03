# go-whosonfirst-edtf

Go package for working with Extended DateTime Format (EDTF) strings in Who's On First documents.

## Tools

```
> make cli
go build -mod vendor -o bin/find-invalid cmd/find-invalid/main.go
go build -mod vendor -o bin/update-unknown-uncertain cmd/update-unknown-uncertain/main.go
```

### find-invalid

```
$> ./bin/find-invalid -h
Usage of ./bin/find-invalid:
  -include-key
    	Include edtf: property of relevant Who's On First record in output. (default true)
  -include-path
    	Include path of relevant Who's On First record in output. (default true)
  -iterator-uri string
    	A valid whosonfirst/go-whosonfirst-iterate/emitter URI. Supported emitter URI schemes are: directory://,featurecollection://,file://,filelist://,geojsonl://,repo:// (default "repo://")
```

For example:

```
$> ./bin/find-invalid -include-path=false /usr/local/data/whosonfirst-data-admin-ca | sort | uniq
edtf:cessation,open
edtf:cessation,uuuu
edtf:inception,uuuu
```

### update-open-unknown

Update `edtf:` properties with pre-ISO-8601:1-2 "open" or "unknown" values (`open` and `uuuu` respectively).

```
$> ./bin/update-unknown-uncertain -h
Usage of ./bin/update-unknown-uncertain:
  -exporter-uri string
    	A valid whosonfirst/go-whosonfirst-export URI. (default "whosonfirst://")
  -iterator-uri string
    	A valid whosonfirst/go-whosonfirst-iterate/emitter URI. Supported emitter URI schemes are: directory://,featurecollection://,file://,filelist://,geojsonl://,repo:// (default "repo://")
  -writer-uri string
    	A valid whosonfirst/go-writer URI. (default "null://")
```

For example:

```
$> ./bin/update-unknown-uncertain \
	-writer-uri file:///usr/local/data/sfomuseum-data-architecture/data \
	/usr/local/data/sfomuseum-data-architecture/
	
2021/02/02 16:26:55 Updated 1376996231 (/usr/local/data/sfomuseum-data-architecture/data/137/699/623/1/1376996231.geojson)
2021/02/02 16:26:55 Updated 1159162825 (/usr/local/data/sfomuseum-data-architecture/data/115/916/282/5/1159162825.geojson)
2021/02/02 16:26:55 Updated 1159160869 (/usr/local/data/sfomuseum-data-architecture/data/115/916/086/9/1159160869.geojson)
2021/02/02 16:26:55 Updated 1477863281 (/usr/local/data/sfomuseum-data-architecture/data/147/786/328/1/1477863281.geojson)
2021/02/02 16:26:55 Updated 1159162827 (/usr/local/data/sfomuseum-data-architecture/data/115/916/282/7/1159162827.geojson)
2021/02/02 16:26:55 Updated 1477863283 (/usr/local/data/sfomuseum-data-architecture/data/147/786/328/3/1477863283.geojson)
2021/02/02 16:26:55 Updated 1477863285 (/usr/local/data/sfomuseum-data-architecture/data/147/786/328/5/1477863285.geojson)
2021/02/02 16:26:55 Updated 1477855661 (/usr/local/data/sfomuseum-data-architecture/data/147/785/566/1/1477855661.geojson)
2021/02/02 16:26:55 Updated 1477855663 (/usr/local/data/sfomuseum-data-architecture/data/147/785/566/3/1477855663.geojson)
2021/02/02 16:26:55 Updated 1477855821 (/usr/local/data/sfomuseum-data-architecture/data/147/785/582/1/1477855821.geojson)
2021/02/02 16:26:55 Updated 1477855823 (/usr/local/data/sfomuseum-data-architecture/data/147/785/582/3/1477855823.geojson)
2021/02/02 16:26:55 Updated 1477855831 (/usr/local/data/sfomuseum-data-architecture/data/147/785/583/1/1477855831.geojson)
... and so on
```

## See also

* https://github.com/whosonfirst/go-edtf
* https://github.com/whosonfirst/go-whosonfirst-iterate
* https://github.com/whosonfirst/go-whosonfirst-export