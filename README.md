# cb_queue_bench 
Fast cross-platform HTTP benchmarking tool with COUCHBASE FTS testing code


# based on bombardier [![Build Status](https://semaphoreci.com/api/v1/codesenberg/bombardier/branches/master/shields_badge.svg)](https://semaphoreci.com/codesenberg/bombardier) [![Go Report Card](https://goreportcard.com/badge/github.com/codesenberg/bombardier)](https://goreportcard.com/report/github.com/codesenberg/bombardier) [![GoDoc](https://godoc.org/github.com/codesenberg/bombardier?status.svg)](http://godoc.org/github.com/codesenberg/bombardier)
bombardier is a HTTP(S) benchmarking tool. It is written in Go programming language and uses a slightly modified version of the excellent [fasthttp](https://github.com/valyala/fasthttp) instead of Go's default http library, because of its lightning fast performance. 

With `bombardier v1.1` and higher you can now use `net/http` client if you need to test HTTP/2.x services or want to use a more RFC-compliant HTTP client.

Tested on go1.8 and higher.

## Installation
You can grab binaries in the [releases] TBD section.
Alternatively, to get latest and greatest run:

`go get -u github.com/jon-strabala/cb_fts_bench`

## Usage
```
cb_fts_bench [<flags>] <url>
```

Flags in cb_fts_bench added to bombardier
```                          
  -u, --basicauth=user:pass      COUCHBASE: Basic Auth
  -R, --debugRateLimit           COUCHBASE: Verbose debug of Couchbase custom /deq and /deq/bulk rate limit or throttle
  -T, --trace                    COUCHBASE: show all requests and all responses
  -F, --dynFtsShow               COUCHBASE: show the dynamically generated queries for the FTS benchmarking and the total_hits
  -M, --showMetering             COUCHBASE: show the FTS metering before and after
  -j, --altMeteringHost=""       COUCHBASE: also access metering from this host some with URL target
  -L, --dynFtsLimit=DYNFTSLIMIT  COUCHBASE: select the dynamically generated query test for FTS 1 31-43, 99
  -J, --ftsTestHelp              COUCHBASE: show details on above tests 1-13, 99 used for the FTS benchmarking (sorry, you most give an arg after -J or --ftsTestHelp)
  -K, --kvHost=localhost         COUCHBASE: Utilize KV 'host' to perform document lookups for FTS hits
  -C, --kvBucket=bucket          COUCHBASE: Utilize bucket._default.default to perform document lookups for FTS hits
  -z, --dynKvShow                COUCHBASE: if -K 'host' show the first 110 chars of any KV reads to resolve FTS docs
  -e, --minBackoff=MINBACKOFF    COUCHBASE: min backoff in ms. HTTP 429 retry progression, default 0


```

For a more detailed information about bombardier flags consult [GoDoc](http://godoc.org/github.com/codesenberg/bombardier).

## Known issues
AFAIK, it's impossible to pass Host header correctly with `fasthttp`, you can use `net/http`(`--http1`/`--http2` flags) to workaround this issue.


## Examples Couchbase FTS OnPrem or Self Managed server 
```
CB_USERNAME=Administrator
CB_PASSWORD=mypasswd
CB_FTSHOST=192.168.3.150
CB_KVHOST=192.168.3.150

#-----------------------------------------
# setup ... 
#-----------------------------------------
# 
# 1) make 4 buckets "ts01", "ts02", "ts03", "ts04"
# 
# 2) load TS into all buckets
#
# 	cd /opt/couchbase/samples
# 	ln -s travel-sample.zip ts01.zip
# 	ln -s travel-sample.zip ts02.zip
# 	ln -s travel-sample.zip ts03.zip
# 	ln -s travel-sample.zip ts04.zip
# 	curl -X POST -u ${CB_USERNAME}:${CB_PASSWORD} http://${CB_FTSHOST}:8091/sampleBuckets/install -d '["ts01", "ts02", "ts03", "ts04"]'
#
#  2) build the SEARH indexes needed, I typically just use the default dynamic index on each bucket named
#	ts01_fts_01 in bucket ts01
#	ts01_fts_02 in bucket ts02
#	ts01_fts_03 in bucket ts03
#	ts01_fts_04 in bucket ts04
#
#-----------------------------------------

# for help ...

./cb_fts_bench --help

#-----------------------------------------
# TEST simple use with a static query (this is the only example where I show an output)
#-----------------------------------------

# 125 clients 1,000,000 requess to one bucket one index

time ./cb_fts_bench -m POST -H  "Content-Type: application/json"  -k -a -u ${CB_USERNAME}:${CB_PASSWORD} \
	-n 1000000  http://${CB_FTSHOST}:8094/api/index/ts01_fts_01/query \
	-b '{"query":{"match":"balcony","field":"reviews.content"},"size":10}' \
	-c 125 -L 1 

# output from above (note ALL responses are 2xx this means success)
#
#	Bombarding http://192.168.3.150:8094/api/index/ts01_fts_01/query with 1000000 request(s) using 125 connection(s)
#	 1000000 / 1000000 [=============================================] 100.00% 34473/s 29s
#	Done!
#	Statistics        Avg      Stdev        Max
#	  Reqs/sec     34700.10    4871.70   44101.07
#	  Latency        3.59ms     2.02ms    47.31ms
#	  HTTP codes:
#	    1xx - 0, 2xx - 1000000, 3xx - 0, 4xx - 0, 5xx - 0
#	    others - 0
#	  Throughput:    73.30MB/s
#	  Ave Sizes
#	    resp.body:            1813 bytes (across   1000000 reqs)
#	    resp.body.hit[]:      1401 bytes (across   1000000 reqs having hits [100.00%])
#	  Other
#	    total_hits           294000000, ave   294.000
#	    tot_hits_docreads     10000000, ave    10.000
#	real    0m29.047s
#	user    2m0.810s
#	sys     0m40.394s


# TEST 1 clients 1 requess to one bucket one index with trace (full debug)

time ./cb_fts_bench -m POST -H  "Content-Type: application/json"  -k -a -u ${CB_USERNAME}:${CB_PASSWORD} \
	-n 1  http://${CB_FTSHOST}:8094/api/index/ts01_fts_01/query \
	-b '{"query":{"match":"balcony","field":"reviews.content"},"size":10}' \
	-c 1 -L 1 -T

#-----------------------------------------
# TEST advanced use with a static query
#-----------------------------------------

# Randomly select a buckets nameed "ts01", "ts02", "ts03", "ts04" (they contain `travel-sample` and a dynic default FTS index)
# load it by 

# send 100,000 requests of the query '{"query":{"match":"balcony","field":"reviews.content"},"size":10}' to FTS
# If you have an issue (like 429 errors) you can lower the number of request clients "-c 125" to say "-c 32" 
# and/or rat limit via -r 1000 (as in 1000 max requests/sec.) 

time ./cb_fts_bench -m POST -H  "Content-Type: application/json"  -k -a -u ${CB_USERNAME}:${CB_PASSWORD} \
	-n 100000  http://${CB_FTSHOST}:8094/api/index/ts[[SEQ:1:4]]_fts_01/query \
	-b '{"query":{"match":"balcony","field":"reviews.content"},"size":10}' \
	-c 125 -L 1 

# send 100,000 requests of the query '{"query":{"match":"balcony","field":"reviews.content"},"size":10}' to FTS
# and then read the top 10 KV docs form KV via batches.  If you have an issue (like 429 errors) you can lower 
# the number of request clients "-c 125" to say "-c 32" and/or rat limit via -r 1000 (as in 1000 max requests/sec.)
# Note the KV read comes form the same bucket as the random request [[SEQ:1:4]] gets replaced with 00, 01, 02, or 04

time ./cb_fts_bench -m POST -H  "Content-Type: application/json"  -k -a -u ${CB_USERNAME}:${CB_PASSWORD} \
	-n 100000  http://${CB_FTSHOST}:8094/api/index/ts[[SEQ:1:4]]_fts_01/query \
	-b '{"query":{"match":"balcony","field":"reviews.content"},"size":10}' \
	-c 125 -L 1 -K ${CB_KVHOST} -C ts[[SEQ:1:4]]

#-----------------------------------------
# TEST advanced use with RANDOM queries
#
# these do random substitutions from a few templates in ./RAND_QUERY_TEMPLATES/
# based on some "largish" hard coded dictionaries. 
#
#        sampleReviewWords    has     18765 items from travel-sample._defaul._default reviews.content
#        hotelLatLons         has       917 items from travel-sample._defaul._default geo.lat and geo.lon
#        commonReviewWords    has        50 items from https://www.researchgate.net/figure/Fifty-Most-Frequently-used-Words-in-60-648-Hotel-Review-Comments_tbl1_233475559
#        commonEnglishWords   has       100 items form https://www.espressoenglish.net/the-100-most-common-words-in-english/
#        commonVerbWords      has        34 items from https://literacyforall.org/docs/100_Most_common_in_American_English.pdf
#
# for more details on all possible tests -L 31 to -L 43 (and -L 99) refer to RAND_QUERY_TEMPLATES.txt
#
# I only will give one sample TEST 32 via '-L 32' select random words from 'sampleReviewWords'
#-----------------------------------------

# selects two random words from sampleReviewWords and populates the template by doing a substitution 
# on __FTS_QUERY__ in  ./RAND_QUERY_TEMPLATES/FTS.json
#
# If ./RAND_QUERY_TEMPLATES/FTS.json contins:
# { __FTS_QUERY__, "size": 10, "from": 0 }
#
# Then we will generate RANDOM queries like:
# { "query": { "query": "+wong +skews" }, "size": 10, "from": 0 }
# { "query": { "query": "+convincing +accessable" }, "size": 10, "from": 0 }
# { "query": { "query": "+odler +react" }, "size": 10, "from": 0 }
# { "query": { "query": "+waiela +influence" }, "size": 10, "from": 0 }
# { "query": { "query": "+recommends +remedied" }, "size": 10, "from": 0 }
#  .....
# { "query": { "query": "+productive +booze" }, "size": 10, "from": 0 }

time ./cb_fts_bench -m POST -H  "Content-Type: application/json"  -k -a -u ${CB_USERNAME}:${CB_PASSWORD} \
	-n 100000  http://${CB_FTSHOST}:8094/api/index/ts[[SEQ:1:4]]_fts_01/query \
	-f ./RAND_QUERY_TEMPLATES/FTS.json \
	-c 125 -L 32 

````

## Examples Couchbase Capella (cloud)

In Capella load the travel-sample data

In Capella allow your IP address to access the database

In Capella create or verify that you have a database user with permissions to access the bucket

	CB_USERNAME=dbuser
	CB_PASSWORD=dbpasswd

In Capella find a node that has FTS running as a service example (you node name with the FTS service will vary):

	svc-dqis-node-003.0y1eouctajsyqla7.cloud.couchbase.com

You will need to provide the -k option to "cb_fts_test" avoid CERT security checks for TLS/SSL 
or optionally install the Capella CERT into your system

You must create an FTS index example assume you indexed all fields in the keyspace 
`travel-sample.inventory.landmark` and it is assumeed you named your index: `myftsindex`

A basic test one request ( -n 1) with a full command respnse dump ( -T):
````
	./cb_fts_bench -m POST -H "Content-Type: application/json" -t 15s -c 1 -k \
		-u ${CB_USERNAME}:${CB_PASSWORD} -n 1 -f ../body.json -T \
		https://svc-dqis-node-003.0y1eouctajsyqla7.cloud.couchbase.com:18094/api/index/myftsindex/query
````
A basic test 1000 requests ( -n 10000) with 100 client threads:
````
	./cb_fts_bench -m POST -H "Content-Type: application/json" -t 15s -c 100 -k \
		-u ${CB_USERNAME}:${CB_PASSWORD} -n 10000 -f ../body.json \
		https://svc-dqis-node-003.0y1eouctajsyqla7.cloud.couchbase.com:18094/api/index/myftsindex/query
````
## Examples Couchbase Elixir (serverless cloud)

Refer to "Examples Couchbase Capella" however the FTS index names are not global, they are scoped so there
is a different endpoint changes from:
````
		https://svc-dqis-node-003.0y1eouctajsyqla7.cloud.couchbase.com:18094/api/index/myftsindex/query
````
to:
````
		https://svc-dqis-node-003.0y1eouctajsyqla7.cloud.couchbase.com:18094/api/bucket/travel-sample/scope/inventory/index/myftsindex/query
````
## Examples (non Couchbase specific same as bombardier)
Example of running `cb_queue_bench` against [this server](https://godoc.org/github.com/codesenberg/bombardier/cmd/utils/simplebenchserver):
```
./cb_queue_bench -c 125 -n 10000000 http://localhost:8080

Bombarding http://localhost:8080 with 10000000 requests using 125 connections
 10000000 / 10000000 [============================================] 100.00% 37s Done!
Statistics        Avg      Stdev        Max
  Reqs/sec    264560.00   10733.06     268434
  Latency      471.00us   522.34us    51.00ms
  ENQUEUE counts:
    enqueue: sent - 0, valid - 0
  DEQUEUE counts:
    dequeue: sent - 0, resp - 0, valid - 0 | ack: sent - 0, resp - 0, valid - 0
  DEQUEUE throttles: 
    throttles - 0
  HTTP codes:
    1xx - 0, 2xx - 10000000, 3xx - 0, 4xx - 0, 5xx - 0
    others - 0
  Throughput:   292.92MB/s
  
  Enq responses:
  Deq responses:
  Ack responses:
```
Or, against a realworld server(with latency distribution):
```
./cb_queue_bench -t 15 -c 200 -d 10s -l http://ya.ru

Bombarding http://ya.ru:80 for 10s using 200 connection(s)
[==========================] 10s
Done!
Statistics        Avg      Stdev        Max
  Reqs/sec       868.75     681.98    3784.49
  Latency      227.45ms   330.96ms      2.45s
  Latency Distribution
     50%   176.74ms
     75%   178.04ms
     90%   178.63ms
     95%   181.52ms
     99%      2.42s
  ENQUEUE counts:
    enqueue: sent - 0, valid - 0
  DEQUEUE counts:
    dequeue: sent - 0, resp - 0, valid - 0 | ack: sent - 0, resp - 0, valid - 0
  DEQUEUE throttles:
    throttles - 0
  HTTP codes:
    1xx - 0, 2xx - 0, 3xx - 0, 4xx - 8875, 5xx - 0
    others - 0
  Throughput:     2.92MB/s

  Enq responses:
  Deq responses:
  Ack responses:
```
