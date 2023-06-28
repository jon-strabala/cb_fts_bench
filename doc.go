/*
Fast cross-platform HTTP benchmarking tool with COUCHBASE Eveneing Queues ack testing code

benchmarking tool written in Go.

Installation:
  go get -u github.com/jon-strabala/cb_fts_bench
  note started as a copy of: github.com/codesenberg/bombardier

Usage: ./cb_queue_bench [<flags>] <url>

Flags:
      --help                  Show context-sensitive help (also try --help-long and --help-man).
      --version               Show application version.
  -c, --connections=125       Maximum number of concurrent connections
  -t, --timeout=2s            Socket/request timeout
  -l, --latencies             Print latency statistics
  -m, --method=GET            Request method
  -b, --body=""               Request body
  -f, --body-file=""          File to use as request body
  -s, --stream                Specify whether to stream body using chunked transfer encoding or to serve it
                              from memory
      --cert=""               Path to the client's TLS Certificate
      --key=""                Path to the client's TLS Certificate Private Key
  -k, --insecure              Controls whether a client verifies the server's certificate chain and host
                              name
  -a, --disableKeepAlives     Disable HTTP keep-alive. For fasthttp use -H 'Connection: close'
  -H, --header="K: V" ...     HTTP headers to use(can be repeated)
  -n, --requests=[pos. int.]  Number of requests
  -d, --duration=10s          Duration of test
  -r, --rate=[pos. int.]      Rate limit in requests per second
      --fasthttp              Use fasthttp client
      --http1                 Use net/http client with forced HTTP/1.x
      --http2                 Use net/http client with enabled HTTP/2.0
  -p, --print=<spec>          Specifies what to output. Comma-separated list of values 'intro' (short: 'i'),
                              'progress' (short: 'p'), 'result' (short: 'r'). Examples:

                                * i,p,r (prints everything)
                                * intro,result (intro & result)
                                * r (result only)
                                * result (same as above)

  -q, --no-print              Don't output anything
  -o, --format=<spec>         Which format to use to output the result. <spec> is either a name (or its
                              shorthand) of some format understood by bombardier or a path to the
                              user-defined template, which uses Go's text/template syntax, prefixed with
                              'path:' string (without single quotes), i.e.
                              "path:/some/path/to/your.template" or "path:C:\some\path\to\your.template" in
                              case of Windows. Formats understood by bombardier are:

                                * plain-text (short: pt)
                                * json (short: j)

  -u, --basicauth=user:pass   COUCHBASE: Basic Auth
  -A, --customAck             COUCHBASE: Couchbase custom /deq and /deq/bulk ack
  -V, --debugCustomAck        COUCHBASE: Vebose debug of Couchbase custom /deq and /deq/bulk ack
  -D, --dynDoc                COUCHBASE: Ignore -b and -f, Dynamically generate POST body or data to send
  -Q, --dynDocShow            COUCHBASE: show the dynamically generate doc for the POST body
  -S, --dynDocSz=72           COUCHBASE: Size in byts of each dynamic JSON doc to send default 72, min 12 If
                              >= 40 will add "binfo": "##########@####" a 10 digit request #, @ seperator,
                              and 4 digit batch # which is useful for seeing how batching works.
  -B, --dynDocBatchSz=0       COUCHBASE: Batch size of array to send: 0 = no batch, 1 = batch of 1, 64 =
                              batch of 64
  -F, --trace                 COUCHBASE: show responses on first document
  -N, --nobatchnum            COUCHBASE: do not tag batch # in "binfo": "0000000000@####" when making
                              dynamic docs
  -F, --dynFtsShow            COUCHBASE: show the dynamically generated FTS queries 
  -L, --dynFtsLimit           COUCHBASE: limit/select dynamically generated FTS query test
  -M, --showMetering          COUCHBASE: show the FTS metering before and after the queries 
  -j, --altMeteringHost       COUCHBASE: also get info from althost IP or FQDN 
  -e, --minBackoff=0          COUCHBASE: In the event of HTTP 429 override the backoff seq. with this a min

sampleReviewWords 18765
commonReviewWords    50


    Data used to generate the FTS search queries comes form the follwoing sources

	# sampleReviewWords            has     18765 items from travel-sample._defaul._default reviews.content
	# hotelLocationLatLons         has      5238 items from travel-sample._defaul._default geo.lat and geo.lon (both hotel and location)
	# commonReviewWords            has        50 items from https://www.researchgate.net/figure/Fifty-Most-Frequently-used-Words-in-60-648-Hotel-Review-Comments_tbl1_233475559
	# commonEnglishWords           has       100 items form https://www.espressoenglish.net/the-100-most-common-words-in-english/
	# commonVerbWords              has        34 items from https://literacyforall.org/docs/100_Most_common_in_American_English.pdf


    1: //  0.0% misses
	repl = buildBasicRandomQueryNumWords(1, conf.sampleReviewWords, conf.sampleReviewWordsLen)

        select one random word from sampleReviewWords 

	example: 
		"query": { "query": "+euro" } 

    2: // 90.4% misses
	repl = buildBasicRandomQueryNumWords(2, conf.sampleReviewWords, conf.sampleReviewWordsLen)

        selects two random words from sampleReviewWords 

	example: 
		"query": { "query": "+wong +skews" } 

    3: //  0.0% misses
	repl = buildBasicRandomQueryNumWords(1, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from commonReviewWords

	example: 
		"query": { "query": "+Store" } 

    4: //  1.3%  misses
	repl = buildBasicRandomQueryNumWords(2, conf.commonReviewWords, conf.commonReviewWordsLen)

        select two random words from commonReviewWords

	example: 
		"query": { "query": "+Toilet +Noisy" } 

    5: // 10.6% misses
	repl = buildBasicRandomQueryNumWords(3, conf.commonReviewWords, conf.commonReviewWordsLen)

        select three random words from commonReviewWords

	example: 
		"query": { "query": "+Downtown +Clerk +View" } 


    6: // 30.4% misses
	repl = buildBasicRandomQueryNumWords(4, conf.commonReviewWords, conf.commonReviewWordsLen)

        select four random words from commonReviewWords

	example: 
		"query": { "query": "+Bath +Dinner +Clerk +Price" }


    7: //  51.0% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 1, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select one random word from commonReviewWords

	example: 
		"query": { "query": "+absolut +Broken" }


    8: // 77.0% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 2, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select two random words from commonReviewWords

	example: 
		"query": { "query": "+abbas +Staff +Noisy" } 

    9: // 88.4% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 3, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select three random words from commonReviewWords

	example: 
		"query": { "query": "+aboard +Towel +Rate +Noise" }

    10: // 94.7% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 4, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select four random words from commonReviewWords

	example: 
		"query": { "query": "+abby +Store +Downtown +Dine +Dinner" }

    11: // 74.1% misses
	repl = buildFuzzyRandomQuery(1, 5, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from commonReviewWords with length at least 5 chars and apply fuzziness of 1
	AND then randomly replace one char [a-z] in any position of the term to seach for from commonReviewWords

	example (the word was Sheet):
		"query": { "term": "Shdet", "fuzziness": 1 } 

    12: //  0.0% misses
	repl = buildFuzzyRandomQuery(2, 5, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from commonReviewWords with length at least 5 chars and apply fuzziness of 2
	AND then randomly replace one char [a-z] in any position of the term to seach for from commonReviewWords

	example (the word was Sheet):
		"query": { "term": "Downiown", "fuzziness": 2 } 

    13: // 14.0% misses
	repl = buildPseudoGeoRandomQuery(0.25, conf, c) // 2 range conjuncts

	Take a random hotel location lat/lon and apply some random offsets to get a new lat/lon point then build a
        bounding box by adjusting the centroid by random amounts - this is a pseudo geo search - via conjuncts

	example:
		"query": {"conjuncts":[ 
			{  "min": -122.479164, "max": -122.345833,  "inclusive_min": false,  "inclusive_max": false,  "field": "geo.lon" },
			{  "min": 37.801128, "max": 37.909462,  "inclusive_min": false,  "inclusive_max": false,  "field": "geo.lat" }
		]}

    99: //  9.9% misses
	repl = buildBasicRandomRandomTermsQuery(conf, c)

        select two random word from commonReviewWords
	50% probability add another random word from commonReviewWords
	50% probability add another random word from commonEnglishWords
	50% probability add another random word from commonVerbWords

	example:
		"query": { "query": "+Noisy +Affordable +Staff +have" } 
	

    OTHER: // 16.5% misses

	If no test is selected we randomly apply thefolloing

	// 80% or 8 out of 10 queries are radom query terms
	repl = buildBasicRandomRandomTermsQuery(conf, c)

	// 10% or 1 out of 10 queries are simple fuzzy
	 repl = buildFuzzyRandomQuery(1, 5, conf.commonReviewWords, conf.commonReviewWordsLen)

	// 10% ro 1 out of 10 queries are 2-"conjuncts" min/max
	repl = buildPseudoGeoRandomQuery(0.25, conf, c)



	



Args:
  <url>  Target's URL

For detailed documentation on user-defined templates see
documentation for package github.com/codesenberg/bombardier/template.
Link (GoDoc):
https://godoc.org/github.com/codesenberg/bombardier/template

*/
package main
