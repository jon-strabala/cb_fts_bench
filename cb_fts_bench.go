package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"regexp"
	"bufio"
	"log"
	"net/http"
	"math/big"

	"github.com/cheggaaa/pb"
	fhist "github.com/codesenberg/concurrent/float64/histogram"
	uhist "github.com/codesenberg/concurrent/uint64/histogram"
	uuid "github.com/satori/go.uuid"

	b64 "encoding/base64"

	"encoding/json"
	"math"
	"math/rand"
	"net/url"
	"strconv"

//	"github.com/jon-strabala/cb_fts_bench/internal"
	"cb_fts_bench/internal"
)


// SafeCounter is safe to use concurrently.
type SafeCounter struct {
	mu sync.Mutex
	v  map[string]int
}

type bombardier struct {
	bytesRead, bytesWritten int64
	reqno                   uint64

	// HTTP codes
	req1xx uint64
	req2xx uint64
	req3xx uint64
	req4xx uint64
	req5xx uint64
	others uint64
	retryReq429 uint64

	conf        config
	barrier     completionBarrier
	ratelimiter limiter
	wg          sync.WaitGroup

	timeTaken time.Duration
	latencies *uhist.Histogram
	requests  *fhist.Histogram

	client     client
	ack_client client
	doneChan   chan struct{}

	// RPS metrics
	rpl   sync.Mutex
	reqs  int64
	start time.Time

	// Errors
	errors *errorMap

	// Progress bar
	bar *pb.ProgressBar

	// Output
	out      io.Writer
	template *template.Template

	// session/queues/level stats
	deqthrottle uint64

	enqcount uint64
	enqvalid uint64

	deqbreqs uint64
	deqcount uint64
	deqvalid uint64

	ackbreqs uint64
	ackcount uint64
	ackvalid uint64

	enqCodes SafeCounter
	deqCodes SafeCounter
	ackCodes SafeCounter

	resp_cnt uint64
	resp_tot_hits uint64
	resp_tot_hits_docreads uint64
	resp_tot_bytes uint64
        resp_tot_bytesRead uint64
	resp_withhits_cnt uint64
	hits_tot_bytes uint64
	tot_kv_reads uint64
	tot_kv_bytes_read uint64
	tot_kv_read_us int64

        resp_tot_status_total uint64
        resp_tot_status_failed uint64
        resp_tot_status_successful uint64
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const TEST_HELP string = 
`
    The test setup is as follwos:

    1 All nodes are aarch64 c6g.2xlarge (3 kv, 2 index, 2 query, 2 search)

    2 There are 25 buckets ts01 to ts25 and each bucket has travel-sample loaded (multiple collections)

    3 Only travel-sample._default._default is indexed via the default FTS settings dynamic 
      (Note, dynamic indexes only recognize basic data types - that's string, numeric and boolean)

    Data used to generate the FTS search queries comes from the following sources, runs of over 100 queries all
    sabilize with the same sort of hit and miss percentages.

	# sampleReviewWords          has 18765 items from travel-sample._default._default reviews.content
	# hotelLocationLatLons       has   917 items from travel-sample._default._default geo.lat and geo.lon
	# commonReviewWords          has    50 items from https://www.researchgate.net/figure/Fifty-Most-Frequently-used-Words-in-60-648-Hotel-Review-Comments_tbl1_233475559
	# commonEnglishWords         has   100 items form https://www.espressoenglish.net/the-100-most-common-words-in-english/
	# commonVerbWords            has    34 items from https://literacyforall.org/docs/100_Most_common_in_American_English.pdf

    The following tests are supported via -L #


    1: //  0.0% misses
	repl = buildBasicRandomQueryMatch(1, conf.sampleReviewWords, conf.sampleReviewWordsLen)

        select one random word from sampleReviewWords 


    31: //  0.0% misses
	repl = buildBasicRandomQueryNumWords(1, conf.sampleReviewWords, conf.sampleReviewWordsLen)

        select one random word from sampleReviewWords 

	example: 
		"query": { "query": "+euro" } 

    32: // 90.4% misses
	repl = buildBasicRandomQueryNumWords(2, conf.sampleReviewWords, conf.sampleReviewWordsLen)

        selects two random words from sampleReviewWords 

	example: 
		"query": { "query": "+wong +skews" } 

    33: //  0.0% misses
	repl = buildBasicRandomQueryNumWords(1, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from commonReviewWords

	example: 
		"query": { "query": "+Store" } 

    34: //  1.3%  misses
	repl = buildBasicRandomQueryNumWords(2, conf.commonReviewWords, conf.commonReviewWordsLen)

        select two random words from commonReviewWords

	example: 
		"query": { "query": "+Toilet +Noisy" } 

    35: // 10.6% misses
	repl = buildBasicRandomQueryNumWords(3, conf.commonReviewWords, conf.commonReviewWordsLen)

        select three random words from commonReviewWords

	example: 
		"query": { "query": "+Downtown +Clerk +View" } 


    36: // 30.4% misses
	repl = buildBasicRandomQueryNumWords(4, conf.commonReviewWords, conf.commonReviewWordsLen)

        select four random words from commonReviewWords

	example: 
		"query": { "query": "+Bath +Dinner +Clerk +Price" }


    37: //  51.0% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 1, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select one random word from commonReviewWords

	example: 
		"query": { "query": "+absolut +Broken" }


    38: // 77.0% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 2, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select two random words from commonReviewWords

	example: 
		"query": { "query": "+abbas +Staff +Noisy" } 

    39: // 88.4% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 3, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select three random words from commonReviewWords

	example: 
		"query": { "query": "+aboard +Towel +Rate +Noise" }

    40: // 94.7% misses
	repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 4, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from sampleReviewWords AND select four random words from commonReviewWords

	example: 
		"query": { "query": "+abby +Store +Downtown +Dine +Dinner" }

    41: // 74.1% misses
	repl = buildFuzzyRandomQuery(1, 5, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from commonReviewWords with length at least 5 chars and apply fuzziness of 1
	AND then randomly replace one char [a-z] in any position of the term to search for from commonReviewWords

	example (the word was Sheet):
		"query": { "term": "Shdet", "fuzziness": 1 } 

    42: //  0.0% misses
	repl = buildFuzzyRandomQuery(2, 5, conf.commonReviewWords, conf.commonReviewWordsLen)

        select one random word from commonReviewWords with length at least 5 chars and apply fuzziness of 2
	AND then randomly replace one char [a-z] in any position of the term to search for from commonReviewWords

	example (the word was Sheet):
		"query": { "term": "Downiown", "fuzziness": 2 } 

    43: // 14.0% misses
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

	If no test is selected we randomly apply the following

	// 80% or 8 out of 10 queries are radom query terms
	repl = buildBasicRandomRandomTermsQuery(conf, c)

	// 10% or 1 out of 10 queries are simple fuzzy
	 repl = buildFuzzyRandomQuery(1, 5, conf.commonReviewWords, conf.commonReviewWordsLen)

	// 10% ro 1 out of 10 queries are 2-"conjuncts" min/max
	repl = buildPseudoGeoRandomQuery(0.25, conf, c)
`

// Inc increments the counter for the given key.
func (c *SafeCounter) Inc(key string) {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key]++
	c.mu.Unlock()
}

// Inc increments the counter for the given key.
func (c *SafeCounter) IncGet(key string) int {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v[key]++
	retval := c.v[key];
	c.mu.Unlock()
	return retval
}

// Value returns the current value of the counter for the given key.
func (c *SafeCounter) Value(key string) int {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	return c.v[key]
}

func (c *SafeCounter) Dump(what string) {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	fmt.Println(what)
	for key, element := range c.v {
		fmt.Println("\t", "Count:", element, "=>", key)
	}
}

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func newBombardier(c config) (*bombardier, error) {

	if err := c.checkArgs(); err != nil {
		return nil, err
	}
	b := new(bombardier)
	b.conf = c

	b.enqCodes = SafeCounter{v: make(map[string]int)}
	b.deqCodes = SafeCounter{v: make(map[string]int)}
	b.ackCodes = SafeCounter{v: make(map[string]int)}

	b.latencies = uhist.Default()
	b.requests = fhist.Default()
	b.reqno = 0

	if b.conf.testType() == counted {
		b.bar = pb.New64(int64(*b.conf.numReqs))
		b.bar.ShowSpeed = true
	} else if b.conf.testType() == timed {
		b.bar = pb.New64(b.conf.duration.Nanoseconds() / 1e9)
		b.bar.ShowCounters = false
		b.bar.ShowPercent = false
	}
	b.bar.ManualUpdate = true

	if b.conf.testType() == counted {
		b.barrier = newCountingCompletionBarrier(*b.conf.numReqs)
	} else {
		b.barrier = newTimedCompletionBarrier(*b.conf.duration)
	}

	if b.conf.rate != nil {
		b.ratelimiter = newBucketLimiter(*b.conf.rate)
	} else {
		b.ratelimiter = &nooplimiter{}
	}

	b.out = os.Stdout

	tlsConfig, err := generateTLSConfig(c)
	if err != nil {
		return nil, err
	}

	var (
		pbody *string
		bsp   bodyStreamProducer
	)
	if c.stream {
		if c.bodyFilePath != "" {
			bsp = func() (io.ReadCloser, error) {
				return os.Open(c.bodyFilePath)
			}
		} else {
			bsp = func() (io.ReadCloser, error) {
				return ioutil.NopCloser(
					proxyReader{strings.NewReader(c.body)},
				), nil
			}
		}
	} else {
		pbody = &c.body
		if c.bodyFilePath != "" {
			var bodyBytes []byte
			bodyBytes, err = ioutil.ReadFile(c.bodyFilePath)
			if err != nil {
				return nil, err
			}
			sbody := string(bodyBytes)
			pbody = &sbody
		}
	}

	// COUCHBASE overrid -b or -f
	if c.dynDoc {

		//fmt.Printf("\ndynDoc:        %v\n",c.dynDoc)
		//fmt.Printf("\ndynDocShow:    %v\n",c.dynDocShow)
		//fmt.Printf("\ndynDocSz:      %v\n",c.dynDocSz)
		//fmt.Printf("\ndynDocBatchSz: %v\n",c.dynDocBatchSz)

		if c.dynDocSz < 12 {
			fmt.Printf("\nERROR: --dynDocSz < 12 not supported - abort\n")
			os.Exit(exitFailure)
		} else if c.dynDocSz == 72 {
			obj := map[string]interface{}{}
			obj["a"] = "1"

			bodyBytes := []byte(`{"binfo":"0000000000@0001","user":"noequal","firstname":"couch","lastname":"base"}`)
			sbody := string(bodyBytes)
			pbody = &sbody
		} else {
			sbody := ""
			obj := map[string]interface{}{}
			for i := 0; len(sbody) < int(c.dynDocSz); i++ {
				var needch int64 = int64(int(c.dynDocSz) - len(sbody))
				var addch int64 = int64(math.Min(float64(40), float64(needch)))
				if needch < 64 {
					addch = needch
				}

				// fmt.Println("A c.dynDocSz %d, len(sbody) %d, needcsh %d, addcsh %d\n", c.dynDocSz, len(sbody), needch, addch)
				if i < 1 {
					if c.dynDocSz >= 40 {
						obj["binfo"] = fmt.Sprintf("%010d@%04d", 0, 1)
					}
				} else {
					obj["d"+strconv.FormatInt(int64(i), 10)] = RandStringBytesRmndr(int(addch))
				}

				b, _ := json.MarshalIndent(obj, "", "")
				sbody = string(b)
				toobig := len(sbody) - int(c.dynDocSz)
				if toobig > 0 {
					obj["d"+strconv.FormatInt(int64(i), 10)] = RandStringBytesRmndr(int(int(addch) - toobig))
					b, _ := json.MarshalIndent(obj, "", "")
					sbody = string(b)
					// fmt.Println("B c.dynDocSz %d, len(sbody) %d, needcsh %d, addcsh %d\n", c.dynDocSz, len(sbody), needch, addch)

				}
			}
			pbody = &sbody

		}

		if c.dynDocBatchSz > 0 {
			// we are sending batches of data
			bbody := "[\n"
			for i := 1; i <= int(c.dynDocBatchSz); i++ {
				if c.dynDocSz >= 40 {
					// doc is big enough update "bpos"
					var jsonMap map[string]interface{}
					json.Unmarshal([]byte(*pbody), &jsonMap)

					jsonMap["binfo"] = fmt.Sprintf("%010d@%04d", 0, i)
					b, _ := json.MarshalIndent(jsonMap, "", "")
					wbody := string(b)
					bbody = bbody + wbody

				} else {
					bbody = bbody + *pbody
				}
				if i == int(c.dynDocBatchSz) {
					bbody = bbody + "\n"
				} else {
					bbody = bbody + ",\n"
				}
			}
			bbody = bbody + "]"
			pbody = &bbody
		}

		if c.dynDocShow {
			fmt.Printf("\ndynDoc length (%d):\n%s\n", len(*pbody), *pbody)
		}
	}

	if b.conf.basicAuth != "" {
		sEnc := b64.StdEncoding.EncodeToString([]byte(b.conf.basicAuth))
		b.conf.headers.Set("Authorization: Basic " + sEnc)
	}

	cc := &clientOpts{
		HTTP2:             false,
		maxConns:          c.numConns,
		timeout:           c.timeout,
		tlsConfig:         tlsConfig,
		disableKeepAlives: c.disableKeepAlives,

		headers:      c.headers,
		url:          c.url,
		method:       c.method,
		body:         pbody,
		bodProd:      bsp,
		bytesRead:    &b.bytesRead,
		bytesWritten: &b.bytesWritten,
	}
	b.client = makeHTTPClient(c.clientType, cc)

	bidx := strings.LastIndex(cc.url, "/bulk")
	if bidx != -1 {
		b.conf.isBulk = true
	} else {
		b.conf.isBulk = false
	}

	bidx = strings.LastIndex(cc.url, "/deq")
	if bidx != -1 {
		b.conf.isDequeue = true
	} else {
		b.conf.isDequeue = false
	}

	bidx = strings.LastIndex(cc.url, "/enq")
	if bidx != -1 {
		b.conf.isEnqueue = true
	} else {
		b.conf.isEnqueue = false
	}

	if b.conf.customAck {
		// COUCHBASE (customAck) make another URL and fastHTTP client for sending ACK's
		tmp := ""

		// FEB-2020 we now need bucket=b01 scope=_default and they can be anywhere
		// "http://192.168.3.150:8096/api/v1/cbms/queues/b-cc-added/deq/bulk?bucket=b01&scope=_default&maxmsgs=4"

		u, err := url.Parse(string(c.url))
		if err != nil {
			fmt.Println(err)
		}
		q := u.Query()

		/*
				fmt.Println("RequestURI:",u.RequestURI())
				fmt.Println("Path:", u.Path)
				fmt.Println("RawPath:", u.RawPath)
				fmt.Println("EscapedPath:", u.EscapedPath())
				fmt.Println("  bucket=",q.Get("bucket"))
				fmt.Println("  scope=",q.Get("scope"))
			        fmt.Println("  maxmsgs=",q.Get("maxmsgs"))
		*/

		n, err := strconv.ParseInt(q.Get("maxmsgs"), 10, 64)
		if err == nil {
			b.conf.reqBatchSz = uint64(n)
		}

		isDequeue := false
		idx := strings.LastIndex(cc.url, "/deq/bulk")
		if idx != -1 {
			tmp = cc.url[0:idx] + "/ack/bulk" // + cc.url[idx+9:len(cc.url)];
			isDequeue = true
		} else {
			idx := strings.LastIndex(cc.url, "/deq")
			if idx != -1 {
				tmp = cc.url[0:idx] + "/ack"
				isDequeue = true
			}
		}

		if isDequeue == false {
			b.conf.reqBatchSz = 0
		}

		tmp = tmp + "?bucket=" + q.Get("bucket") + "&scope=" + q.Get("scope")
		//fmt.Println("HMMM",tmp)

		if len(tmp) == 0 {
			fmt.Println("COUCHBASE (customAck) failed to make ack_client based on", cc.url)
			b.conf.customAck = false
		} else {
			sav := cc.url
			cc.url = tmp
			if b.conf.debugCustomAck {
				fmt.Println("COUCHBASE (customAck) normal client URL:", sav)
				fmt.Println("COUCHBASE (customAck) then ack send URL:", cc.url)
			}
			b.ack_client = makeHTTPClient(c.clientType, cc)
			cc.url = sav
		}
	}

	if !b.conf.printProgress {
		b.bar.Output = ioutil.Discard
		b.bar.NotPrint = true
	}

	b.template, err = b.prepareTemplate()
	if err != nil {
		return nil, err
	}

	b.wg.Add(int(c.numConns))
	b.errors = newErrorMap()
	b.doneChan = make(chan struct{}, 2)
	return b, nil
}

func makeHTTPClient(clientType clientTyp, cc *clientOpts) client {
	var cl client
	switch clientType {
	case nhttp1:
		cl = newHTTPClient(cc)
	case nhttp2:
		cc.HTTP2 = true
		cl = newHTTPClient(cc)
	case fhttp:
		fallthrough
	default:
		cl = newFastHTTPClient(cc)
	}
	return cl
}

func (b *bombardier) prepareTemplate() (*template.Template, error) {
	var (
		templateBytes []byte
		err           error
	)
	switch f := b.conf.format.(type) {
	case knownFormat:
		templateBytes = f.template()
	case userDefinedTemplate:
		templateBytes, err = ioutil.ReadFile(string(f))
		if err != nil {
			return nil, err
		}
	default:
		panic("format can't be nil at this point, this is a bug")
	}
	outputTemplate, err := template.New("output-template").
		Funcs(template.FuncMap{
			"WithLatencies": func() bool {
				return b.conf.printLatencies
			},
			"FormatBinary": formatBinary,
			"FormatTimeUs": formatTimeUs,
			"FormatTimeUsUint64": func(us uint64) string {
				return formatTimeUs(float64(us))
			},
			"FloatsToArray": func(ps ...float64) []float64 {
				return ps
			},
			"Multiply": func(num, coeff float64) float64 {
				return num * coeff
			},
			"StringToBytes": func(s string) []byte {
				return []byte(s)
			},
			"UUIDV1": uuid.NewV1,
			"UUIDV2": uuid.NewV2,
			"UUIDV3": uuid.NewV3,
			"UUIDV4": uuid.NewV4,
			"UUIDV5": uuid.NewV5,
		}).Parse(string(templateBytes))

	if err != nil {
		return nil, err
	}
	return outputTemplate, nil
}

func (b *bombardier) writeStatistics(
	code int, usTaken uint64,
) {
	b.latencies.Increment(usTaken)
	b.rpl.Lock()
	b.reqs++
	b.rpl.Unlock()
	var counter *uint64
	switch code / 100 {
	case 1:
		counter = &b.req1xx
	case 2:
		counter = &b.req2xx
	case 3:
		counter = &b.req3xx
	case 4:
		counter = &b.req4xx
	case 5:
		counter = &b.req5xx
	default:
		counter = &b.others
	}
	atomic.AddUint64(counter, 1)
}

func (b *bombardier) performSingleRequest() {

	// fmt.Println(b.client)
	// fmt.Println(b.conf.customAck)

	// JAS need to track batches for pretty data
	var preqno uint64 = 0

	if b.conf.dynDoc && b.conf.dynDocSz >= 40 {
		// only do this fo larger synamic documents
		preqno = atomic.AddUint64(&b.reqno, 1)
		//fmt.Printf("inc %d",preqno)
	}
	// fmt.Println("preqno",preqno,"b.conf.dynDoc",b.conf.dynDoc,"b.conf.dynDocSz",b.conf.dynDocSz);

	code, usTaken, ackBody, numToAck, err := b.client.do(b, preqno, b.conf, "", 0)
	if err != nil {
		b.errors.add(err)
	}
	b.writeStatistics(code, usTaken)
	if b.conf.customAck == false {
		// this is NEVER a dequeue ACK
		return
	}
	if ackBody != nil && numToAck > 0 {
		// fmt.Println(b.ack_client)
		if b.conf.debugCustomAck || (uint64(numToAck) != b.conf.reqBatchSz) {
			// fmt.Println("COUCHBASE (customAck num "+strconv.Itoa(numToAck)+") sending a customAck based on prior HTTP couchbase /deq or /deq/bulk, size req was ", b.conf.reqBatchSz)
			// fmt.Println("COUCHBASE (customAck num "+strconv.Itoa(numToAck)+") with body\n" + string(ackBody) + "\n")
		}
		code, _, _, _, err := b.ack_client.do(b, preqno, b.conf, string(ackBody), numToAck)
		if err != nil {
			fmt.Println("COUCHBASE (customAck) failed in send of customAck based on prior HTTP couchbase /deq or /deq/bulk", code)
		} else {
			if b.conf.debugCustomAck {
				fmt.Println("COUCHBASE (customAck) success in send of customAck based on prior HTTP couchbase /deq or /deq/bulk", code)
			}
		}
	} else {
		//if b.conf.debugCustomAck || (uint64(numToAck) != b.conf.reqBatchSz) {
		//    fmt.Println("COUCHBASE (customAck num "+strconv.Itoa(numToAck)+") NOT sending a customAck /deq or /deq/bulk returned no data, size req was ",b.conf.reqBatchSz)
		//}
	}
}

func (b *bombardier) worker() {
	done := b.barrier.done()
	for b.barrier.tryGrabWork() {
		if b.ratelimiter.pace(done) == brk {
			break
		}
		b.performSingleRequest()
		b.barrier.jobDone()
	}
}

func (b *bombardier) barUpdater() {
	done := b.barrier.done()
	for {
		select {
		case <-done:
			b.bar.Set64(b.bar.Total)
			b.bar.Update()
			b.bar.Finish()
			if b.conf.printProgress {
				fmt.Fprintln(b.out, "Done!")
			}
			b.doneChan <- struct{}{}
			return
		default:
			current := int64(b.barrier.completed() * float64(b.bar.Total))
			b.bar.Set64(current)
			b.bar.Update()
			time.Sleep(b.bar.RefreshRate)
		}
	}
}

func (b *bombardier) rateMeter() {
	requestsInterval := 10 * time.Millisecond
	if b.conf.rate != nil {
		requestsInterval, _ = estimate(*b.conf.rate, rateLimitInterval)
	}
	requestsInterval += 10 * time.Millisecond
	ticker := time.NewTicker(requestsInterval)
	defer ticker.Stop()
	done := b.barrier.done()
	for {
		select {
		case <-ticker.C:
			b.recordRps()
			continue
		case <-done:
			b.wg.Wait()
			b.recordRps()
			b.doneChan <- struct{}{}
			return
		}
	}
}

func (b *bombardier) recordRps() {
	b.rpl.Lock()
	duration := time.Since(b.start)
	reqs := b.reqs
	b.reqs = 0
	b.start = time.Now()
	b.rpl.Unlock()

	reqsf := float64(reqs) / duration.Seconds()
	b.requests.Increment(reqsf)
}

func (b *bombardier) bombard() {
	if b.conf.printIntro {
		b.printIntro()
	}
	b.bar.Start()
	bombardmentBegin := time.Now()
	b.start = time.Now()
	for i := uint64(0); i < b.conf.numConns; i++ {
		go func() {
			defer b.wg.Done()
			b.worker()
		}()
	}
	go b.rateMeter()
	go b.barUpdater()
	b.wg.Wait()
	b.timeTaken = time.Since(bombardmentBegin)
	<-b.doneChan
	<-b.doneChan
}

func (b *bombardier) printIntro() {
	if b.conf.testType() == counted {
		fmt.Fprintf(b.out,
			"Bombarding %v with %v request(s) using %v connection(s)\n",
			b.conf.url, *b.conf.numReqs, b.conf.numConns)
	} else if b.conf.testType() == timed {
		fmt.Fprintf(b.out, "Bombarding %v for %v using %v connection(s)\n",
			b.conf.url, *b.conf.duration, b.conf.numConns)
	}
}

func (b *bombardier) gatherInfo() internal.TestInfo {
	info := internal.TestInfo{
		Spec: internal.Spec{
			NumberOfConnections: b.conf.numConns,

			Method: b.conf.method,
			URL:    b.conf.url,

			Body:         b.conf.body,
			BodyFilePath: b.conf.bodyFilePath,

			CertPath: b.conf.certPath,
			KeyPath:  b.conf.keyPath,

			Stream:     b.conf.stream,
			Timeout:    b.conf.timeout,
			ClientType: internal.ClientType(b.conf.clientType),

			Rate: b.conf.rate,
		},
		Result: internal.Results{
			BytesRead:    b.bytesRead,
			BytesWritten: b.bytesWritten,
			TimeTaken:    b.timeTaken,

			EnqCount: b.enqcount,
			EnqValid: b.enqvalid,

			DeqBreqs: b.deqbreqs,
			DeqCount: b.deqcount,
			DeqValid: b.deqvalid,

			AckBreqs: b.ackbreqs,
			AckCount: b.ackcount,
			AckValid: b.ackvalid,

			DeqThrottle: b.deqthrottle,

			Req1XX: b.req1xx,
			Req2XX: b.req2xx,
			Req3XX: b.req3xx,
			Req4XX: b.req4xx,
			Req5XX: b.req5xx,
			Others: b.others,

			Latencies: b.latencies,
			Requests:  b.requests,
		},
	}

	testType := b.conf.testType()
	info.Spec.TestType = internal.TestType(testType)
	if testType == timed {
		info.Spec.TestDuration = *b.conf.duration
	} else if testType == counted {
		info.Spec.NumberOfRequests = *b.conf.numReqs
	}

	if b.conf.headers != nil {
		for _, h := range *b.conf.headers {
			info.Spec.Headers = append(info.Spec.Headers,
				internal.Header{
					Key:   h.key,
					Value: h.value,
				})
		}
	}

	for _, ewc := range b.errors.byFrequency() {
		info.Result.Errors = append(info.Result.Errors,
			internal.ErrorWithCount{
				Error: ewc.error,
				Count: ewc.count,
			})
	}

	return info
}

func (b *bombardier) printStats() {
	info := b.gatherInfo()
	err := b.template.Execute(b.out, info)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func (b *bombardier) redirectOutputTo(out io.Writer) {
	b.bar.Output = out
	b.out = out
}

func (b *bombardier) disableOutput() {
	b.redirectOutputTo(ioutil.Discard)
	b.bar.NotPrint = true
}

func LinesFromReader(r io.Reader) ([]string, error) {
        var lines []string
        scanner := bufio.NewScanner(r)
        for scanner.Scan() {
                lines = append(lines, scanner.Text())
        }
        if err := scanner.Err(); err != nil {
                return nil, err
        }

        return lines, nil
}

func getLinesFromUrl( username string, passwd string, host string) ([]string, error) {
    client := &http.Client{}
    req, err := http.NewRequest("GET", "http://" + host + ":8094/_metering", nil)
    req.SetBasicAuth(username, passwd)
    resp, err := client.Do(req)
    if err != nil{
        log.Fatal(err)
    }
    defer resp.Body.Close()
    return LinesFromReader(resp.Body)
}

func dumpMetering( username string, passwd string, host string, altMeteringHost string, tag string) (*big.Int) {

	total := big.NewInt(0)

        lines, err := getLinesFromUrl(username,passwd,host)
        if err != nil {
                log.Fatal(err)
        }

        lh1 := len(lines)
	if len(altMeteringHost) > 0 {
		lines2, err2 := getLinesFromUrl(username,passwd,altMeteringHost)
		if err2 != nil {
			log.Fatal(err)
		}

		for _, x := range lines2 {
			lines = append(lines, x)
		}
	}

        cnt := 0;
        for _, line := range lines {
		cnt++
                // 2023/04/20 11:57:36 meter_ru_total{bucket="ts01",for="fts"} 9.34389e+07
                matched, _ := regexp.MatchString(`meter_ru_total\{.*for="fts"`, line)
                if matched {

			scinum := strings.Split(line, " ")


			flt, _, err := big.ParseFloat(scinum[len(scinum)-1], 10, 0, big.ToNearestEven)
			if err != nil {
			    /* handle a parsing error here */
			}
			var i = new(big.Int)
			i, acc := flt.Int(i)

			total.Add(total,i);

			sn := 1
			src := host
			if (cnt > lh1) {
			    src = altMeteringHost
			    sn = 2
			} 
                        log.Printf("%s -or- %d %v (src %d %s)\n", line, i, acc, sn, src)
                }
        }
        log.Printf("TOTAL RU %-10s = %12d\n", tag, total)
	return total
}

func main() {

	arg_len:= len(os.Args[1:])
	for i := 0; i < arg_len; i++ {
		if os.Args[i+1] == "-J" || os.Args[i+1] == "--ftsTestHelp" {
			fmt.Println(TEST_HELP);
			os.Exit(0)
		}
	}

	cfg, err := parser.parse(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(exitFailure)
	}

        up := strings.Split(cfg.basicAuth, ":")
	url, _ := url.Parse(cfg.url)
	host := url.Hostname()

	begRU := big.NewInt(0)
	if cfg.showMetering {
		fmt.Printf("\n")
		begRU = dumpMetering(up[0],up[1],host,cfg.altMeteringHost,"(begRU)")
		fmt.Printf("\n")
	}


if cfg.dynFtsShow {
	fmt.Printf("# %-20s has %9d items\n","sampleReviewWords",cfg.sampleReviewWordsLen);
	fmt.Printf("# %-20s has %9d items\n","commonReviewWords",cfg.commonReviewWordsLen);
	fmt.Printf("# %-20s has %9d items\n","commonEnglishWords",cfg.commonEnglishWordsLen);
	fmt.Printf("# %-20s has %9d items\n","commonVerbWords",cfg.commonVerbWordsLen);
	fmt.Printf("# %-20s has %9d items\n","hotelLocationLatLons",cfg.hotelLocationLatLonsLen);
}

        start := time.Now()

        // fmt.Println(elapsed.Microseconds(),"uS")


	bombardier, err := newBombardier(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(exitFailure)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		bombardier.barrier.cancel()
	}()
	bombardier.bombard()
	if bombardier.conf.printResult {
		bombardier.printStats()
	}

	// fmt.Printf("%v",bombardier.deqCodes);

        elapsed := time.Since(start)
        elapsedUs := elapsed.Microseconds()
        elapsedSec := elapsed.Seconds()

	fmt.Printf("  HTTP 429 retries %12d, pct. %9.3f%% across %d reqs\n",bombardier.retryReq429, float64(bombardier.retryReq429)/float64(bombardier.resp_cnt)*100, bombardier.resp_cnt)


	fmt.Println("  Ave Sizes");
	if bombardier.resp_cnt != 0 {
		fmt.Printf("    resp.body:       %9d bytes (across %9d reqs)\n", bombardier.resp_tot_bytes / bombardier.resp_cnt, bombardier.resp_cnt)
	} else {
		fmt.Printf("    resp.body:       %9s bytes (across %9d reqs)\n", "n/a", bombardier.resp_cnt)
        }
	if bombardier.resp_withhits_cnt !=0 {
		fmt.Printf("    resp.body.hit[]: %9d bytes (across %9d reqs having hits [%4.2f%%])\n", bombardier.hits_tot_bytes / bombardier.resp_withhits_cnt, 
			bombardier.resp_withhits_cnt,
			float64(bombardier.resp_withhits_cnt)/float64(bombardier.resp_cnt) * 100)
	} else {
		fmt.Printf("    resp.body.hit[]: %9s bytes (across %9d reqs having hits [%4.2f%%])\n", "n/a", bombardier.resp_withhits_cnt, 
			float64(bombardier.resp_withhits_cnt)/float64(bombardier.resp_cnt) * 100)
	}

	fmt.Println("  Other");
	fmt.Printf("    total_hits        %12d, ave %9.3f\n",bombardier.resp_tot_hits, float64(bombardier.resp_tot_hits)/float64(bombardier.resp_cnt))
	fmt.Printf("    tot_hits_docreads %12d, ave %9.3f\n",bombardier.resp_tot_hits_docreads, float64(bombardier.resp_tot_hits_docreads)/float64(bombardier.resp_cnt))
	fmt.Printf("    status.total      %12d\n",bombardier.resp_tot_status_total)
	fmt.Printf("    status.failed     %12d\n",bombardier.resp_tot_status_failed)
	fmt.Printf("    status.successful %12d\n",bombardier.resp_tot_status_successful)
	fmt.Printf("    Test Time in Sec. %12.3f\n",float64(elapsedUs/1000.0/1000.0))
	fmt.Printf("    Server side *RU* info\n")
	fmt.Printf("      bytesRead       %12d\n", bombardier.resp_tot_bytesRead)
	fmt.Printf("      bytesRead/Resp  %12.3f\n", float64(bombardier.resp_tot_bytesRead)/float64(bombardier.resp_cnt))
	fmt.Printf("      bytesRead/Succ  %12.3f\n", float64(bombardier.resp_tot_bytesRead)/float64(bombardier.resp_tot_status_successful))
	fmt.Printf("      bytesRead/Sec   %12.3f (MbytesRead/Sec %9.3f)\n", 
		float64(bombardier.resp_tot_bytesRead)/float64(elapsedUs/1000.0/1000.0), 
		float64(bombardier.resp_tot_bytesRead)/float64(elapsedUs/1000.0/1000.0)/1000.0/1000.0)

        if len(cfg.kvDocLookups) > 0 {
	    //fmt.Printf("Arg -K 'host' ... read the above tot_hits_docreads directly from KV .. need stats ...")
	    fmt.Println("  KV reads (due to -K)");
	    fmt.Printf("    tot_kv_reads        %12d\n",bombardier.tot_kv_reads)
	    fmt.Printf("    tot_kv_bytes_read   %12d, ave %9.3f\n",bombardier.tot_kv_bytes_read, float64(bombardier.tot_kv_bytes_read)/float64(bombardier.tot_kv_reads))
	    fmt.Printf("    WARNING: This progam with -K is limited only reads 20,863 (21K) docs/sec. per second,\n") 
	    fmt.Printf("             but pillowfight 135,917 (135K) docs/sec. from an FTS node in the cluster\n")
	    fmt.Printf("    elapsed %12d uS. (%6.2f sec) KV %9.3f reads/sec\n",elapsedUs, float64(elapsedSec), float64(bombardier.tot_kv_reads)/float64(elapsedUs/1000/1000))
	}
	fmt.Println();

	if cfg.showMetering {
	    endRU := dumpMetering(up[0],up[1],host,cfg.altMeteringHost,"(endRU)")

	    deltaRU := big.NewInt(0).Sub(endRU, begRU)
	    zero := big.NewInt(0)

	    if deltaRU.Cmp(zero) != 0 {
		deltaRUfloat := new(big.Float).SetInt(deltaRU)

		floatDiv := big.NewFloat(float64(elapsedUs/1000.0/1000.0))
		ru_per_sec := new(big.Float).Quo(deltaRUfloat, floatDiv)
		byte_per_ru := new(big.Float).Quo(big.NewFloat(float64(bombardier.resp_tot_bytesRead)/float64(elapsedUs/1000.0/1000.0)), ru_per_sec)

		log.Printf("DELTA endRU - begRU = %12d\n", deltaRU)
		log.Printf("RUs/sec.            = %12.3f\n", ru_per_sec)
		log.Printf("bytes/RU            = %12.3f\n", byte_per_ru)

		numbkts := big.NewInt(1)
		if cfg.lenBucketSeq > 0 {
		    numbkts = big.NewInt(int64(cfg.endBucketSeq - cfg.begBucketSeq + 1))
		}
		nbfloat := new(big.Float).SetInt(numbkts)
		log.Printf("numbkts             = %12d\n", numbkts)

		ru_per_sec_per_bkt := new(big.Float).Quo(ru_per_sec, nbfloat) 
		log.Printf("RUs/sec./bkt.       = %12.3f\n", ru_per_sec_per_bkt)
            }
	}



/*
        fmt.Printf("resp_cnt=%d, resp_tot_bytes=%d\n",bombardier.resp_cnt, bombardier.resp_tot_bytes)
        fmt.Printf("resp_withhits_cnt=%d, hits_tot_bytes=%d\n",bombardier.resp_withhits_cnt,bombardier.hits_tot_bytes)

	bombardier.enqCodes.Dump("  Enq responses:")
	bombardier.deqCodes.Dump("  Deq responses:")
	bombardier.ackCodes.Dump("  Ack responses:")
*/
}
