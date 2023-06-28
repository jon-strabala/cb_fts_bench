package main

import (
	"crypto/tls"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
	"strconv"

	"github.com/jon-strabala/fasthttp"
	"golang.org/x/net/http2"

	"bytes"
	"fmt"
	// "os"
	"encoding/json"
	"math/rand"

        "github.com/tidwall/gjson"

	"log"
	"os"
        "github.com/couchbase/gocb/v2"

)

type client interface {
	do(b *bombardier, preqno uint64, conf config, altbody string, acksToSend int) (code int, usTaken uint64, ackBody []byte, numToAck int, err error)
}

type bodyStreamProducer func() (io.ReadCloser, error)

type clientOpts struct {
	HTTP2 bool

	maxConns          uint64
	timeout           time.Duration
	tlsConfig         *tls.Config
	disableKeepAlives bool

	headers     *headersList
	url, method string

	body    *string
	bodProd bodyStreamProducer

	bytesRead, bytesWritten *int64
}

type fasthttpClient struct {
	client *fasthttp.HostClient

	headers                  *fasthttp.RequestHeader
	host, requestURI, method string

	body    *string
	bodProd bodyStreamProducer
}

// ========= JAS =========

// use short struct(s) to remove status stuff for return ack

type CbFtsRespShort struct {
	Status struct {
		Total      int `json:"total"`
		Failed     int `json:"failed"`
		Successful int `json:"successful"`
	} `json:"status"`
	TotalHits      int    `json:"total_hits"`
	BytesRead      int    `json:"bytesRead"`
	Hits[] struct {
		Id      string `json:"id"`
	} `json:"hits"`
}

type CbQueueOneRespShort struct {
	Status struct {
		Type      int    `json:"type"`
		Code      int    `json:"code"`
		Name      string `json:"name"`
		Desc      string `json:"desc"`
		ErrorInfo string `json:"error_info"`
	} `json:"status"`
	Envelope struct {
		MsgID string `json:"msg_id"`
	} `json:"envelope"`
}

type CbQueueBatchRespShort struct {
	Responses []struct {
		Status struct {
			Type      int    `json:"type"`
			Code      int    `json:"code"`
			Name      string `json:"name"`
			Desc      string `json:"desc"`
			ErrorInfo string `json:"error_info"`
		} `json:"status"`
		Envelope struct {
			MsgID string `json:"msg_id"`
		} `json:"envelope"`
	} `json:"responses"`
}

const fts_query_pat string = "__FTS_QUERY__"


/*
// HasPrefix tests whether the byte slice s begins with prefix.
func HasPrefix(s, prefix []byte) bool {
    return len(s) >= len(prefix) && bytes.Equal(s[0:len(prefix)], prefix)
}
*/

// PrettyPrint to print struct in a readable way
func PrettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

// PrettyPrint to print struct in a readable way
func PrettyPrintOneLine(i interface{}) string {
	s, _ := json.Marshal(i)
	return string(s)
}

func formatJSON(data []byte) ([]byte, error) {
	var out bytes.Buffer
	err := json.Indent(&out, data, "", "    ")
	if err == nil {
		return out.Bytes(), err
	}
	return data, nil
}

// =======================

func replaceAtIndex(in string, r rune, i int) string {
    out := []rune(in)
    out[i] = r
    return string(out)
}

func randIntFromRange(min int, max int) int {
    return rand.Intn(max - min + 1) + min
}

func buildBasicRandomQueryMatch(numterms int, words []string, wordsLen int) string {
	var num int
	repl := "";

	for i := 1; i <= numterms; i++ {
	    num = randIntFromRange(0,wordsLen -1);
            if i == 1 {
	        repl = words[num];
            } else {
	        repl = repl + words[num];
	    }
	}

	// picks up only 'hotel' (not 'landmark')
	// repl = "\"query\": {  \"match\": \"balcony\",  \"field\": \"reviews.content\" }"

	repl = "\"query\": {  \"match\": \""+ repl + "\",  \"field\": \"reviews.content\" }"

	return repl
} 

func buildBasicRandomQueryNumWords(numterms int, words []string, wordsLen int) string {
	var num int
	repl := "";

	for i := 1; i <= numterms; i++ {
	    num = randIntFromRange(0,wordsLen -1);
            if i == 1 {
	        repl = "+" + words[num];
            } else {
	        repl = repl + " +" + words[num];
	    }
	}

	// picks up both 'hotel' and 'landmark'
	repl = "\"query\": { \"query\": \""+ repl + "\" }"


	return repl
} 

func buildBasicRandomQueryTwoNumWords(numterms int, words []string, wordsLen int, numterms2 int, words2 []string, words2Len int) string {
	var num int
	repl := "";

	for i := 1; i <= numterms; i++ {
	    num = randIntFromRange(0,wordsLen -1);
            if i == 1 {
	        repl = "+" + words[num];
            } else {
	        repl = repl + " +" + words[num];
	    }
	}

	for i := 1; i <= numterms2; i++ {
	    num = randIntFromRange(0,words2Len -1);
	    repl = repl + " +" + words2[num];
	}

	repl = "\"query\": { \"query\": \""+ repl + "\" }"

	return repl
} 

func buildFuzzyRandomQuery(fuzziness int, termminlen int, words []string, wordsLen int) string {
	var num int
	var word1 string;
	repl := "";

	for {
		num = randIntFromRange(0,wordsLen -1);
		word1 = words[num];
		if len(word1) >= termminlen {
		    break;
		}
	}

	mychar := string(randIntFromRange(97,122))
	// fmt.Printf("A mychar %s\n",mychar)
        mypos := randIntFromRange(0,len(word1)-1)
	// fmt.Printf("A mychar %s mypos %d word1[%d] <%s> len(word1)=%d\n",mychar,mypos,mypos,word1, len(word1));

        s := ""
        for i := 0; i < len(word1); i++ {
	    if i != mypos {
	       s = s + string(word1[i])
	    } else {
	       s = s + mychar
    	    }
        }
        word1 = s

	// fmt.Printf("B mychar %s mypos %d word1[%d] <%s>\n",mychar,mypos,mypos,word1);

	repl = fmt.Sprintf("\"query\": { \"term\": \"%s\", \"fuzziness\": %d }",word1, fuzziness)
	// fmt.Println(repl);

	//aaa := "\"query\": { \"term\": \"Breakfasz\", \"fuzziness\": 2 }"
	//fmt.Println("OK %s",aaa);
	//fmt.Println("?? %s",repl);

	// repl = "\"query\": { \"term\": \"Breakfasz\", \"fuzziness\": 2 }"
	return repl
} 


func buildBasicRandomRandomTermsQuery(conf config, c *fasthttpClient) string {
	var num int
	var word1 string;
	var word1a string;
	var word2 string;
	var word3 string;
	var word4 string;
	var repl string;

	// num = (rand.Intn(conf.commonReviewWordsLen - 1) + 1)
	num = randIntFromRange(0,conf.commonReviewWordsLen -1);
	word1 = "+" + conf.commonReviewWords[num];
	// num = (rand.Intn(conf.commonReviewWordsLen - 1) + 1)
	num = randIntFromRange(0,conf.commonReviewWordsLen -1);
	word1a = "+" + conf.commonReviewWords[num];
	repl = word1 + " " + word1a

	if randIntFromRange(1,10) < 6 {
		// num = (rand.Intn(conf.commonReviewWordsLen - 1) + 1)
		num = randIntFromRange(0,conf.commonReviewWordsLen -1);
		word2 = "+" + conf.commonReviewWords[num];
		repl = repl + " " + word2
	}

	if randIntFromRange(1,10) < 6 {
		// num = (rand.Intn(conf.commonEnglishWordsLen - 1) + 1)
		num = randIntFromRange(0,conf.commonEnglishWordsLen - 1);
		word3 = "+" + conf.commonEnglishWords[num];
		repl = repl + " " + word3
	}

	if randIntFromRange(1,10) < 6 {
		// num = (rand.Intn(conf.commonVerbWordsLen - 1) + 1)
		num = randIntFromRange(0,conf.commonVerbWordsLen - 1);
		word4 = "+" + conf.commonVerbWords[num];
		repl = repl + " " + word4
	}
	repl = "\"query\": { \"query\": \""+ repl + "\" }"

	return repl
} 

func buildPseudoGeoRandomQuery(scale float32, conf config, c *fasthttpClient) string {
	repl := "";

	num := randIntFromRange(0,conf.hotelLocationLatLonsLen -1);
        center := conf.hotelLocationLatLons[num]
	lat := center[1]
	lon := center[0]

	delta_lat := float32(randIntFromRange(0,1000))/float32(10000) - 0.05 * scale;
	delta_lon := float32(randIntFromRange(0,1000))/float32(10000) - 0.05;

	new_lat := lat + delta_lat
	new_lon := lon + delta_lon

	// fmt.Println(lat,lon);
	// fmt.Println(new_lat,new_lon);

	lat_min := new_lat - float32(randIntFromRange(1,15))/30.0 * scale
	lat_max := new_lat + float32(randIntFromRange(1,15))/30.0 * scale

	lon_min := new_lon - float32(randIntFromRange(1,15))/30.0 * scale
	lon_max := new_lon + float32(randIntFromRange(1,15))/30.0 * scale

	lat_min_str := fmt.Sprintf("%f", lat_min)
	lat_max_str := fmt.Sprintf("%f", lat_max)
	lon_min_str := fmt.Sprintf("%f", lon_min)
	lon_max_str := fmt.Sprintf("%f", lon_max)

    
	// aaa := "\"query\": {\"conjuncts\":[ {  \"min\": -94, \"max\": -93,  \"inclusive_min\": false,  \"inclusive_max\": false,  \"field\": \"geo.lon\" },{  \"min\": 45, \"max\": 46,  \"inclusive_min\": false,  \"inclusive_max\": false,  \"field\": \"geo.lat\" }]} "

	// repl = aaa

	repl = "\"query\": {\"conjuncts\":[ {  \"min\": " +
		lon_min_str +", \"max\": " + 
		lon_max_str +",  \"inclusive_min\": false,  \"inclusive_max\": false,  \"field\": \"geo.lon\" },{  \"min\": " +
		lat_min_str +", \"max\": " +
		lat_max_str +",  \"inclusive_min\": false,  \"inclusive_max\": false,  \"field\": \"geo.lat\" }]} "

	return repl
}

func newFastHTTPClient(opts *clientOpts) client {
	c := new(fasthttpClient)
	u, err := url.Parse(opts.url)
	if err != nil {
		// opts.url guaranteed to be valid at this point
		panic(err)
	}
	c.host = u.Host
	c.requestURI = u.RequestURI()
	c.client = &fasthttp.HostClient{
		Addr:                          u.Host,
		IsTLS:                         u.Scheme == "https",
		MaxConns:                      int(opts.maxConns),
		ReadTimeout:                   opts.timeout,
		WriteTimeout:                  opts.timeout,
		DisableHeaderNamesNormalizing: true,
		TLSConfig:                     opts.tlsConfig,
		Dial: fasthttpDialFunc(
			opts.bytesRead, opts.bytesWritten,
		),
	}
	c.headers = headersToFastHTTPHeaders(opts.headers)
	c.method, c.body = opts.method, opts.body
	c.bodProd = opts.bodProd
	return client(c)
}

func (c *fasthttpClient) do(b *bombardier, preqno uint64, conf config, altbody string, acksToSend int) (
	code int, usTaken uint64, ackBody []byte, numToAck int, err error,
) {
	retries := 0
	var uSdelay uint64 = 500 

	if conf.minBackoff > 0 {
		uSdelay = uint64(conf.minBackoff * 1000);
	}


	REDO_LOOP:
	if conf.customAck && len(altbody) > 0 {
		if altbody == "_SKIP_ONE_" {
			// there was no data so we don't need to ACK
			tag := "ACK "
			if conf.trace {
				fmt.Printf("%s skipping no data in prior fetch\n", tag)
			}
			start := time.Now()
			usTaken = uint64(time.Since(start).Nanoseconds() / 1000)
			return
		}
	}

	var repl string

	// prepare the request
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	if c.headers != nil {
		c.headers.CopyTo(&req.Header)
	}
	if len(req.Header.Host()) == 0 {
		req.Header.SetHost(c.host)
	}
	req.Header.SetMethod(c.method)
	if c.client.IsTLS {
		req.URI().SetScheme("https")
	} else {
		req.URI().SetScheme("http")
	}
/* FTS SUBS HERE "[[SEQ:#:##]] 
        conf.begBucketSeq int
        conf.endBucketSeq int
        conf.lenBucketSeq int // if ZERO we didn't have [[SEQ:#:##]] in the URL
	conf.patBucketSeq     // what we need to substitute for
*/

        var newuri string = ""
        var bktstr string = ""
        var bktseq int = 0
	if conf.lenBucketSeq > 0 {
/*
	    for nn := 0; nn < 100; nn++ {
		tmp := randIntFromRange(conf.begBucketSeq,conf.endBucketSeq);
		fmt.Println(tmp)
	    }
	    os.Exit(exitFailure)
*/
	    num := randIntFromRange(conf.begBucketSeq,conf.endBucketSeq);
            strnum := strconv.Itoa(num)
	    if len(strnum) != conf.lenBucketSeq {
		strnum = "0" + strnum
            }
	    newuri = strings.ReplaceAll(c.requestURI, conf.patBucketSeq, strnum)
	    bktstr = strings.ReplaceAll(conf.kvBucket, conf.patBucketSeq, strnum)
	    bktseq = num
// fmt.Printf("AAA %s b %d e %d len %d strnum %s <<%s>> bucket <<%s>>\n",conf.patBucketSeq,conf.begBucketSeq,conf.endBucketSeq,conf.lenBucketSeq, strnum, newuri, bktstr);

	}
	if len(newuri) > 0 {
		// fmt.Printf("newuri %s\n", newuri)
		req.SetRequestURI(newuri)
	} else {
		// fmt.Printf("olduri %s\n", c.requestURI)
		req.SetRequestURI(c.requestURI)
	}


	doSend := false
	tag := ""
	if conf.customAck && len(altbody) > 0 {
		// This is pass two (2) use the ACK body
		req.SetBodyString(altbody)
		tag = "ACK "
	} else if c.body != nil {
		doSend = true
		// This is pass one
		var newbody string = ""

		// JAS need to trakc batches and update our body
		if conf.nobatchnum == false && len(*c.body) > 0 && preqno > 0 {
			startpos := strings.Index(*c.body, "\"binfo\": \"")
			if startpos >= 0 {
				// pfxstr := (*c.body)[0:startpos+10];
				// oldstr := (*c.body)[startpos+10:startpos+20];
				// newstr := fmt.Sprintf("%010d", preqno)
				//sfxstr := (*c.body)[startpos+20:len(*c.body)];
				// newbody = pfxstr+newstr+sfxstr;

				oldstr := (*c.body)[startpos+10 : startpos+20]
				newstr := fmt.Sprintf("%010d", preqno)
				newbody = strings.ReplaceAll(*c.body, oldstr, newstr)

				// fmt.Fprintf(os.Stderr, "preqno %d: %s\n", preqno,newbody)
			}
		}


/* FTS SUBS HERE "__FTS_QUERY__" */
		if strings.Index(*c.body, fts_query_pat) != -1 {



			if conf.dynFtsLimit >0 {
				switch conf.dynFtsLimit {

				    case  1: //  0.0% misses
					repl = buildBasicRandomQueryMatch(1, conf.sampleReviewWords, conf.sampleReviewWordsLen)


				    case 31: //  0.0% misses
					repl = buildBasicRandomQueryNumWords(1, conf.sampleReviewWords, conf.sampleReviewWordsLen)
				    case 32: // 90.4% misses
					repl = buildBasicRandomQueryNumWords(2, conf.sampleReviewWords, conf.sampleReviewWordsLen)


				    case 33: //  0.0% misses
					repl = buildBasicRandomQueryNumWords(1, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 34: //  1.3%  misses
					repl = buildBasicRandomQueryNumWords(2, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 35: // 10.6% misses
					repl = buildBasicRandomQueryNumWords(3, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 36: // 30.4% misses
					repl = buildBasicRandomQueryNumWords(4, conf.commonReviewWords, conf.commonReviewWordsLen)


				    case 37: // 51.0% misses
					repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 1, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 38: // 77.0% misses
					repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 2, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 39: // 88.4% misses
					repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 3, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 40: // 94.7% misses
					repl = buildBasicRandomQueryTwoNumWords(1, conf.sampleReviewWords, conf.commonReviewWordsLen, 4, conf.commonReviewWords, conf.commonReviewWordsLen)


				    case 41: // 74.1% misses
					repl = buildFuzzyRandomQuery(1 /*"fuzziness*/,5 /*termminlen*/, conf.commonReviewWords, conf.commonReviewWordsLen)
				    case 42: //  0.0% misses
					repl = buildFuzzyRandomQuery(2 /*"fuzziness*/,5 /*termminlen*/, conf.commonReviewWords, conf.commonReviewWordsLen)


				    case 43: // 14.0% misses
					repl = buildPseudoGeoRandomQuery(0.25, conf, c) // 2 range conjuncts

				    case 99: //  9.9% misses
					repl = buildBasicRandomRandomTermsQuery(conf, c)
				}

			} else {
				num := randIntFromRange(1,10)

				if num == 10 {
					// 1 out of 10 queries are 2-"conjuncts" min/max
					repl = buildPseudoGeoRandomQuery(0.25, conf, c)
				} else
				if num == 9 {
					// 1 out of 10 queries are simple fuzzy
					 repl = buildFuzzyRandomQuery(1 /*"fuzziness*/,5 /*termminlen*/, conf.commonReviewWords, conf.commonReviewWordsLen)
				} else {
					// 8 out of 10 queries are radom query terms
					repl = buildBasicRandomRandomTermsQuery(conf, c)
				}
			}


			//newbody = strings.ReplaceAll(*c.body, fts_query_pat, "+very +nice +food +part")
			newbody = strings.ReplaceAll(*c.body, fts_query_pat, repl)
		}

		if len(newbody) > 0 {
			req.SetBodyString(newbody)
		} else {
			req.SetBodyString(*c.body)
		}
	} else {
		bs, bserr := c.bodProd()
		if bserr != nil {
			return 0, 0, nil, 0, bserr
		}
		req.SetBodyStream(bs, -1)
	}

	// fire the request
	if conf.trace {
		// fmt.Printf("REQ\n%v\n",string(req.Body()));
		str, _ := formatJSON(req.Body())
		fmt.Printf("%sREQ %d\n%s\n", tag, preqno, str)
	}
	start := time.Now()
// fmt.Println("000",req)
	err = c.client.Do(req, resp)
	if err != nil {
		code = -1
		if conf.dynFtsShow {
			fmt.Println(repl,"nohitserr?")
		}
	} else {
		code = resp.StatusCode()
		if code != 200 {
		    if code != 429 {
		        fmt.Printf("HTTP status %d\n",code)
		    } else {
			// HTTP 429 here what should we do
			// a) back off just the thread (but we randomly go to N buckets)
			// b) try to tune a rate request?
			// *) this is tough to do.
			// *) lock the bucket??

			retries++
			// 0.5 2 8 16 32 64 128  loop delays

			if retries < 2 {
		            /* leave alone */
			} else if retries < 4 {
			    uSdelay = uSdelay * 4
			} else if retries < 8 {
			    uSdelay = uSdelay * 2
			} else {
			    uSdelay = uint64(float64(uSdelay) * 1.2) // slow the growth rate down
			}

			if uSdelay > 1000000 {
			    uSdelay =  uint64(1000000)
			}

			tst := atomic.AddUint64(&b.retryReq429, 1)
			if (tst == 1) {
			    if conf.minBackoff == 0 {
			        fmt.Printf("HTTP status %d, adaptive retry (min=0.5 ms.) then inc. for 17 loops, consider '-r #' rate limit -or- '-c #' to lower client threads:\n",code)
			    } else {
			        fmt.Printf("HTTP status %d, adaptive retry (min=%d ms.) then inc. for 17 loops, consider '-r #' rate limit -or- '-c #' to lower client threads:\n",code,conf.minBackoff)
			    }
			    // fmt.Printf("%v\n", resp);
			    // fmt.Printf("sleep %d uSec. refer to final HTTP 429 retries stat (this message only prints once)\n",uSdelay);
			}

			if (retries <= 18) { 
		            // sleep uSdelay Microseconds
                            time.Sleep(time.Duration(uSdelay) * time.Microsecond)
		            goto REDO_LOOP
			}
			// tried a delay up to N seconds things are just stuck, so fail
		    }
		}

/* XXX ANALYZE RESPONSE */
		resp_bytes := len(resp.Body())
		hits_bytes := 0
		total_hits := 0
		bytesRead := 0

		resp_status_total := 0
		resp_status_failed := 0
		resp_status_successful := 0


		var result CbFtsRespShort
if (code == 200) {
		s := gjson.Get(string(resp.Body()), "total_hits")
		n, err := strconv.Atoi(s.String())
		if err == nil {
			total_hits = n
			if total_hits == 0 {
				hits_bytes = 0
			} else {
				value := gjson.Get(string(resp.Body()), "hits")
				hits_bytes = len(value.String())
			}
		}

		ts := gjson.Get(string(resp.Body()), "bytesRead")
		tn, terr := strconv.Atoi(ts.String())
		if terr == nil {
			bytesRead = tn
		}


		if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
			fmt.Println("Can not unmarshal JSON for Couchbase Fts Resp")
			fmt.Println(repl,"unmarhall_issue does the index exist?")
		} 

		resp_status_total = result.Status.Total
		resp_status_failed = result.Status.Failed
		resp_status_successful = result.Status.Successful
}
/*
		if total_hits > 0 {
			curindex := gjson.Get(string(resp.Body()), "hits.0.index")
fmt.Printf("HHHHH hits %v GGGG %v\n",result.Hits[0],curindex)
		}
*/
		atomic.AddUint64(&b.resp_cnt, 1)
		atomic.AddUint64(&b.resp_tot_hits, uint64(total_hits))
		atomic.AddUint64(&b.resp_tot_bytesRead, uint64(bytesRead))
		// min10 := math.Min(float64(total_hits),10)
if (code == 200) {
		min10 := len(result.Hits);
		atomic.AddUint64(&b.resp_tot_hits_docreads, uint64(min10))
		atomic.AddUint64(&b.resp_tot_bytes, uint64(resp_bytes))

		atomic.AddUint64(&b.resp_tot_status_total, uint64(resp_status_total))
		atomic.AddUint64(&b.resp_tot_status_failed, uint64(resp_status_failed))
		atomic.AddUint64(&b.resp_tot_status_successful, uint64(resp_status_successful))
}

		if total_hits > 0 {
			atomic.AddUint64(&b.resp_withhits_cnt, 1)
		        atomic.AddUint64(&b.hits_tot_bytes, uint64(hits_bytes))

			if len(conf.kvDocLookups) > 0 {
				// read the min10 docs we emulate an end-to-end application
// fmt.Printf("BBB bktseq %d <<%s>> bucket <<%s>> %v\n", bktseq, newuri, bktstr, req);
				doKvRead(bktstr,bktseq,b,conf,result)
			}
		}

		if conf.dynFtsShow {
			fmt.Println(repl,result.TotalHits)
			// just the Hits
			fmt.Printf("resp_sz_bytes: %d, totalhits %d, hits_returned %d, ids_returned: %v\n",len(resp.Body()), result.TotalHits, len(result.Hits), result.Hits)
			fmt.Println("NEW",resp_bytes,hits_bytes,total_hits)
			fmt.Printf("total_hits %d\n",total_hits)
		}

		if conf.trace {
			str, _ := formatJSON(resp.Body())
			fmt.Printf("%sRESP for req %d, code: %d\n%s\n", tag, preqno, code, str)
		}

		// fmt.Fprintf(os.Stderr, "\nJAS ALL fasthttpClient do() %v\n\n",string(resp.Body()));

		// =================== JAS ENQUEUE ====================
		if doSend && conf.isEnqueue {
			if conf.isBulk == false {
				var result CbQueueOneRespShort
				if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
					fmt.Println("Can not unmarshal JSON for Couchbase Queue One Send")
				} else {
					ekey := fmt.Sprintf("\"%v %v %v %v\"", result.Status.Type, result.Status.Code, result.Status.Name, result.Status.Desc)
					patternCnt := b.enqCodes.IncGet(ekey)
					name := result.Status.Name
					if name == "ENQ_SUCCESSFUL" {
						atomic.AddUint64(&b.enqcount, 1)
						atomic.AddUint64(&b.enqvalid, 1)
					} else {
						//fmt.Printf("ENQUEUE enqueue sent %d, accepted %d (rejected %d KV issue?)\n", 1, 0, 1)
						atomic.AddUint64(&b.enqcount, 1)
						if conf.debugErrorStatus && (patternCnt <= conf.debugEsLimit || conf.debugEsLimit == 0) {
							fmt.Printf("ERROR: ENQUEUE reqsz %d, idx N/A of N/A arysz: %v\n", 1, PrettyPrintOneLine(result.Status))
						}
					}
				}
			} else {
				var result CbQueueBatchRespShort
				if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
					fmt.Println("Can not unmarshal JSON for Couchbase Queue Batch Send")
				} else {
					var alen = len(result.Responses)
					var valid = alen
					for i := 0; i < alen; i++ {
						rec := result.Responses[i]
						ekey := fmt.Sprintf("\"%v %v %v %v\"", rec.Status.Type, rec.Status.Code, rec.Status.Name, rec.Status.Desc)
						patternCnt := b.enqCodes.IncGet(ekey)
						// aryStatus := rec.Status.Code
						aryName := rec.Status.Name
						if aryName != "ENQ_SUCCESSFUL" {
							if conf.debugErrorStatus && (patternCnt <= conf.debugEsLimit || conf.debugEsLimit == 0) {
								fmt.Printf("ERROR: ENQUEUE reqsz %d, idx N/A of N/A arysz: %v\n", 1, PrettyPrintOneLine(rec.Status))
							}
							valid--
						}
					}
					atomic.AddUint64(&b.enqcount, uint64(alen))
					atomic.AddUint64(&b.enqvalid, uint64(valid))
					if alen != valid {
						//fmt.Printf("ENQUEUE enqueue sent %d, accepted %d (rejected %d KV issue?)\n", alen, valid, alen - valid)
					}
				}
			}
		}

		// =================== JAS DEQUEUE FOR ACK ====================
		if conf.customAck && len(altbody) == 0 {
			// This is pass one (1) of a DEQUEUE action generate the ACK body

			if conf.isBulk == false {

				var result CbQueueOneRespShort
				if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
					fmt.Println("Can not unmarshal JSON for Couchbase Queue One Receive")
				} else {
					ekey := fmt.Sprintf("\"%v %v %v %v\"", result.Status.Type, result.Status.Code, result.Status.Name, result.Status.Desc)
					patternCnt := b.deqCodes.IncGet(ekey)
					name := result.Status.Name
					if name == "DEQ_SUCCESSFUL_NO_MSGS" {
						tmp := "[]"
						ackBody = []byte(tmp)
						numToAck = 0
						if conf.debugRateLimit {
							fmt.Printf("DEQUEUE dequeue requested %d, received %d, valid %d (queue empty?)\n", int(conf.reqBatchSz), 0, 0)
						}
						sleepTime := 100 * time.Millisecond
						time.Sleep(sleepTime)
						atomic.AddUint64(&b.deqthrottle, 1)

					} else if name == "DEQ_SUCCESSFUL" {
						tmp := "{\"msg_id\": \"" + result.Envelope.MsgID + "\"}"
						ackBody = []byte(tmp)
						numToAck = 1
						atomic.AddUint64(&b.deqbreqs, 1)
						atomic.AddUint64(&b.deqcount, 1)
						atomic.AddUint64(&b.deqvalid, 1)
					} else {
						//fmt.Printf("DEQUEUE dequeue requested %d, received %d, valid %d (missing %d KV docs?)\n", int(conf.reqBatchSz), 0, 0, 1)
						tmp := "[]"
						ackBody = []byte(tmp)
						numToAck = 0
						if conf.debugErrorStatus {
						}
						if conf.debugErrorStatus && (patternCnt <= conf.debugEsLimit || conf.debugEsLimit == 0) {
							fmt.Printf("ERROR: DEQUEUE reqsz %d, idx N/A of N/A arysz: %v\n", 1, PrettyPrintOneLine(result.Status))
						}
						atomic.AddUint64(&b.deqbreqs, 1)
						atomic.AddUint64(&b.deqcount, 1)
						atomic.AddUint64(&b.deqvalid, 0)
					}

					// fmt.Println(PrettyPrint(result));
					// fmt.Println(PrettyPrint(result.Envelope));
					// ackBody, _ = json.Marshal(result.Envelope);

				}
			} else {

				var result CbQueueBatchRespShort
				if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
					fmt.Println("Can not unmarshal JSON for Couchbase Queue Batch Receive")
				} else {
					// fmt.Println(PrettyPrint(result));
					// fmt.Println(PrettyPrint(result.Responses));

					// Loop through the data node for the FirstName
					var tmp string = "["
					var alen = len(result.Responses)
					valid := alen
					if alen == 0 {
						tmp = tmp + "]"
					} else {
						for i := 0; i < alen; i++ {
							rec := result.Responses[i]
							ekey := fmt.Sprintf("\"%v %v %v %v\"", rec.Status.Type, rec.Status.Code, rec.Status.Name, rec.Status.Desc)
							patternCnt := b.deqCodes.IncGet(ekey)

							// aryStatus := rec.Status.Code
							aryName := rec.Status.Name
							if aryName != "DEQ_SUCCESSFUL" {
								if conf.debugErrorStatus && (patternCnt <= conf.debugEsLimit || conf.debugEsLimit == 0) {
									fmt.Printf("ERROR: BATCH DEQUEUE reqsz %d, idx %d of %d arysz: %v\n", int(conf.reqBatchSz), i, alen, PrettyPrintOneLine(rec.Status))
								}
								valid--
							} else {
								// avoid illegal JSON if we have some non-valids
								if i == 0 || tmp == "[" {
									tmp = tmp + "{\"msg_id\": \"" + rec.Envelope.MsgID + "\"}"
								} else {
									tmp = tmp + ",{\"msg_id\": \"" + rec.Envelope.MsgID + "\"}"
								}
								// fmt.Println(rec.Envelope.MsgID)
							}
						}
						tmp = tmp + "]"
						// fmt.Println(tmp)
					}

					if alen == valid && alen < int(conf.reqBatchSz) {
						// for both 0 and any respnse without an error that is less than batch size
						if conf.debugRateLimit {
							fmt.Printf("DEQUEUE batch dequeue requested %d, received %d, valid %d (queue empty?)\n", int(conf.reqBatchSz), alen, valid)
						}
						sleepTime := 100 * time.Millisecond
						time.Sleep(sleepTime)
						atomic.AddUint64(&b.deqthrottle, 1)
					}
					if alen != valid {
						//fmt.Printf("DEQUEUE batch dequeue requested %d, received %d, valid %d (missing %d KV docs?)\n", int(conf.reqBatchSz), alen, valid, alen - valid)
					}

					atomic.AddUint64(&b.deqbreqs, uint64(conf.reqBatchSz))
					atomic.AddUint64(&b.deqcount, uint64(alen))
					atomic.AddUint64(&b.deqvalid, uint64(valid))
					numToAck = valid
					ackBody = []byte(tmp)
				}
			}
			// ackBody is a byte string that we want to send back to the client
			// fmt.Println(string(ackBody));
		}

		// =================== JAS CHECK DEQUEUE ACK RESPAONSE  ====================
		if conf.customAck && len(altbody) > 0 && conf.isDequeue {
			if conf.isBulk == false {
				var result CbQueueOneRespShort
				if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
					fmt.Println("Can not unmarshal JSON for Couchbase Queue One Send")
				} else {
					ekey := fmt.Sprintf("\"%v %v %v %v\"", result.Status.Type, result.Status.Code, result.Status.Name, result.Status.Desc)
					patternCnt := b.ackCodes.IncGet(ekey)
					name := result.Status.Name
					if name == "ACK_ACCEPTED" {
						atomic.AddUint64(&b.ackbreqs, 1)
						atomic.AddUint64(&b.ackcount, 1)
						atomic.AddUint64(&b.ackvalid, 1)
					} else {
						if conf.debugErrorStatus && (patternCnt <= conf.debugEsLimit || conf.debugEsLimit == 0) {
							fmt.Printf("ERROR ACK reqsz %d, idx N/A of N/A arysz: %v\n", 1, PrettyPrintOneLine(result.Status))
						}
						atomic.AddUint64(&b.ackbreqs, 1)
						atomic.AddUint64(&b.ackcount, 1)
					}
				}
			} else {
				var result CbQueueBatchRespShort
				if err := json.Unmarshal(resp.Body(), &result); err != nil { // Parse []byte to the go struct pointer
					fmt.Println("Can not unmarshal JSON for Couchbase ACK Batch Send")
				} else {
					var alen = len(result.Responses)
					var valid = alen
					for i := 0; i < alen; i++ {
						rec := result.Responses[i]
						ekey := fmt.Sprintf("\"%v %v %v %v\"", rec.Status.Type, rec.Status.Code, rec.Status.Name, rec.Status.Desc)
						patternCnt := b.ackCodes.IncGet(ekey)
						// aryStatus := rec.Status.Code
						aryName := rec.Status.Name
						if aryName != "ACK_ACCEPTED" {
							if conf.debugErrorStatus && (patternCnt <= conf.debugEsLimit || conf.debugEsLimit == 0) {
								fmt.Printf("ERROR: BATCH ACK reqsz %d, idx %d of %d arysz: %v\n", int(conf.reqBatchSz), i, alen, PrettyPrintOneLine(rec.Status))
							}
							valid--
						}
					}
					atomic.AddUint64(&b.ackbreqs, uint64(acksToSend))
					atomic.AddUint64(&b.ackcount, uint64(alen))
					atomic.AddUint64(&b.ackvalid, uint64(valid))
					if alen != valid {
						//fmt.Printf("ACK batch after dequeue requested %d, received %d, valid %d (missing %d KV docs?)\n", int(numToAck), alen, valid, alen - valid)
					}
				}
			}
		}

		// ============================================

	}
	usTaken = uint64(time.Since(start).Nanoseconds() / 1000)

	// release resources
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)

	return
}

type httpClient struct {
	client *http.Client

	headers http.Header
	url     *url.URL
	method  string

	body    *string
	bodProd bodyStreamProducer
}

func newHTTPClient(opts *clientOpts) client {
	c := new(httpClient)
	tr := &http.Transport{
		TLSClientConfig:     opts.tlsConfig,
		MaxIdleConnsPerHost: int(opts.maxConns),
		DisableKeepAlives:   opts.disableKeepAlives,
	}
	tr.DialContext = httpDialContextFunc(opts.bytesRead, opts.bytesWritten)
	if opts.HTTP2 {
		_ = http2.ConfigureTransport(tr)
	} else {
		tr.TLSNextProto = make(
			map[string]func(authority string, c *tls.Conn) http.RoundTripper,
		)
	}

	cl := &http.Client{
		Transport: tr,
		Timeout:   opts.timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	c.client = cl

	c.headers = headersToHTTPHeaders(opts.headers)
	c.method, c.body, c.bodProd = opts.method, opts.body, opts.bodProd
	var err error
	c.url, err = url.Parse(opts.url)
	if err != nil {
		// opts.url guaranteed to be valid at this point
		panic(err)
	}

	return client(c)
}

func (c *httpClient) do(b *bombardier, preqno uint64, conf config, altbody string, acksToSend int) (
	code int, usTaken uint64, ackBody []byte, numToAck int, err error,
) {
	req := &http.Request{}

	req.Header = c.headers
	req.Method = c.method
	req.URL = c.url

	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	if c.body != nil {
		br := strings.NewReader(*c.body)
		req.ContentLength = int64(len(*c.body))
		req.Body = ioutil.NopCloser(br)
	} else {
		bs, bserr := c.bodProd()
		if bserr != nil {
			return 0, 0, nil, 0, bserr
		}
		req.Body = bs
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		code = -1
	} else {
		code = resp.StatusCode

		_, berr := io.Copy(ioutil.Discard, resp.Body)

		// ==== JAS NOT IMPLEMENTED ====
		// b, berr := io.Copy(ioutil.Discard, resp.Body)
		// fmt.Fprintf(os.Stderr, "\nJAS httpClient do() %v\n\n",b);
		// =============================

		if berr != nil {
			err = berr
		}

		if cerr := resp.Body.Close(); cerr != nil {
			err = cerr
		}
	}
	usTaken = uint64(time.Since(start).Nanoseconds() / 1000)

	return
}

func headersToFastHTTPHeaders(h *headersList) *fasthttp.RequestHeader {
	if len(*h) == 0 {
		return nil
	}
	res := new(fasthttp.RequestHeader)
	for _, header := range *h {
		res.Set(header.key, header.value)
	}
	return res
}

func headersToHTTPHeaders(h *headersList) http.Header {
	if len(*h) == 0 {
		return http.Header{}
	}
	headers := http.Header{}

	for _, header := range *h {
		headers[header.key] = []string{header.value}
	}
	return headers
}


// var collection *gocb.Collection
var collections [80]*gocb.Collection

func init() {
        cfg, err := parser.parse(os.Args)
        if err != nil {
                fmt.Println(err)
                os.Exit(exitFailure)
        }
//fmt.Println(cfg.kvDocLookups)
	if len(cfg.kvDocLookups) == 0 {
		return;
	}

/*
                u, err := url.Parse(string(cfg.url))
                if err != nil {
                        fmt.Println(err)
                }
                q := u.Query()

    fmt.Println(u.Host)
    host, port, _ := net.SplitHostPort(u.Host)
    fmt.Println(host)
    fmt.Println(port)

                
		fmt.Println("RequestURI:",u.RequestURI())
		fmt.Println("Path:", u.Path)
		fmt.Println("RawPath:", u.RawPath)
		fmt.Println("EscapedPath:", u.EscapedPath())
		fmt.Println("  bucket=",q.Get("bucket"))
		fmt.Println("  scope=",q.Get("scope"))
		fmt.Println("  maxmsgs=",q.Get("maxmsgs"))
*/

  //initialize static instance on load
  // cluster, err := gocb.Connect("couchbase://10.0.1.28", gocb.ClusterOptions{
  // cluster, err := gocb.Connect("couchbase://127.0.0.1", gocb.ClusterOptions{

    connstr := "couchbase://"+ cfg.kvDocLookups
    fmt.Println("INIT KV connect to: " + connstr);

    cluster, err := gocb.Connect("couchbase://"+ cfg.kvDocLookups, gocb.ClusterOptions{
        Authenticator: gocb.PasswordAuthenticator{
          Username: "admin",
          Password: "jtester",
        },
    })
    if err != nil {
        log.Fatal(err)
    }


    var bucket *gocb.Bucket

    // we could have more than one bucket
    var curbkt string = ""
    if cfg.lenBucketSeq > 0 {
	for num := cfg.begBucketSeq; num <= cfg.endBucketSeq; num++ {
	    strnum := strconv.Itoa(num)
	    if len(strnum) != cfg.lenBucketSeq {
		strnum = "0" + strnum
	    }
	    curbkt = strings.ReplaceAll(cfg.kvBucket, cfg.patBucketSeq, strnum)
	    // fmt.Printf("%s b %d e %d len %d strnum %s <<%s>>\n",cfg.patBucketSeq,cfg.begBucketSeq,cfg.endBucketSeq,cfg.lenBucketSeq, strnum, curbkt);

   
	    bucket = cluster.Bucket(curbkt)
	    // bucket := cluster.Bucket("ts01")
	    err = bucket.WaitUntilReady(5*time.Second, nil)
	    if err != nil {
		log.Fatal(err)
	    }
	    // fmt.Println("INIT open scope.collection of _default._default");
	    collection := bucket.DefaultCollection()
	    collections[num-1] = collection
	}
	fmt.Println("INIT open bucket(s): " + cfg.kvBucket + ", on scope.collection of _default._default");
    } else {
	curbkt = cfg.kvBucket
	fmt.Println("INIT open bucket: " + curbkt + ", on scope.collection of _default._default");
   
        bucket = cluster.Bucket(curbkt)
	// bucket := cluster.Bucket("ts01")
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
	    log.Fatal(err)
	}
	// fmt.Println("INIT open scope.collection of _default._default");
        collection := bucket.DefaultCollection()
        collections[0] = collection
    }

   
fmt.Println("INIT done");

}

/*
//GetInstanceA - get singleton instance pre-initialized
func GetCollection() *gocb.Collection {
 return collections[0]
}
*/


func doKvRead(bktstr string, bktseq int, b *bombardier, conf config, result CbFtsRespShort) {

    var err error
    var collection = collections[0]
    if conf.lenBucketSeq > 0 {
	 // fmt.Printf("CCC source collection from idx %d bucket %s._default._default\n", bktseq, bktstr)
	 collection = collections[bktseq-1]
    }

    // break up into batches of 128
    var ops []gocb.BulkOp
    var maxbsz = 32
    bnum := 0
    idx := 0
    icnt := 0
    imax := len(result.Hits)
    for _, hit := range result.Hits {
	 icnt++;
	 str := fmt.Sprintf("%s",hit.Id)
	 //fmt.Println("fetch: ",str)
         ops = append(ops, &gocb.GetOp{ID: string(str)})
	 idx++

	 if idx >= maxbsz || imax == icnt {


        //ops = append(ops, &gocb.GetOp{ID: "airline_391"})

        err = collection.Do(ops,nil)
        if err != nil {
            fmt.Println(err)
            return
        }

        for _, op := range ops {
                getOp, ok := op.(*gocb.GetOp)
                if !ok {
                        fmt.Println("Could not type assert BulkOp into GetOp")
                        os.Exit(0);
                }

                if getOp.Err != nil {
                        // fmt.Println("Expected GetOp Err to be nil");
                        fmt.Println("Expected GetOp Err to be nil but was %v", getOp.Err)
                } else {

                        if getOp.Result.Cas() == 0 {
                                fmt.Println("Expected GetOp Cas to be non zero")
                                os.Exit(0);
                        }

                        var content interface{}
                        err = getOp.Result.Content(&content)
                        if err != nil {
                                fmt.Println("F\nailed to get content from GetOp %v", err)
                                os.Exit(0);
                        }
                        // prettyJSON, err := json.MarshalIndent(content, "", "    ")
                        prettyJSON, err := json.Marshal(content)
                        if err != nil {
                                log.Fatal("Failed to generate json", err)
                                os.Exit(0);
                        }

                        s := string(prettyJSON)
			doclen := len(s)
		        atomic.AddUint64(&b.tot_kv_reads, 1)
		        atomic.AddUint64(&b.tot_kv_bytes_read, uint64(doclen))

			if conf.dynKvShow {
			    if (doclen <= 110) {
			        fmt.Printf("Batch item (len=%d):%s\n", len(s),s)
			    } else {
			        fmt.Printf("Batch item (len=%d):%s ...\n", len(s),s[0:110])
			    }
			}
                }
    }
		//fmt.Printf("processing batch %d of %d items (len of result.Hits %d)\n",bnum,idx,imax)
		ops = nil
		idx = 0
		bnum++
        }
    }

}
