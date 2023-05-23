package krtspd

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Client to server for presentation and stream objects; recommended
	DESCRIBE = "DESCRIBE"
	// Bidirectional for client and stream objects; optional
	ANNOUNCE = "ANNOUNCE"
	// Bidirectional for client and stream objects; optional
	GET_PARAMETER = "GET_PARAMETER"
	// Bidirectional for client and stream objects; required for Client to server, optional for server to client
	OPTIONS = "OPTIONS"
	// Client to server for presentation and stream objects; recommended
	PAUSE = "PAUSE"
	// Client to server for presentation and stream objects; required
	PLAY = "PLAY"
	// Client to server for presentation and stream objects; optional
	RECORD = "RECORD"
	// Server to client for presentation and stream objects; optional
	REDIRECT = "REDIRECT"
	// Client to server for stream objects; required
	SETUP = "SETUP"
	// Bidirectional for presentation and stream objects; optional
	SET_PARAMETER = "SET_PARAMETER"
	// Client to server for presentation and stream objects; required
	TEARDOWN = "TEARDOWN"
)

const (
	// all requests
	Continue = 100

	// all requests
	OK = 200
	// RECORD
	Created = 201
	// RECORD
	LowOnStorageSpace = 250

	// all requests
	MultipleChoices = 300
	// all requests
	MovedPermanently = 301
	// all requests
	MovedTemporarily = 302
	// all requests
	SeeOther = 303
	// all requests
	UseProxy = 305

	// all requests
	BadRequest = 400
	// all requests
	Unauthorized = 401
	// all requests
	PaymentRequired = 402
	// all requests
	Forbidden = 403
	// all requests
	NotFound = 404
	// all requests
	MethodNotAllowed = 405
	// all requests
	NotAcceptable = 406
	// all requests
	ProxyAuthenticationRequired = 407
	// all requests
	RequestTimeout = 408
	// all requests
	Gone = 410
	// all requests
	LengthRequired = 411
	// DESCRIBE, SETUP
	PreconditionFailed = 412
	// all requests
	RequestEntityTooLarge = 413
	// all requests
	RequestURITooLong = 414
	// all requests
	UnsupportedMediaType = 415
	// SETUP
	Invalidparameter = 451
	// SETUP
	IllegalConferenceIdentifier = 452
	// SETUP
	NotEnoughBandwidth = 453
	// all requests
	SessionNotFound = 454
	// all requests
	MethodNotValidInThisState = 455
	// all requests
	HeaderFieldNotValid = 456
	// PLAY
	InvalidRange = 457
	// SET_PARAMETER
	ParameterIsReadOnly = 458
	// all requests
	AggregateOperationNotAllowed = 459
	// all requests
	OnlyAggregateOperationAllowed = 460
	// all requests
	UnsupportedTransport = 461
	// all requests
	DestinationUnreachable = 462

	// all requests
	InternalServerError = 500
	// all requests
	NotImplemented = 501
	// all requests
	BadGateway = 502
	// all requests
	ServiceUnavailable = 503
	// all requests
	GatewayTimeout = 504
	// all requests
	RTSPVersionNotSupported = 505
	// all requests
	OptionNotsupport = 551
)

type RtspRequest struct {
	Method        string
	URL           *url.URL
	Proto         string
	ProtoMajor    int
	ProtoMinor    int
	Header        http.Header
	ContentLength int
	Body          io.ReadCloser
}

func (r RtspRequest) String() string {
	s := fmt.Sprintf("%s %s %s/%d.%d\r\n", r.Method, r.URL, r.Proto, r.ProtoMajor, r.ProtoMinor)
	for k, v := range r.Header {
		for _, v := range v {
			s += fmt.Sprintf("%s: %s\r\n", k, v)
		}
	}
	s += "\r\n"
	if r.Body != nil {
		str, _ := ioutil.ReadAll(r.Body)
		s += string(str)
	}
	return s
}

func NewRtspRequest(method, urlStr, cSeq string, body io.ReadCloser) (*RtspRequest, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	req := &RtspRequest{
		Method:     method,
		URL:        u,
		Proto:      "RTSP",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     map[string][]string{"CSeq": {cSeq}},
		Body:       body,
	}
	return req, nil
}

type RtspSession struct {
	cSeq    int
	conn    net.Conn
	session string
}

func NewRtspSession() *RtspSession {
	return &RtspSession{}
}

func (s *RtspSession) nextCSeq() string {
	s.cSeq++
	return strconv.Itoa(s.cSeq)
}

func (s *RtspSession) genSessionId() {
	bytes0 := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 9; i++ {
		result = append(result, bytes0[r.Intn(len(bytes0))])
	}
	s.session = string(result)
}

func (s *RtspSession) Describe(urlStr string) (*RtspResponse, error) {
	req, err := NewRtspRequest(DESCRIBE, urlStr, s.nextCSeq(), nil)
	if err != nil {
		panic(err)
	}

	req.Header.Add("Accept", "application/sdp")

	if s.conn == nil {
		s.conn, err = net.Dial("tcp", req.URL.Host)
		if err != nil {
			return nil, err
		}
	}

	_, err = io.WriteString(s.conn, req.String())
	if err != nil {
		return nil, err
	}
	return ReadRtspResponse(s.conn)
}

func (s *RtspSession) Options(urlStr string) (*RtspResponse, error) {
	req, err := NewRtspRequest(OPTIONS, urlStr, s.nextCSeq(), nil)
	if err != nil {
		panic(err)
	}

	if s.conn == nil {
		s.conn, err = net.Dial("tcp", req.URL.Host)
		if err != nil {
			return nil, err
		}
	}

	_, err = io.WriteString(s.conn, req.String())
	if err != nil {
		return nil, err
	}
	return ReadRtspResponse(s.conn)
}

func (s *RtspSession) Setup(urlStr, transport string) (*RtspResponse, error) {
	req, err := NewRtspRequest(SETUP, urlStr, s.nextCSeq(), nil)
	if err != nil {
		panic(err)
	}

	req.Header.Add("Transport", transport)

	if s.conn == nil {
		s.conn, err = net.Dial("tcp", req.URL.Host)
		if err != nil {
			return nil, err
		}
	}

	_, err = io.WriteString(s.conn, req.String())
	if err != nil {
		return nil, err
	}
	resp, err := ReadRtspResponse(s.conn)
	s.session = resp.Header.Get("Session")
	return resp, err
}

func (s *RtspSession) Play(urlStr, sessionId string) (*RtspResponse, error) {
	req, err := NewRtspRequest(PLAY, urlStr, s.nextCSeq(), nil)
	if err != nil {
		panic(err)
	}

	req.Header.Add("Session", sessionId)

	if s.conn == nil {
		s.conn, err = net.Dial("tcp", req.URL.Host)
		if err != nil {
			return nil, err
		}
	}

	_, err = io.WriteString(s.conn, req.String())
	if err != nil {
		return nil, err
	}
	return ReadRtspResponse(s.conn)
}

type closer struct {
	*bufio.Reader
	r io.Reader
}

func (c closer) Close() error {
	if c.Reader == nil {
		return nil
	}
	defer func() {
		c.Reader = nil
		c.r = nil
	}()
	if r, ok := c.r.(io.ReadCloser); ok {
		return r.Close()
	}
	return nil
}

func ParseRTSPVersion(s string) (proto string, major int, minor int, err error) {
	parts := strings.SplitN(s, "/", 2)
	proto = parts[0]
	parts = strings.SplitN(parts[1], ".", 2)
	if major, err = strconv.Atoi(parts[0]); err != nil {
		return
	}
	if minor, err = strconv.Atoi(parts[0]); err != nil {
		return
	}
	return
}

func ReadRtspRequest(r io.Reader) (req *RtspRequest, err error) {
	req = new(RtspRequest)
	req.Header = make(map[string][]string)

	b := bufio.NewReader(r)
	var s string

	if s, err = b.ReadString('\n'); err != nil {
		return
	}

	parts := strings.SplitN(s, " ", 3)
	req.Method = parts[0]
	if req.URL, err = url.Parse(parts[1]); err != nil {
		return
	}

	req.Proto, req.ProtoMajor, req.ProtoMinor, err = ParseRTSPVersion(parts[2])
	if err != nil {
		return
	}

	for {
		if s, err = b.ReadString('\n'); err != nil {
			return
		} else if s = strings.TrimRight(s, "\r\n"); s == "" {
			break
		}

		parts := strings.SplitN(s, ":", 2)
		req.Header.Add(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	}

	req.ContentLength, _ = strconv.Atoi(req.Header.Get("Content-Length"))
	req.Body = closer{b, r}
	return
}

type RtspResponse struct {
	Proto      string
	ProtoMajor int
	ProtoMinor int

	StatusCode int
	Status     string

	ContentLength int64

	Header http.Header
	Body   io.ReadCloser
}

func (res RtspResponse) String() string {
	s := fmt.Sprintf("%s/%d.%d %d %s\n", res.Proto, res.ProtoMajor, res.ProtoMinor, res.StatusCode, res.Status)
	for k, v := range res.Header {
		for _, v := range v {
			s += fmt.Sprintf("%s: %s\n", k, v)
		}
	}
	s += "\n"
	return s
}

func NewRtspResponse(cSeq string) *RtspResponse {
	return &RtspResponse{
		Proto:      "RTSP",
		ProtoMajor: 1,
		ProtoMinor: 0,
		StatusCode: OK,
		Status:     "OK",
		Header:     map[string][]string{"CSeq": {cSeq}},
	}
}

func ReadRtspResponse(r io.Reader) (res *RtspResponse, err error) {
	res = new(RtspResponse)
	res.Header = make(map[string][]string)

	b := bufio.NewReader(r)
	var s string

	if s, err = b.ReadString('\n'); err != nil {
		return
	}

	parts := strings.SplitN(s, " ", 3)
	res.Proto, res.ProtoMajor, res.ProtoMinor, err = ParseRTSPVersion(parts[0])
	if err != nil {
		return
	}

	if res.StatusCode, err = strconv.Atoi(parts[1]); err != nil {
		return
	}

	res.Status = strings.TrimSpace(parts[2])

	for {
		if s, err = b.ReadString('\n'); err != nil {
			return
		} else if s = strings.TrimRight(s, "\r\n"); s == "" {
			break
		}

		parts := strings.SplitN(s, ":", 2)
		res.Header.Add(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	}

	res.ContentLength, _ = strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)

	res.Body = closer{b, r}
	return
}

type RtspdHook struct {
	PushStart func(string, *url.URL) bool
	PushEnd   func(string, *url.URL)

	PullStart func(string, *url.URL) bool
	PullEnd   func(string, *url.URL)
}

const (
	Pusher = 0
	Puller = 1
)

type RtspClient struct {
	s          *RtspSession
	cid        string
	channel    string
	flag       int
	rtspOK     bool
	cacheSend  bool
	pullers    map[string]*RtspClient
	pullerLock sync.RWMutex
	sdp        []byte
	dc         chan []byte
	url        url.URL
}

type RtspServer struct {
	port       string
	pushers    map[string]*RtspClient
	pusherLock sync.RWMutex
	frameCache int
	rtspdHook  RtspdHook
}

func NewRtspServer(port string, frameCache int) *RtspServer {
	rs := new(RtspServer)
	rs.port = port
	rs.frameCache = frameCache

	return rs
}

func (rs *RtspServer) push(rc *RtspClient) {
	caches := make([][]byte, 0)
	for {
		d := <-rc.dc

		if len(d) == 1 {
			break
		}
		if rs.frameCache > 0 {
			if len(caches) == rs.frameCache {
				caches = append(caches[:0], caches[1:]...)
			}
			caches = append(caches, d)
		}

		removes := make(map[string]string)
		removeFlag := false
		rc.pullerLock.RLock()
		for _, v := range rc.pullers {
			if rs.frameCache > 0 && v.cacheSend == false {
				v.cacheSend = true
				for _, d := range caches {
					_, e := v.s.conn.Write(d)
					if e != nil {
						removes[v.cid] = v.cid
						removeFlag = true
						break
					}
				}
			}
			_, e := v.s.conn.Write(d)
			if e != nil {
				removes[v.cid] = v.cid
				removeFlag = true
			}
		}
		rc.pullerLock.RUnlock()

		if removeFlag {
			rc.pullerLock.Lock()
			for key := range removes {
				if _, ok := rc.pullers[key]; ok {
					delete(rc.pullers, key)
				}
			}
			rc.pullerLock.Unlock()
		}
	}
}

func (rs *RtspServer) close(rc *RtspClient) {
	rc.s.conn.Close()

	if rc.flag == Puller {
		rs.pusherLock.Lock()
		for k, v := range rs.pushers {
			if k == rc.channel {
				v.pullerLock.Lock()
				if _, ok := v.pullers[rc.cid]; ok {
					delete(v.pullers, rc.cid)
				}
				v.pullerLock.Unlock()
			}
		}
		rs.pusherLock.Unlock()
		if rs.rtspdHook.PullEnd != nil {
			rs.rtspdHook.PullEnd(rc.cid, &rc.url)
		}
	}

	if rc.flag == Pusher {
		rc.dc <- []byte("0")
		rs.pusherLock.Lock()
		if _, ok := rs.pushers[rc.channel]; ok {
			delete(rs.pushers, rc.channel)
		}
		rs.pusherLock.Unlock()
		if rs.rtspdHook.PushEnd != nil {
			rs.rtspdHook.PushEnd(rc.cid, &rc.url)
		}
	}
}

func (rs *RtspServer) handle(c net.Conn) {
	rc := new(RtspClient)
	rc.s = NewRtspSession()
	rc.s.genSessionId()
	rc.s.conn = c
	rc.cid = c.RemoteAddr().String()
	rc.dc = make(chan []byte, 10000)
	rc.rtspOK = false
	rc.cacheSend = false
	rc.pullers = make(map[string]*RtspClient)

	defer rs.close(rc)

	for {
		if rc.rtspOK {
			buf := make([]byte, 2048)
			n, err := c.Read(buf)
			if n == 0 || err != nil {
				break
			}
			rc.dc <- buf[0:n]
		} else {
			req, err := ReadRtspRequest(c)
			if err != nil {
				log.Println(err)
				break
			}

			switch req.Method {
			case OPTIONS:
				rc.channel = req.URL.Path

				resp := NewRtspResponse(req.Header.Get("Cseq"))
				resp.Header.Add("Session", rc.s.session)
				resp.Header.Add("Public", "DESCRIBE, SETUP, TEARDOWN, PLAY, PAUSE, OPTIONS, ANNOUNCE, RECORD")
				_, err = io.WriteString(c, resp.String())
				if err != nil {
					log.Println(err)
				}
			case ANNOUNCE:
				if rs.rtspdHook.PushStart != nil {
					if !rs.rtspdHook.PushStart(rc.cid, req.URL) {
						log.Println("push start hook failed")
						break
					}
				}
				rc.url = *req.URL
				rc.flag = Pusher
				rc.sdp = make([]byte, req.ContentLength)
				req.Body.Read(rc.sdp)

				rs.pusherLock.Lock()
				rs.pushers[rc.channel] = rc
				rs.pusherLock.Unlock()

				resp := NewRtspResponse(req.Header.Get("Cseq"))
				resp.Header.Add("Session", rc.s.session)
				_, err = io.WriteString(c, resp.String())
				if err != nil {
					log.Println(err)
				}
			case SETUP:
				resp := NewRtspResponse(req.Header.Get("Cseq"))
				resp.Header.Add("Session", rc.s.session)
				resp.Header.Add("Transport", req.Header.Get("Transport"))
				_, err = io.WriteString(c, resp.String())
				if err != nil {
					log.Println(err)
				}
			case RECORD:
				resp := NewRtspResponse(req.Header.Get("Cseq"))
				resp.Header.Add("Session", rc.s.session)
				_, err = io.WriteString(c, resp.String())
				if err != nil {
					log.Println(err)
				}
				if !rc.rtspOK {
					rc.rtspOK = true
					go rs.push(rc)
				}
			case DESCRIBE:
				if rs.rtspdHook.PullStart != nil {
					if !rs.rtspdHook.PullStart(rc.cid, req.URL) {
						log.Println("pull start hook failed")
						break
					}
				}
				rc.url = *req.URL
				rc.channel = req.URL.Path
				rc.flag = Puller

				rs.pusherLock.RLock()
				v, ok := rs.pushers[rc.channel]
				rs.pusherLock.RUnlock()

				resp := NewRtspResponse(req.Header.Get("Cseq"))
				if ok {
					resp.Header.Add("Session", rc.s.session)
					resp.Header.Add("Content-Length", strconv.Itoa(len(v.sdp)))
				} else {
					resp.StatusCode = NotFound
					resp.Status = "NotFound"
				}
				_, err = io.WriteString(c, resp.String())
				if err != nil {
					log.Println(err)
				}
				if ok {
					_, err = c.Write(v.sdp)
					if err != nil {
						log.Println(err)
					}
				}
			case PLAY:
				rs.pusherLock.Lock()
				v, ok := rs.pushers[rc.channel]
				if ok {
					rc.rtspOK = true
					v.pullerLock.Lock()
					v.pullers[rc.cid] = rc
					v.pullerLock.Unlock()
				}
				rs.pusherLock.Unlock()

				resp := NewRtspResponse(req.Header.Get("Cseq"))
				resp.Header.Add("Session", rc.s.session)
				resp.Header.Add("Range", "npt=0.000-")
				_, err = io.WriteString(c, resp.String())
				if err != nil {
					log.Println(err)
				}
			default:
				log.Printf("invalid method:%s\n", req.Method)
				break
			}
		}
	}
}

func (rs *RtspServer) SetHook(hook RtspdHook) {
	rs.rtspdHook = hook
}

func (rs *RtspServer) Start() {
	rs.pushers = make(map[string]*RtspClient)

	l, err := net.Listen("tcp", rs.port)
	if err != nil {
		log.Println(err)
		return
	}
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go rs.handle(c)
	}
}
