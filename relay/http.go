package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/beeker1121/goque"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"runtime/debug"

	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HTTP is a relay for HTTP clickhouse writes
type HTTP struct {
	addr   string
	name   string
	schema string

	cert string

	closing int64
	l       net.Listener

	backends []*httpBackend
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512

	KB = 1024
	MB = 1024 * KB
)

func NewHTTP(cfg HTTPConfig) (Relay, error) {
	h := new(HTTP)

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i], cfg.BufferDir)
		if err != nil {
			return nil, err
		}

		h.backends = append(h.backends, backend)
	}

	return h, nil
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.l = l

	log.Printf("Starting %s relay %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = http.Serve(l, h)
	if atomic.LoadInt64(&h.closing) != 0 {
		return nil
	}
	return err
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)
	return h.l.Close()
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if len(h.backends) == 0 {
		w.Write([]byte("Ok.\n"))
		return
	}

	if r.Method != "POST" {
		h.backends[0].ReverseProxy(w, r)
		return
	}
	queryParams := r.URL.Query()
	if queryParams.Get("decompress") == "1" {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	if ac := queryParams.Get("query"); len(ac) >= 7 && strings.ToUpper(ac[:7]) == "SELECT " {
		h.backends[0].ReverseProxy(w, r)
		return
	}

	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(r.Body)
	if err != nil {
		putBuf(bodyBuf)
		dbError(w, http.StatusInternalServerError, "problem reading request body")
		return
	}
	if strings.ToUpper(string(bodyBuf.Bytes()[:7])) == "SELECT " {
		r.Body = ioutil.NopCloser(bodyBuf)
		h.backends[0].ReverseProxy(w, r)
		return
	}

	r.Body = ioutil.NopCloser(bytes.NewReader(bodyBuf.Bytes()))
	if err := h.backends[0].ReverseProxy(w, r); err != nil {
		return
	}

	for _, b := range h.backends[1:] {
		err = b.Append(r, bodyBuf.Bytes())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			log.Printf("Append %+v to %s %s", r, b.name, err.Error())
			return
		}
	}
	return
}

type httpBackend struct {
	name     string
	location *url.URL

	proxy *httputil.ReverseProxy

	timeout time.Duration
	queue   *goque.Queue
	done    func()
}

func newHTTPBackend(cfg *HTTPOutputConfig, dir string) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}
	location, err := url.Parse(cfg.Location)
	if err != nil {
		return nil, err
	}

	backend := &httpBackend{
		name:     cfg.Name,
		location: location,
		proxy:    httputil.NewSingleHostReverseProxy(location),
		timeout:  DefaultHTTPTimeout,
	}
	backend.proxy.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.SkipTLSVerification,
		},
	}
	backend.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(err.Error()))
	}

	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		backend.timeout = t
	}
	backend.queue, err = goque.OpenQueue(filepath.Join(dir, cfg.Name))
	if err != nil {
		return nil, err
	}
	ctx, done := context.WithCancel(context.Background())
	go backend.loop(ctx)
	backend.done = done

	return backend, nil
}

func (b *httpBackend) loop(ctx context.Context) {
	var wait bool

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if wait {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}

		it, err := b.queue.Peek()
		if err == goque.ErrEmpty || err == goque.ErrOutOfBounds {
			wait = true
			continue
		} else if err != nil {
			log.Println(b.name, "Peek", err)
			wait = true
			continue
		}

		var br BodyRequest
		if err = br.Unmarshal(it.Value); err != nil {
			log.Println(b.name, "Unmarshal", it.Value, err)
			b.queue.Dequeue()
			continue
		}

		ctx, cancel := context.WithTimeout(ctx, b.timeout)
		rq := br.BuildRequest(ctx)
		rp := &recordResponseWriter{ResponseWriter: DiscardResponseWriter}
		err = b.ReverseProxy(rp, rq)
		cancel()
		if err != nil {
			//log.Println(b.name, "ReverseProxy", err)
			wait = true
			continue
		} else if rp.statusCode != 200 {
			// TODO:
			log.Println(b.name, "ReverseProxy", rp.statusCode, string(rp.body))
		}

		b.queue.Dequeue()
		wait = false
	}
}

func (b *httpBackend) Append(r *http.Request, body []byte) error {
	br := &BodyRequest{body: body}

	if b.queue.Length() == 0 {
		rq := r.WithContext(r.Context())
		rq.Body = ioutil.NopCloser(bytes.NewReader(body))
		rp := &recordResponseWriter{ResponseWriter: DiscardResponseWriter}
		err := b.ReverseProxy(rp, rq)
		if err == nil && rp.statusCode == 200 {
			return nil
		}
	}

	data, err := br.Marshal(r)
	if err != nil {
		return err
	}
	_, err = b.queue.Enqueue(data)
	return err
}

func (b *httpBackend) ReverseProxy(w http.ResponseWriter, r *http.Request) error {
	rw := &recordResponseWriter{ResponseWriter: w}
	b.proxy.ServeHTTP(rw, r)
	return rw.Error()
}

var DiscardResponseWriter = discardResponseWriter{}

type discardResponseWriter struct{}

func (d discardResponseWriter) Header() http.Header         { return http.Header{} }
func (d discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d discardResponseWriter) WriteHeader(statusCode int)  {}

type recordResponseWriter struct {
	http.ResponseWriter
	statusCode int
	body       []byte
}

func (w *recordResponseWriter) Write(b []byte) (int, error) {
	if w.statusCode == http.StatusBadGateway {
		w.body = append(w.body, b...)
	}
	return w.ResponseWriter.Write(b)
}

func (w *recordResponseWriter) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)
	w.statusCode = statusCode
}

func (w *recordResponseWriter) Error() error {
	if w.statusCode != http.StatusBadGateway {
		return nil
	}
	return errors.New(string(w.body))
}

func dbError(w http.ResponseWriter, code int, message string) {
	data := fmt.Sprintf("Code:%d, e.displayText() = %s, e.what() = %s", code, "clickhouse-relay: "+message, string(debug.Stack()))
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}
