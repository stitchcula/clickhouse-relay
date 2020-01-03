package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/golang/snappy"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
)

type BodyRequest struct {
	Method           string
	URL              *url.URL
	Proto            string // "HTTP/1.0"
	ProtoMajor       int    // 1
	ProtoMinor       int    // 0
	Header           http.Header
	ContentLength    int64
	TransferEncoding []string
	Close            bool
	Host             string
	Form             url.Values
	PostForm         url.Values
	MultipartForm    *multipart.Form
	Trailer          http.Header
	RemoteAddr       string
	RequestURI       string
	TLS              *tls.ConnectionState
	body             []byte
}

func (r *BodyRequest) Unmarshal(b []byte) error {
	data, err := snappy.Decode(nil, b)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, r)
}

func (r BodyRequest) Marshal(rRequest *http.Request) ([]byte, error) {
	r.Method = rRequest.Method
	r.URL = rRequest.URL
	r.Proto = rRequest.Proto
	r.ProtoMajor = rRequest.ProtoMajor
	r.ProtoMinor = rRequest.ProtoMinor
	r.Header = rRequest.Header
	r.ContentLength = rRequest.ContentLength
	r.TransferEncoding = rRequest.TransferEncoding
	r.Close = rRequest.Close
	r.Host = rRequest.Host
	r.Form = rRequest.Form
	r.PostForm = rRequest.PostForm
	r.MultipartForm = rRequest.MultipartForm
	r.Trailer = rRequest.Trailer
	r.RemoteAddr = rRequest.RemoteAddr
	r.RequestURI = rRequest.RequestURI
	r.TLS = rRequest.TLS

	byt, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, byt), nil
}

func (r *BodyRequest) BuildRequest(ctx context.Context) *http.Request {
	cpy := &http.Request{
		Method:           r.Method,
		URL:              r.URL,
		Proto:            r.Proto,
		ProtoMajor:       r.ProtoMajor,
		ProtoMinor:       r.ProtoMinor,
		Header:           r.Header,
		ContentLength:    r.ContentLength,
		TransferEncoding: r.TransferEncoding,
		Close:            r.Close,
		Host:             r.Host,
		Form:             r.Form,
		PostForm:         r.PostForm,
		MultipartForm:    r.MultipartForm,
		Trailer:          r.Trailer,
		RemoteAddr:       r.RemoteAddr,
		RequestURI:       r.RequestURI,
		TLS:              r.TLS,
		Cancel:           ctx.Done(),
		GetBody: func() (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewReader(r.body)), nil
		},
		Body: ioutil.NopCloser(bytes.NewReader(r.body)),
	}
	return cpy.WithContext(ctx)
}
