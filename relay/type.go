package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/golang/snappy"
	"io/ioutil"
	"net/http"
)

type BodyRequest struct {
	*http.Request
	body []byte
}

func (r *BodyRequest) Unmarshal(b []byte) error {
	data, err := snappy.Decode(nil, b)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, r)
}

func (r BodyRequest) Marshal() ([]byte, error) {
	r.Request.Body = nil
	byt, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, byt), nil
}

func (r *BodyRequest) BuildRequest(ctx context.Context) *http.Request {
	cpy := r.Request.WithContext(ctx)
	cpy.Body = ioutil.NopCloser(bytes.NewReader(r.body))
	return cpy
}
