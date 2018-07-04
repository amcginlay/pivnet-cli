package download

import (
	"time"
	"github.com/cavaliercoder/grab"
	"net/url"
	"fmt"
)

//go:generate counterfeiter -o ./fakes/proxy_request.go --fake-name ProxyRequest . IProxyRequest
type IProxyRequest interface {
	Wrapped() *grab.Request
	URL() *url.URL
}

type ProxyRequest struct {
	wrapped *grab.Request
}

func NewProxyRequest(request *grab.Request) IProxyRequest {
	return &ProxyRequest{
		wrapped: request,
	}
}

func (p ProxyRequest) Wrapped() *grab.Request {
	return p.wrapped
}

func (p ProxyRequest) URL() *url.URL {
	return p.Wrapped().URL()
}

//go:generate counterfeiter -o ./fakes/proxy_response.go --fake-name ProxyResponse . IProxyResponse
type IProxyResponse interface {
	Filename() string
	Size() int64
	Request() IProxyRequest
	DidTimeout() bool
	SetDidTimeout()

	Err() error
	IsComplete() bool
	BytesPerSecond() float64
	BytesComplete() int64
	Progress() float64
	Done()
	ETA() time.Time
}

type ProxyResponse struct {
	Wrapped *grab.Response
	filename string
	size int64
	request IProxyRequest
	didTimeout bool
}

func NewProxyResponse(response *grab.Response) IProxyResponse {
	return &ProxyResponse {
		Wrapped: response,
		filename: response.Filename,
		size: response.Size,
		request: NewProxyRequest(response.Request),
		didTimeout: false,
	}
}

func (p ProxyResponse) Filename() string {
	return p.filename
}

func (p ProxyResponse) Size() int64 {
	return p.size
}

func (p ProxyResponse) Request() IProxyRequest {
	return p.request
}

func (p ProxyResponse) DidTimeout() bool {
	return p.didTimeout
}

func (p ProxyResponse) SetDidTimeout() {
	p.didTimeout = true
}

func (p ProxyResponse) Err() error {
	if p.didTimeout {
		return fmt.Errorf("a download timed out for chunk: %s", p.filename)
	}
	return p.Wrapped.Err()
}

func (p ProxyResponse) IsComplete() bool {
	return p.Wrapped.IsComplete()
}

func (p ProxyResponse) BytesPerSecond() float64 {
	return p.Wrapped.BytesPerSecond()
}

func (p ProxyResponse) BytesComplete() int64 {
	return p.Wrapped.BytesComplete()
}

func (p ProxyResponse) Progress() float64 {
	return p.Wrapped.Progress()
}

func (p ProxyResponse) Done() {
	close(p.Wrapped.Done)
}

func (p ProxyResponse) ETA() time.Time {
	return p.Wrapped.ETA()
}

type ChannelProxy struct {
	Wrapped <-chan *grab.Response
	Channel <-chan IProxyResponse
}

func NewChannelProxy(responseChannel <-chan *grab.Response) *ChannelProxy {
	channel := make(chan IProxyResponse)
	go func() {
		for {
			select {
			case response := <-responseChannel:
				if response == nil {
					close(channel)
					return
				} else {
					channel <- NewProxyResponse(response)
				}

			}
		}
	}()
	return &ChannelProxy {
		Wrapped: responseChannel,
		Channel: channel,
	}
}