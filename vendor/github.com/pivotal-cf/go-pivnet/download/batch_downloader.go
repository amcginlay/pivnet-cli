package download

import (
	"fmt"
	"time"
	"os"
	"strings"
	"github.com/cavaliercoder/grab"
	"net/url"
)

type BatchDownloader struct {
	DownloadClient *grab.Client
}

type ErrorDownload struct {
	CanRetry bool
	Error error
	Requests []*ProxyRequest
}

func NewBatchDownloader() *BatchDownloader {
	return &BatchDownloader{DownloadClient: grab.NewClient()}
}


func (c *BatchDownloader) DoOLD (requests ...*grab.Request) ErrorDownload {
	responseChannel := c.DownloadClient.DoBatch(-1, requests...)
	proxyResponseChannel := NewChannelProxy(responseChannel)
	return WaitForComplete(proxyResponseChannel.Channel)
}

func (c *BatchDownloader) Do (requests ...*ProxyRequest) ErrorDownload {
	var realGrabRequests []*grab.Request
	for _, request := range requests {
		realGrabRequests = append(realGrabRequests, request.Wrapped)
	}

	responseChannel := c.DownloadClient.DoBatch(-1, realGrabRequests...)
	proxyResponseChannel := NewChannelProxy(responseChannel)
	return WaitForComplete(proxyResponseChannel.Channel)
}

type ProxyRequest struct {
	Wrapped *grab.Request
}

func NewProxyRequest(request *grab.Request) *ProxyRequest {
	return &ProxyRequest{
		Wrapped: request,
	}
}

func (p ProxyRequest) URL() *url.URL {
	return p.Wrapped.URL()
}

type ProxyResponse struct {
	Wrapped *grab.Response
	Filename string
	Size int64
	Request *ProxyRequest
}

func NewProxyResponse(response *grab.Response) *ProxyResponse {
	return &ProxyResponse {
		Wrapped: response,
		Filename: response.Filename,
		Size: response.Size,
		Request: NewProxyRequest(response.Request),
	}
}

func (p ProxyResponse) Err() error {
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
	Channel <-chan *ProxyResponse
}

func NewChannelProxy(responseChannel <-chan *grab.Response) *ChannelProxy {
	channel := make(chan *ProxyResponse)
	go func() {
		for {
			select {
				case response := <-responseChannel:
					if response == nil {
						close(channel)
						break
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

func MapToErrorDownload(downloadResponses []*ProxyResponse) ErrorDownload {
	var failedDownloadResponses []*ProxyRequest
	var errstrings []string

	for _, downloadResponse := range downloadResponses {
		if downloadResponse.Err() != nil {
			failedDownloadResponses = append(failedDownloadResponses, downloadResponse.Request)
			errstrings = append(errstrings, downloadResponse.Err().Error())
		}
	}

	var error error
	if len(errstrings) > 0 {
		error = fmt.Errorf(strings.Join(errstrings, "\n"))
	} else {
		error = nil
	}

	return ErrorDownload{
		Requests: failedDownloadResponses,
		Error: error,
		CanRetry: true,
	}
}

func WaitForComplete(channelProxy <-chan *ProxyResponse) ErrorDownload{
	const timeoutDuration = 5 * time.Second
	stalledDownloadChannel := make(chan *ProxyResponse)
	var downloadResponses []*ProxyResponse
	t := time.NewTicker(500 * time.Millisecond)
	stalledDownloadTimer := time.AfterFunc(timeoutDuration, func() {
		for _, response := range downloadResponses {
			if response != nil && !response.IsComplete() && response.BytesPerSecond() == 0 {
				stalledDownloadChannel <- response
			}
		}
	})

	defer t.Stop()
	defer stalledDownloadTimer.Stop()

	for {
		select {
		case downloadResponse := <-channelProxy:
			if downloadResponse != nil {
				// a new response has been received and has started downloading
				downloadResponses = append(downloadResponses, downloadResponse)
			} else {
				return MapToErrorDownload(downloadResponses)
			}

		case <-t.C:
			updateUI(downloadResponses)
			for _, response := range downloadResponses {
				if response != nil && response.BytesPerSecond() > 0 {
					stalledDownloadTimer.Reset(timeoutDuration)
				} else {
					//a download has stalled
				}
			}

		case stalledResponse := <-stalledDownloadChannel:
			stalledResponse.Done()  //stop download
			return ErrorDownload {
				CanRetry: false,
				Error: fmt.Errorf("a download timed out for chunk: %s", stalledResponse.Filename),
			}
		}
	}
}

func updateUI(responses []*ProxyResponse) {
	fmt.Println("***************\n\n\n\n\n********************")
	// print newly completed downloads
	for i, resp := range responses {
		if resp != nil && resp.IsComplete() {
			if resp.Err() != nil {
				fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n",
					resp.Request.URL(),
					resp.Err())
			} else {
				fmt.Printf("Finished %s %d / %d bytes (%d%%)\n",
					resp.Filename,
					resp.BytesComplete(),
					resp.Size,
					int(100*resp.Progress()))
			}
			responses[i] = nil
		}
	}

	// print progress for incomplete downloads
	for _, resp := range responses {
		if resp != nil {
			fmt.Printf("Downloading %s %d / %d bytes (%d%%) - %.02fKBp/s ETA: %ds \033[K\n",
				resp.Filename,
				resp.BytesComplete(),
				resp.Size,
				int(100*resp.Progress()),
				resp.BytesPerSecond()/1024,
				int64(resp.ETA().Sub(time.Now()).Seconds()))
		}
	}
}
