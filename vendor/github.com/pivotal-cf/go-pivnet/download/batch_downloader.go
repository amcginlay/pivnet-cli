package download

import (
	"fmt"
	"time"
	"os"
	"strings"
	"github.com/cavaliercoder/grab"
)

type BatchDownloader struct {
	DownloadClient *grab.Client
}

type ErrorDownload struct {
	CanRetry bool
	Error error
	Requests []IProxyRequest
}

func NewBatchDownloader() *BatchDownloader {
	return &BatchDownloader{DownloadClient: grab.NewClient()}
}

func (c *BatchDownloader) Do (requests ...IProxyRequest) ErrorDownload {
	const timeoutDuration = 5 * time.Second
	var realGrabRequests []*grab.Request
	for _, request := range requests {
		realGrabRequests = append(realGrabRequests, request.Wrapped())
	}

	responseChannel := c.DownloadClient.DoBatch(-1, realGrabRequests...)
	proxyResponseChannel := NewChannelProxy(responseChannel)

	return WaitForComplete(proxyResponseChannel.Channel, timeoutDuration)
}

func MapToErrorDownload(downloadResponses []IProxyResponse) ErrorDownload {
	var failedDownloadResponses []IProxyRequest
	var errstrings []string

	for _, downloadResponse := range downloadResponses {
		if downloadResponse != nil && downloadResponse.Err() != nil {
			failedDownloadResponses = append(failedDownloadResponses, downloadResponse.Request())
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
		CanRetry: error != nil,
	}
}

func WaitForComplete(channelProxy <-chan IProxyResponse, timeoutDuration time.Duration) ErrorDownload{
	stalledDownloadChannel := make(chan IProxyResponse)
	var downloadResponses []IProxyResponse
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
				Error: fmt.Errorf("a download timed out for chunk: %s", stalledResponse.Filename()),
			}
		}
	}
}

func updateUI(responses []IProxyResponse) {
	fmt.Println("***************\n\n\n\n\n********************")
	// print newly completed downloads
	for i, resp := range responses {
		if resp != nil && resp.IsComplete() {
			if resp.Err() != nil {
				fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n",
					resp.Request().URL(),
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
