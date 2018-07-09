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
	ShouldRetry bool
	Error       error
	Requests    []IProxyRequest
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
	var failedDownloadRequests []IProxyRequest
	var errstrings []string
	shouldRetry := true

	for _, downloadResponse := range downloadResponses {
		if downloadResponse != nil {
			var errorText string
			if downloadResponse.Err() == nil {
				errorText = "no errors!"
			} else {
				errorText = downloadResponse.Err().Error()
			}
			fmt.Println(fmt.Sprintf("during for loop: DidTimeout: %t Error: %s", downloadResponse.DidTimeout(), errorText))
		}

		if downloadResponse != nil && downloadResponse.Err() != nil {
			failedDownloadRequests = append(failedDownloadRequests, downloadResponse.Request())
			errstrings = append(errstrings, downloadResponse.Err().Error())
			shouldRetry = shouldRetry && downloadResponse.DidTimeout()
		}
	}

	var error error
	if len(errstrings) > 0 {
		error = fmt.Errorf(strings.Join(errstrings, "\n"))
	} else {
		error = nil
		shouldRetry = false
	}

	fmt.Println(fmt.Sprintf("failedDownloadRequests count: %d", len(failedDownloadRequests)))
	fmt.Println(fmt.Sprintf("shouldRetry: %s", shouldRetry))

	return ErrorDownload{
		Requests:    failedDownloadRequests,
		Error:       error,
		ShouldRetry: shouldRetry,
	}
}

func WaitForComplete(channelProxy <-chan IProxyResponse, timeoutDuration time.Duration) ErrorDownload{
	var downloadResponses []IProxyResponse
	var downloadResponseCounters []int
	ticker := time.NewTicker(500 * time.Millisecond)

	defer ticker.Stop()

	for {
		select {
		case downloadResponse := <-channelProxy:
			if downloadResponse != nil {
				// a new response has been received and has started downloading
				downloadResponses = append(downloadResponses, downloadResponse)
				downloadResponseCounters = append(downloadResponseCounters, 0)
			} else {
				fmt.Println("mapping to error download")
				return MapToErrorDownload(downloadResponses)
			}

		case <-ticker.C:
			updateUI(downloadResponses)
			for i, response := range downloadResponses {
				if !response.IsComplete() && response.BytesPerSecond() <= 0 && downloadResponseCounters[i] < 10 {
					downloadResponseCounters[i] ++
					if downloadResponseCounters[i] == 10 {
						fmt.Println(fmt.Sprintf("RESPONSE CLOSED!!! for %d", i))
						response.Done()  //stop download
						response.SetDidTimeout()
					}
				}
			}
		}
	}
}

func updateUI(responses []IProxyResponse) {
	fmt.Println("***************\n\n\n\n\n********************")
	// print newly completed downloads
	for _, resp := range responses {
		if resp != nil && resp.IsComplete() {
			if resp.Err() != nil {
				fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n",
					resp.Request().URL(),
					resp.Err())
			} else {
				fmt.Printf("Finished %s %d / %d bytes (%d%%)\n",
					resp.Filename(),
					resp.BytesComplete(),
					resp.Size(),
					int(100*resp.Progress()))
			}
		}
	}

	// print progress for incomplete downloads
	for _, resp := range responses {
		if resp != nil {
			fmt.Printf("Downloading %s %d / %d bytes (%d%%) - %.02fKBp/s ETA: %ds \033[K\n",
				resp.Filename(),
				resp.BytesComplete(),
				resp.Size(),
				int(100*resp.Progress()),
				resp.BytesPerSecond()/1024,
				int64(resp.ETA().Sub(time.Now()).Seconds()))
		}
	}
}
