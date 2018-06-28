package download

import (
	"github.com/cavaliercoder/grab"
	"fmt"
	"time"
	"os"
	"strings"
)

type BatchDownloader struct {
	DownloadClient *grab.Client
}

type ErrorDownload struct {
	CanRetry bool
	Error error
	Requests []*grab.Request
}

func NewBatchDownloader() *BatchDownloader {
	return &BatchDownloader{DownloadClient: grab.NewClient()}
}


func (c *BatchDownloader) Do (requests ...*grab.Request) ErrorDownload {
	responseChannel := c.DownloadClient.DoBatch(-1, requests...)
	return WaitForComplete(responseChannel)
}

func MapToErrorDownload(downloadResponses []TestableGrabResponse) ErrorDownload {
	var failedDownloadResponses []*grab.Request
	var errstrings []string

	for _, downloadResponse := range downloadResponses {
		if downloadResponse.Error != nil {
			failedDownloadResponses = append(failedDownloadResponses, downloadResponse.Request)
			errstrings = append(errstrings, downloadResponse.Error.Error())
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

type TestableGrabResponse struct {
	Error error
	Request *grab.Request
}

func createTestableGrabResponses(responses []*grab.Response) []TestableGrabResponse {
	var testableResponses []TestableGrabResponse
	for _, response := range responses {
		testableResponses = append(testableResponses, TestableGrabResponse{
			Error: response.Err(),
			Request: response.Request,
		})
	}
	return testableResponses
}

func WaitForComplete(downloadBatchChannel <-chan *grab.Response) ErrorDownload{
	const timeoutDuration = 5 * time.Second
	stalledDownloadChannel := make(chan(*grab.Response))
	var downloadResponses []*grab.Response
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
		case downloadResponse := <-downloadBatchChannel:
			if downloadResponse != nil {
				// a new response has been received and has started downloading
				downloadResponses = append(downloadResponses, downloadResponse)
			} else {
				return MapToErrorDownload(createTestableGrabResponses(downloadResponses))
			}

		case <-t.C:
			// update UI every 200ms
			updateUI(downloadResponses)
			for _, response := range downloadResponses {
				if response != nil && response.BytesPerSecond() > 0 {
					stalledDownloadTimer.Reset(timeoutDuration)
				} else {
					//everything is ok
				}
			}

		case stalledRequest := <-stalledDownloadChannel:
			close(stalledRequest.Done)  //stop download
			return ErrorDownload {
				CanRetry: false,
				Error: fmt.Errorf("a download timed out for chunk: %s", stalledRequest.Filename),
			}
		}
	}
}

func updateUI(responses []*grab.Response) {
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
