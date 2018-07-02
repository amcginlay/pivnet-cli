package download

import (
	"fmt"
	"github.com/pivotal-cf/go-pivnet/logger"
	"io"
	"net/http"
	"os"
	"github.com/shirou/gopsutil/disk"
	"github.com/cavaliercoder/grab"
)

//go:generate counterfeiter -o ./fakes/ranger.go --fake-name Ranger . ranger
type ranger interface {
	BuildRange(contentLength int64) ([]Range, error)
}

//go:generate counterfeiter -o ./fakes/http_client.go --fake-name HTTPClient . httpClient
type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

//go:generate counterfeiter -o ./fakes/batch_downloader.go --fake-name BatchDownloader . batchDownloader
type batchDownloader interface {
	Do (...IProxyRequest) ErrorDownload
}

type downloadLinkFetcher interface {
	NewDownloadLink() (string, error)
}

type Client struct {
	HTTPClient httpClient
	BatchDownloader batchDownloader
	Ranger     ranger
	Logger     logger.Logger
}

func GetFileChunkNames(location string, ranges []Range) []string {
	var list []string
	for _, r := range ranges {
		fileName := fmt.Sprintf("%s_%d", location, r.Lower)
		list = append(list, fileName)
	}
	return list
}

func CleanupFileChunks(fileChunkNames []string) error {
	for _, fileChunkName := range fileChunkNames {
		err := os.Remove(fileChunkName)
		if err != nil {
			return err
		}
	}
	return nil
}

func CombineFileChunks(fileWriter *os.File, fileChunkNames []string) error {
	fileInfo, err := fileWriter.Stat()
	if err != nil {
		return fmt.Errorf("failed to read information from output file: %s", err)
	}
	file, err := os.OpenFile(fileWriter.Name(), os.O_RDWR, fileInfo.Mode())
	if err != nil {
		return fmt.Errorf("failed to open file for writing: %s", err)
	}

	for _, fileChunkName := range fileChunkNames {
		fileChunk, err := os.Open(fileChunkName)
		if err != nil {
			return err
		}

		_, err = io.Copy(file, fileChunk)
		errClose := fileChunk.Close()
		if err != nil {
			return err
		}
		if errClose != nil {
			return errClose
		}
	}
	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

func (c Client) Get(
	location *os.File,
	downloadLinkFetcher downloadLinkFetcher,
	progressWriter io.Writer,
) error {
	contentURL, err := downloadLinkFetcher.NewDownloadLink()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("HEAD", contentURL, nil)
	if err != nil {
		return fmt.Errorf("failed to construct HEAD request: %s", err)
	}

	req.Header.Add("Referer","https://go-pivnet.network.pivotal.io")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make HEAD request: %s", err)
	}

	contentURL = resp.Request.URL.String()

	ranges, err := c.Ranger.BuildRange(resp.ContentLength)
	if err != nil {
		return fmt.Errorf("failed to construct range: %s", err)
	}

	diskStats, err := disk.Usage(location.Name())
	if err != nil {
		return fmt.Errorf("failed to get disk free space: %s", err)
	}

	if diskStats.Free < uint64(resp.ContentLength) {
		return fmt.Errorf("file is too big to fit on this drive")
	}

	fileNameChunks := GetFileChunkNames(location.Name(), ranges)
	requests, err := GetRequests(contentURL, fileNameChunks, ranges)

	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}

	err = performDownload(c.BatchDownloader, requests...)
	if err != nil {
		return fmt.Errorf("download failed: %s", err)
	}

	c.Logger.Debug(fmt.Sprintf("assembling chunks"))
	if err := CombineFileChunks(location, fileNameChunks); err != nil {
		return fmt.Errorf("failed to combine file chunks: %s", err)
	}

	c.Logger.Debug(fmt.Sprintf("cleaning up chunks"))
	if err := CleanupFileChunks(fileNameChunks); err != nil {
		return fmt.Errorf("failed to cleanup file chunks: %s", err)
	}

	return nil
}

func GetRequests(contentURL string, fileNameChunks []string, ranges []Range) ([]IProxyRequest, error) {
	var requests []IProxyRequest

	for i, r := range ranges {
		request, err := grab.NewRequest(fileNameChunks[i], contentURL)

		if err != nil {
			return nil, err
		}
		request.HTTPRequest.Header = r.HTTPHeader
		request.HTTPRequest.Header.Add("Referer", "https://go-pivnet.network.pivotal.io")
		requests = append(requests, NewProxyRequest(request))
	}
	return requests, nil
}

func performDownload(batchDownloader batchDownloader, requests ...IProxyRequest) error {
	errorDownload := batchDownloader.Do(requests...)
	if errorDownload.Error != nil {
		if errorDownload.CanRetry { //try one more time
			errorDownload = batchDownloader.Do(errorDownload.Requests...)
			if errorDownload.Error != nil {
				fmt.Errorf("retry failed: %s", errorDownload.Error)
			}
		} else {
			return fmt.Errorf("first failed: %s", errorDownload.Error)
		}
	}
	return nil
}
