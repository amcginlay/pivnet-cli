package download

import (
	"net/http/httptrace"
	"fmt"
	"github.com/pivotal-cf/go-pivnet/logger"
	"golang.org/x/sync/errgroup"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"github.com/shirou/gopsutil/disk"
)

//go:generate counterfeiter -o ./fakes/ranger.go --fake-name Ranger . ranger
type ranger interface {
	BuildRange(contentLength int64) ([]Range, error)
}

//go:generate counterfeiter -o ./fakes/http_client.go --fake-name HTTPClient . httpClient
type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type downloadLinkFetcher interface {
	NewDownloadLink() (string, error)
}

//go:generate counterfeiter -o ./fakes/bar.go --fake-name Bar . bar
type bar interface {
	SetTotal(contentLength int64)
	SetOutput(output io.Writer)
	Add(totalWritten int) int
	Kickoff()
	Finish()
	NewProxyReader(reader io.Reader) io.Reader
}

type Client struct {
	HTTPClient httpClient
	Ranger     ranger
	Bar        bar
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

	c.Bar.SetOutput(progressWriter)
	c.Bar.SetTotal(resp.ContentLength)
	c.Bar.Kickoff()

	defer c.Bar.Finish()

	var g errgroup.Group
	fileNameChunks := GetFileChunkNames(location.Name(), ranges)

	for i, r := range ranges {
		byteRange := r
		fileName := fileNameChunks[i]

		g.Go(func() error {
			fileWriter, err := os.Create(fileName)

			if err != nil {
				return fmt.Errorf("failed to open file for writing: %s %s", err, fileName)
			}
			defer fileWriter.Close()
			err = c.retryableRequest(contentURL, byteRange.HTTPHeader, fileWriter, byteRange.Lower, downloadLinkFetcher)
			if err != nil {
				return fmt.Errorf("failed during retryable request: %s", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
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

func newTrace(logger logger.Logger, startingByte int64) *httptrace.ClientTrace {
	trace := &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			logger.Debug(fmt.Sprintf("Got Conn %d", startingByte))
		},
		ConnectStart: func(network, addr string) {
			logger.Debug(fmt.Sprintf("Dial start %d", startingByte))
		},
		ConnectDone: func(network, addr string, err error) {
			logger.Debug(fmt.Sprintf("Dial done %d", startingByte))
		},
		GotFirstResponseByte: func() {
			logger.Debug(fmt.Sprintf("First response byte! %d", startingByte))
		},
		WroteHeaders: func() {
			logger.Debug(fmt.Sprintf("Wrote headers %d", startingByte))
		},
		WroteRequest: func(wr httptrace.WroteRequestInfo) {
			logger.Debug(fmt.Sprintf("Wrote request %d %v", startingByte, wr))
		},
		PutIdleConn: func(err error) {
			logger.Debug(fmt.Sprintf("PutIdleConn %d %s", startingByte, err))
		},
	}
	return trace
}

func (c Client) retryableRequest(contentURL string, rangeHeader http.Header, fileWriter *os.File, startingByte int64, downloadLinkFetcher downloadLinkFetcher) error {
	currentURL := contentURL

	var err error
Retry:
	_, err = fileWriter.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to correct byte of output file: %s", err)
	}

	c.Logger.Debug(fmt.Sprintf("startingByte: %d - Making new GET request", startingByte))
	req, err := http.NewRequest("GET", currentURL, nil)
	if err != nil {
		return err
	}
	trace := newTrace(c.Logger, startingByte)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	c.Logger.Debug(fmt.Sprintf("startingByte: %d - Finished making GET request", startingByte))

	rangeHeader.Add("Referer", "https://go-pivnet.network.pivotal.io")
	req.Header = rangeHeader

	c.Logger.Debug(fmt.Sprintf("startingByte: %d - About to make a download request", startingByte))
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		if netErr, ok := err.(net.Error); ok {
			if netErr.Temporary() {
				c.Logger.Debug(fmt.Sprintf("startingByte: %d - Failed making download request, goto RETRY", startingByte))
				goto Retry
			}
		}

		return fmt.Errorf("download request failed: %s", err)
	}

	defer resp.Body.Close()

	c.Logger.Debug(fmt.Sprintf("startingByte: %d - Succeeded making download request", startingByte))
	if resp.StatusCode == http.StatusForbidden {
		c.Logger.Debug(fmt.Sprintf("startingByte: %d - Request 404'd or something, trying to make new download link", startingByte))
		c.Logger.Debug("received unsuccessful status code: %d", logger.Data{"statusCode": resp.StatusCode})
		currentURL, err = downloadLinkFetcher.NewDownloadLink()
		if err != nil {
			return err
		}
		c.Logger.Debug("fetched new download url: %d", logger.Data{"url": currentURL})

		c.Logger.Debug(fmt.Sprintf("startingByte: %d - Made new download link, goto RETRY", startingByte))
		goto Retry
	}

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("during GET unexpected status code was returned: %d", resp.StatusCode)
	}

	c.Logger.Debug(fmt.Sprintf("startingByte: %d - About to read/write content", startingByte))
	var proxyReader io.Reader
	proxyReader = c.Bar.NewProxyReader(resp.Body)

	bytesWritten, err := io.Copy(fileWriter, proxyReader)
	if err != nil {
		c.Logger.Debug(fmt.Sprintf("startingByte: %d - Failed to write content", startingByte))
		if err == io.ErrUnexpectedEOF {
			c.Bar.Add(int(-1 * bytesWritten))
			c.Logger.Debug(fmt.Sprintf("startingByte: %d - Found unexpected EOF, goto RETRY", startingByte))
			goto Retry
		}
		oe, _ := err.(*net.OpError)
		if strings.Contains(oe.Err.Error(), syscall.ECONNRESET.Error()) {
			c.Bar.Add(int(-1 * bytesWritten))
			c.Logger.Debug(fmt.Sprintf("startingByte: %d - Found some other weird error like ECONNRESET or w/e, goto RETRY", startingByte))
			goto Retry
		}
		return fmt.Errorf("failed to write file during io.Copy: %s", err)
	}

	c.Logger.Debug(fmt.Sprintf("\n\nstartingByte: %d - SUCCESSFULLY COMPLETED", startingByte))
	return nil
}
