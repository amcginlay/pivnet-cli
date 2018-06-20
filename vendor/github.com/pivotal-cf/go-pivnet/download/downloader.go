package download

import (
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

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make HEAD request: %s", err)
	}

	contentURL = resp.Request.URL.String()

	ranges, err := c.Ranger.BuildRange(resp.ContentLength)
	if err != nil {
		return fmt.Errorf("failed to construct range: %s", err)
	}
	fmt.Fprintln(os.Stderr, fmt.Sprintf("Length of file to downlad: %d", resp.ContentLength))
	fmt.Fprintln(os.Stderr, fmt.Sprintf("We made %d chunks:", len(ranges)))
	for i := 0; i < len(ranges); i++ {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("    %d: %d - %d", i, ranges[i].Lower, ranges[i].Upper))
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
	fileInfo, err := location.Stat()
	if err != nil {
		return fmt.Errorf("failed to read information from output file: %s", err)
	}

	var g errgroup.Group
	for i, r := range ranges {
		byteRange := r

		fileWriter, err := os.OpenFile(location.Name(), os.O_RDWR, fileInfo.Mode())
		if err != nil {
			return fmt.Errorf("failed to open file for writing: %s", err)
		}

		g.Go(func() error {
			err = c.retryableRequest(contentURL, byteRange.HTTPHeader, fileWriter, byteRange.Lower, downloadLinkFetcher, i)
			if err != nil {
				return fmt.Errorf("failed during retryable request: %s", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (c Client) retryableRequest(contentURL string, rangeHeader http.Header, fileWriter *os.File, startingByte int64, downloadLinkFetcher downloadLinkFetcher, chunkIndex int) error {
	fmt.Fprintln(os.Stderr, fmt.Sprintf("\nchunkIndex: %d - Entered retryableRequest", chunkIndex))
	currentURL := contentURL
	defer fileWriter.Close()

	var err error
Retry:
	fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Began retry block", chunkIndex))
	_, err = fileWriter.Seek(startingByte, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to correct byte of output file: %s", err)
	}

	fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Making new GET request", chunkIndex))
	req, err := http.NewRequest("GET", currentURL, nil)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Finished making GET request", chunkIndex))

	req.Header = rangeHeader

	fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - About to make a download request", chunkIndex))
	resp, err := c.HTTPClient.Do(req)

	if err != nil {
		if netErr, ok := err.(net.Error); ok {
			if netErr.Temporary() {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Failed making download request, goto RETRY", chunkIndex))

				goto Retry
			}
		}

		return fmt.Errorf("download request failed: %s", err)
	}

	defer resp.Body.Close()

	fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Succeeded making download request", chunkIndex))
	if resp.StatusCode == http.StatusForbidden {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Request 404'd or something, trying to make new download link", chunkIndex))

		c.Logger.Debug("received unsuccessful status code: %d", logger.Data{"statusCode": resp.StatusCode})
		currentURL, err = downloadLinkFetcher.NewDownloadLink()
		if err != nil {
			return err
		}
		c.Logger.Debug("fetched new download url: %d", logger.Data{"url": currentURL})

		fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Made new download link, goto RETRY", chunkIndex))

		goto Retry
	}

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("during GET unexpected status code was returned: %d", resp.StatusCode)
	}

	fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - About to read/write content", chunkIndex))

	var proxyReader io.Reader
	proxyReader = c.Bar.NewProxyReader(resp.Body)

	bytesWritten, err := io.Copy(fileWriter, proxyReader)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Failed to write content", chunkIndex))

		if err == io.ErrUnexpectedEOF {
			c.Bar.Add(int(-1 * bytesWritten))
			fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Found unexpected EOF, goto RETRY", chunkIndex))

			goto Retry
		}
		oe, _ := err.(*net.OpError)
		if strings.Contains(oe.Err.Error(), syscall.ECONNRESET.Error()) {
			c.Bar.Add(int(-1 * bytesWritten))
			fmt.Fprintln(os.Stderr, fmt.Sprintf("chunkIndex: %d - Found some other weird error like ECONNRESET or w/e, goto RETRY", chunkIndex))

			goto Retry
		}
		return fmt.Errorf("failed to write file during io.Copy: %s", err)
	}

	return nil
}
