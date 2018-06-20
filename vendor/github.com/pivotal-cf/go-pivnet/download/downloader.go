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
	fmt.Fprintln(os.Stderr,"Length of file to downlad: %d\n", resp.ContentLength)
	fmt.Fprintln(os.Stderr,"We made %d chunks:\n", len(ranges))
	for i := 0; i < len(ranges); i++ {
		fmt.Fprintln(os.Stderr,"    %d: %d - %d\n", i, ranges[i].Lower, ranges[i].Upper)
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
	for _, r := range ranges {
		byteRange := r

		fileWriter, err := os.OpenFile(location.Name(), os.O_RDWR, fileInfo.Mode())
		if err != nil {
			return fmt.Errorf("failed to open file for writing: %s", err)
		}

		g.Go(func() error {
			err := c.retryableRequest(contentURL, byteRange.HTTPHeader, fileWriter, byteRange.Lower, downloadLinkFetcher)
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

func (c Client) retryableRequest(contentURL string, rangeHeader http.Header, fileWriter *os.File, startingByte int64, downloadLinkFetcher downloadLinkFetcher) error {
	fmt.Fprintln(os.Stderr,"Entered retryableRequest\n")
	currentURL := contentURL
	defer fileWriter.Close()

	var err error
Retry:
	fmt.Fprintln(os.Stderr,"Began retry block\n")
	_, err = fileWriter.Seek(startingByte, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to correct byte of output file: %s", err)
	}

	fmt.Fprintln(os.Stderr,"Making new GET request\n")
	req, err := http.NewRequest("GET", currentURL, nil)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr,"Finished making GET request\n")

	req.Header = rangeHeader

	fmt.Fprintln(os.Stderr,"About to make a download request\n")
	resp, err := c.HTTPClient.Do(req)

	if err != nil {
		if netErr, ok := err.(net.Error); ok {
			if netErr.Temporary() {
				fmt.Fprintln(os.Stderr,"Failed making download request, goto RETRY\n")

				goto Retry
			}
		}

		return fmt.Errorf("download request failed: %s", err)
	}

	defer resp.Body.Close()

	fmt.Fprintln(os.Stderr,"Succeeded making download request\n")
	if resp.StatusCode == http.StatusForbidden {
		fmt.Fprintln(os.Stderr,"Request 404'd or something, trying to make new download link\n")

		c.Logger.Debug("received unsuccessful status code: %d", logger.Data{"statusCode": resp.StatusCode})
		currentURL, err = downloadLinkFetcher.NewDownloadLink()
		if err != nil {
			return err
		}
		c.Logger.Debug("fetched new download url: %d", logger.Data{"url": currentURL})

		fmt.Fprintln(os.Stderr,"Made new download link, goto RETRY\n")

		goto Retry
	}

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("during GET unexpected status code was returned: %d", resp.StatusCode)
	}

	fmt.Fprintln(os.Stderr,"About to read/write content\n")

	var proxyReader io.Reader
	proxyReader = c.Bar.NewProxyReader(resp.Body)

	bytesWritten, err := io.Copy(fileWriter, proxyReader)
	if err != nil {
		fmt.Fprintln(os.Stderr,"Failed to write content\n")

		if err == io.ErrUnexpectedEOF {
			c.Bar.Add(int(-1 * bytesWritten))
			fmt.Fprintln(os.Stderr,"Found unexpected EOF, goto RETRY\n")

			goto Retry
		}
		oe, _ := err.(*net.OpError)
		if strings.Contains(oe.Err.Error(), syscall.ECONNRESET.Error()) {
			c.Bar.Add(int(-1 * bytesWritten))
			fmt.Fprintln(os.Stderr,"Found some other weird error like ECONNRESET or w/e, goto RETRY\n")

			goto Retry
		}
		return fmt.Errorf("failed to write file during io.Copy: %s", err)
	}

	return nil
}
