package download_test

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/pivotal-cf/go-pivnet/download"
	"github.com/pivotal-cf/go-pivnet/download/fakes"

	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net"
	"syscall"
	"math"
	"os"
	"github.com/pivotal-cf/go-pivnet/logger"
	"github.com/pivotal-cf/go-pivnet/logger/loggerfakes"
)

type EOFReader struct{}

func (e EOFReader) Read(p []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

type ConnectionResetReader struct{}

func (e ConnectionResetReader) Read(p []byte) (int, error) {
	return 0, &net.OpError{Err: fmt.Errorf(syscall.ECONNRESET.Error())}
}

type NetError struct {
	error
}

func (ne NetError) Temporary() bool {
	return true
}

func (ne NetError) Timeout() bool {
	return true
}

var _ = Describe("Downloader", func() {
	var (
		httpClient          *fakes.HTTPClient
		ranger              *fakes.Ranger
		bar                 *fakes.Bar
		downloadLinkFetcher *fakes.DownloadLinkFetcher
		logger				logger.Logger
	)

	BeforeEach(func() {
		httpClient = &fakes.HTTPClient{}
		ranger = &fakes.Ranger{}
		bar = &fakes.Bar{}
		logger = &loggerfakes.FakeLogger{}

		bar.NewProxyReaderStub = func(reader io.Reader) io.Reader { return reader }

		downloadLinkFetcher = &fakes.DownloadLinkFetcher{}
		downloadLinkFetcher.NewDownloadLinkStub = func() (string, error) {
			return "https://example.com/some-file", nil
		}
	})

	Describe("GetFileChunkNames", func() {
		It("returns an array of file names", func() {
			location := "location"
			ranges, _ := download.NewRanger(2).BuildRange(2)

			result := download.GetFileChunkNames(location, ranges)

			Expect(len(result)).To(Equal(len(ranges)))
			Expect(result[0]).To(Equal(fmt.Sprintf("%s_%d", location, 0)))
			Expect(result[1]).To(Equal(fmt.Sprintf("%s_%d", location, 1)))
		})
	})

	Describe("CombineFileChunks", func() {
		It("puts files back together", func() {
			var err error
			file, err := ioutil.TempFile("", "")
			Expect(err).NotTo(HaveOccurred())

			fileA, err := ioutil.TempFile("", "")
			Expect(err).NotTo(HaveOccurred())
			fileB, err := ioutil.TempFile("", "")
			Expect(err).NotTo(HaveOccurred())
			fileC, err := ioutil.TempFile("", "")
			Expect(err).NotTo(HaveOccurred())

			_, err = fileA.WriteString("A")
			Expect(err).NotTo(HaveOccurred())
			_, err = fileB.WriteString("B")
			Expect(err).NotTo(HaveOccurred())
			_, err = fileB.WriteString("C")
			Expect(err).NotTo(HaveOccurred())

			err = fileA.Close()
			Expect(err).NotTo(HaveOccurred())
			err = fileB.Close()
			Expect(err).NotTo(HaveOccurred())
			err = fileC.Close()
			Expect(err).NotTo(HaveOccurred())

			err = download.CombineFileChunks(file, []string{fileA.Name(), fileB.Name(), fileC.Name()})
			Expect(err).NotTo(HaveOccurred())

			result, err := ioutil.ReadFile(file.Name())
			Expect(err).NotTo(HaveOccurred())

			Expect(result).To(Equal([]byte("ABC")))
		})
	})

	Describe("CleanupChunkFiles", func() {
		It("deletes all the files", func() {
			var err error
			fileChunk1, err := ioutil.TempFile("", "file1")
			Expect(err).NotTo(HaveOccurred())
			fileChunk2, err := ioutil.TempFile("", "file2")
			Expect(err).NotTo(HaveOccurred())
			fileChunk3, err := ioutil.TempFile("", "file3")
			Expect(err).NotTo(HaveOccurred())

			fileChunkNames := []string{ fileChunk1.Name(), fileChunk2.Name(), fileChunk3.Name() }
			err = download.CleanupFileChunks(fileChunkNames)
			Expect(err).NotTo(HaveOccurred())

			_, err = os.OpenFile(fileChunk1.Name(), os.O_RDWR, 0)
			Expect(err).To(HaveOccurred())
			_, err = os.OpenFile(fileChunk2.Name(), os.O_RDWR, 0)
			Expect(err).To(HaveOccurred())
			_, err = os.OpenFile(fileChunk3.Name(), os.O_RDWR, 0)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Get", func() {
		It("writes the product to the given location", func() {
			ranger.BuildRangeReturns([]download.Range{
				download.NewRange(
					0,
					9,
					http.Header{"Range": []string{"bytes=0-9"}},
			),
				download.NewRange(
					10,
					19,
					http.Header{"Range": []string{"bytes=10-19"}},
			),
			}, nil)

			var receivedRequests []*http.Request
			var m = &sync.Mutex{}
			httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
				m.Lock()
				receivedRequests = append(receivedRequests, req)
				m.Unlock()

				switch req.Header.Get("Range") {
				case "bytes=0-9":
					return &http.Response{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(strings.NewReader("fake produ")),
					}, nil
				case "bytes=10-19":
					return &http.Response{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(strings.NewReader("ct content")),
					}, nil
				default:
					return &http.Response{
						StatusCode:    http.StatusOK,
						ContentLength: 10,
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					}, nil
				}
			}

			downloader := download.Client{
				HTTPClient: httpClient,
				Ranger:     ranger,
				Bar:        bar,
				Logger:		logger,
			}

			tmpFile, err := ioutil.TempFile("", "")
			Expect(err).NotTo(HaveOccurred())

			err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())

			content, err := ioutil.ReadAll(tmpFile)
			Expect(err).NotTo(HaveOccurred())

			Expect(string(content)).To(Equal("fake product content"))

			Expect(ranger.BuildRangeCallCount()).To(Equal(1))
			Expect(ranger.BuildRangeArgsForCall(0)).To(Equal(int64(10)))

			Expect(bar.SetTotalArgsForCall(0)).To(Equal(int64(10)))
			Expect(bar.KickoffCallCount()).To(Equal(1))

			Expect(httpClient.DoCallCount()).To(Equal(3))

			methods := []string{
				receivedRequests[0].Method,
				receivedRequests[1].Method,
				receivedRequests[2].Method,
			}
			urls := []string{
				receivedRequests[0].URL.String(),
				receivedRequests[1].URL.String(),
				receivedRequests[2].URL.String(),
			}
			headers := []string{
				receivedRequests[1].Header.Get("Range"),
				receivedRequests[2].Header.Get("Range"),
			}
			refererHeaders := []string {
				receivedRequests[0].Header.Get("Referer"),
				receivedRequests[1].Header.Get("Referer"),
				receivedRequests[2].Header.Get("Referer"),
			}

			Expect(methods).To(ConsistOf([]string{"HEAD", "GET", "GET"}))
			Expect(urls).To(ConsistOf([]string{"https://example.com/some-file", "https://example.com/some-file", "https://example.com/some-file"}))
			Expect(headers).To(ConsistOf([]string{"bytes=0-9", "bytes=10-19"}))
			Expect(refererHeaders).To(ConsistOf([]string{
				"https://go-pivnet.network.pivotal.io",
				"https://go-pivnet.network.pivotal.io",
				"https://go-pivnet.network.pivotal.io",
				}))

			Expect(bar.FinishCallCount()).To(Equal(1))
		})
	})

	Context("when a retryable error occurs", func() {
		Context("when there is an unexpected EOF", func() {
			It("successfully retries the download", func() {
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					},
					{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(io.MultiReader(strings.NewReader("some"), EOFReader{})),
					},
					{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(strings.NewReader("something")),
					},
				}
				errors := []error{nil, nil, nil}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}

				ranger.BuildRangeReturns([]download.Range{download.NewRange(0,15, http.Header{})}, nil)

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				tmpFile, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
				Expect(err).NotTo(HaveOccurred())

				stats, err := tmpFile.Stat()
				Expect(err).NotTo(HaveOccurred())

				Expect(stats.Size()).To(BeNumerically(">", 0))
				Expect(bar.AddArgsForCall(0)).To(Equal(-4))

				content, err := ioutil.ReadAll(tmpFile)
				Expect(err).NotTo(HaveOccurred())

				Expect(string(content)).To(Equal("something"))
			})
		})

		Context("when there is a temporary network error", func() {
			It("successfully retries the download", func() {
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					},
					{
						StatusCode: http.StatusPartialContent,
					},
					{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(strings.NewReader("something")),
					},
				}
				errors := []error{nil, NetError{errors.New("whoops")}, nil}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}

				ranger.BuildRangeReturns([]download.Range{download.NewRange(0,15, http.Header{})}, nil)

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				tmpFile, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
				Expect(err).NotTo(HaveOccurred())

				stats, err := tmpFile.Stat()
				Expect(err).NotTo(HaveOccurred())

				Expect(stats.Size()).To(BeNumerically(">", 0))
			})
		})

		Context("when the connection is reset", func() {
			It("successfully retries the download", func() {
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					},
					{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(io.MultiReader(strings.NewReader("some"), ConnectionResetReader{})),
					},
					{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(strings.NewReader("something")),
					},
				}

				errors := []error{nil, nil, nil}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}

				ranger.BuildRangeReturns([]download.Range{download.NewRange(0,15, http.Header{})}, nil)

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				tmpFile, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
				Expect(err).NotTo(HaveOccurred())

				stats, err := tmpFile.Stat()
				Expect(err).NotTo(HaveOccurred())

				Expect(stats.Size()).To(BeNumerically(">", 0))
				Expect(bar.AddArgsForCall(0)).To(Equal(-4))

				content, err := ioutil.ReadAll(tmpFile)
				Expect(err).NotTo(HaveOccurred())

				Expect(string(content)).To(Equal("something"))
			})
		})
	})

	Context("when an error occurs", func() {
		Context("when the disk is out of memory", func() {
			It("returns an error", func() {
				tooBig := int64(math.MaxInt64)
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
						ContentLength: tooBig,
					},
				}
				errors := []error{nil, nil}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}


				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				file, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(file, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError("file is too big to fit on this drive"))
			})
		})

		Context("when the HEAD request cannot be constucted", func() {
			It("returns an error", func() {
				downloader := download.Client{
					HTTPClient: nil,
					Ranger:     nil,
					Bar:        nil,
					Logger:		logger,
				}
				downloadLinkFetcher.NewDownloadLinkStub = func() (string, error) {
					return "%%%", nil
				}

				err := downloader.Get(nil, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError(ContainSubstring("failed to construct HEAD request")))
			})
		})

		Context("when the HEAD has an error", func() {
			It("returns an error", func() {
				httpClient.DoReturns(&http.Response{}, errors.New("failed request"))

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     nil,
					Bar:        nil,
					Logger:		logger,
				}

				err := downloader.Get(nil, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError("failed to make HEAD request: failed request"))
			})
		})

		Context("when building a range fails", func() {
			It("returns an error", func() {
				httpClient.DoReturns(&http.Response{Request: &http.Request{
					URL: &url.URL{
						Scheme: "https",
						Host:   "example.com",
						Path:   "some-file",
					},
				},
				}, nil)

				ranger.BuildRangeReturns([]download.Range{}, errors.New("failed range build"))

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        nil,
					Logger:		logger,
				}

				err := downloader.Get(nil, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError("failed to construct range: failed range build"))
			})
		})

		Context("when the GET fails", func() {
			It("returns an error", func() {
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					},
					{},
				}
				errors := []error{nil, errors.New("failed GET")}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}

				ranger.BuildRangeReturns([]download.Range{download.NewRange(0, 0, http.Header{})}, nil)

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				file, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(file, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError("download failed: failed during retryable request: download request failed: failed GET"))
			})
		})

		Context("when the GET returns a non-206", func() {
			It("returns an error", func() {
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					},
					{
						StatusCode: http.StatusInternalServerError,
						Body:       ioutil.NopCloser(strings.NewReader("")),
					},
				}
				errors := []error{nil, nil}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}

				ranger.BuildRangeReturns([]download.Range{download.NewRange(0, 0, http.Header{})}, nil)

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				file, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(file, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError("download failed: failed during retryable request: during GET unexpected status code was returned: 500"))
			})
		})

		Context("when the file cannot be written to", func() {
			It("returns an error", func() {
				responses := []*http.Response{
					{
						Request: &http.Request{
							URL: &url.URL{
								Scheme: "https",
								Host:   "example.com",
								Path:   "some-file",
							},
						},
					},
					{
						StatusCode: http.StatusPartialContent,
						Body:       ioutil.NopCloser(strings.NewReader("something")),
					},
				}
				errors := []error{nil, nil}

				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					count := httpClient.DoCallCount() - 1
					return responses[count], errors[count]
				}

				ranger.BuildRangeReturns([]download.Range{download.NewRange(0, 15, http.Header{})}, nil)

				downloader := download.Client{
					HTTPClient: httpClient,
					Ranger:     ranger,
					Bar:        bar,
					Logger:		logger,
				}

				closedFile, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				err = closedFile.Close()
				Expect(err).NotTo(HaveOccurred())

				err = downloader.Get(closedFile, downloadLinkFetcher, GinkgoWriter)
				Expect(err).To(MatchError(ContainSubstring("failed to read information from output file")))
			})
		})
	})
})
