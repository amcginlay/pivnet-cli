package download_test

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

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
		batchDownloader     *fakes.BatchDownloader
		ranger              *fakes.Ranger
		downloadLinkFetcher *fakes.DownloadLinkFetcher
		logger              logger.Logger
	)

	BeforeEach(func() {
		httpClient = &fakes.HTTPClient{}
		batchDownloader = &fakes.BatchDownloader{}
		ranger = &fakes.Ranger{}
		logger = &loggerfakes.FakeLogger{}

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
			ranges := []download.Range{
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
			}
			ranger.BuildRangeReturns(ranges, nil)

			var receivedRequest *http.Request
			httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
				receivedRequest = req

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

			tmpFile, err := ioutil.TempFile("", "")
			Expect(err).NotTo(HaveOccurred())

			fileNameChunks := download.GetFileChunkNames(tmpFile.Name(), ranges)
			err = ioutil.WriteFile(fileNameChunks[0], []byte("fake produ"), 0644)
			Expect(err).NotTo(HaveOccurred())
			err = ioutil.WriteFile(fileNameChunks[1], []byte("ct content"), 0644)
			Expect(err).NotTo(HaveOccurred())

			downloader := download.Client{
				HTTPClient:     httpClient,
				BatchDownloader: batchDownloader,
				Ranger:         ranger,
				Logger:         logger,
			}

			err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())

			content, err := ioutil.ReadAll(tmpFile)
			Expect(err).NotTo(HaveOccurred())

			Expect(string(content)).To(Equal("fake product content"))

			Expect(ranger.BuildRangeCallCount()).To(Equal(1))
			Expect(ranger.BuildRangeArgsForCall(0)).To(Equal(int64(10)))

			Expect(httpClient.DoCallCount()).To(Equal(1))
			Expect(receivedRequest.Method).To(Equal("HEAD"))
			Expect(receivedRequest.URL.String()).To(Equal("https://example.com/some-file"))
			Expect(receivedRequest.Header.Get("Referer")).To(Equal("https://go-pivnet.network.pivotal.io"))
		})
		Context("when a retryable error occurs", func() {
			It("successfully retries the download", func() {
				ranges := []download.Range{
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
				}
				ranger.BuildRangeReturns(ranges, nil)

				var receivedRequest *http.Request
				httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
					receivedRequest = req

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

				tmpFile, err := ioutil.TempFile("", "")
				Expect(err).NotTo(HaveOccurred())

				fileNameChunks := download.GetFileChunkNames(tmpFile.Name(), ranges)
				err = ioutil.WriteFile(fileNameChunks[0], []byte("fake produ"), 0644)
				Expect(err).NotTo(HaveOccurred())
				err = ioutil.WriteFile(fileNameChunks[1], []byte("ct content"), 0644)
				Expect(err).NotTo(HaveOccurred())

				downloader := download.Client{
					HTTPClient:     httpClient,
					BatchDownloader: batchDownloader,
					Ranger:         ranger,
					Logger:         logger,
				}

				batchDownloaderCallCount := -1
				batchDownloader.DoStub = func(requests ...download.IProxyRequest) download.ErrorDownload {
						errorDownloads := []download.ErrorDownload{{
							CanRetry: true,
							Error: fmt.Errorf("expected error"),
						},
						{
							CanRetry: false,
							Error: nil,
						},
					}
					batchDownloaderCallCount++
					return errorDownloads[batchDownloaderCallCount]
				}

				err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
				Expect(err).NotTo(HaveOccurred())
				content, err := ioutil.ReadAll(tmpFile)
				Expect(err).NotTo(HaveOccurred())

				Expect(batchDownloaderCallCount).To(Equal(1))
				Expect(string(content)).To(Equal("fake product content"))
			})
		})
		Context("when a non-retryable error occurs", func() {
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
						HTTPClient:     httpClient,
						BatchDownloader: batchDownloader,
						Ranger:         ranger,
						Logger:         logger,
					}

					file, err := ioutil.TempFile("", "")
					Expect(err).NotTo(HaveOccurred())

					err = downloader.Get(file, downloadLinkFetcher, GinkgoWriter)
					Expect(err).To(MatchError("file is too big to fit on this drive"))
				})
			})

			Context("when the HEAD request cannot be constructed", func() {
				It("returns an error", func() {
					downloader := download.Client{
						HTTPClient: nil,
						Ranger:     nil,
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
						Logger:		logger,
					}

					err := downloader.Get(nil, downloadLinkFetcher, GinkgoWriter)
					Expect(err).To(MatchError("failed to construct range: failed range build"))
				})
			})

			Context("and the error occurs while downloading", func() {
				It("returns an error", func() {
					ranges := []download.Range{
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
					}
					ranger.BuildRangeReturns(ranges, nil)

					var receivedRequest *http.Request
					httpClient.DoStub = func(req *http.Request) (*http.Response, error) {
						receivedRequest = req

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

					tmpFile, err := ioutil.TempFile("", "")
					Expect(err).NotTo(HaveOccurred())

					downloader := download.Client{
						HTTPClient:     httpClient,
						BatchDownloader: batchDownloader,
						Ranger:         ranger,
						Logger:         logger,
					}

					batchDownloader.DoStub = func(requests ...download.IProxyRequest) download.ErrorDownload {
						return download.ErrorDownload{
							CanRetry: false,
							Error: fmt.Errorf("expected error"),
						}
					}

					err = downloader.Get(tmpFile, downloadLinkFetcher, GinkgoWriter)
					Expect(err).To(MatchError("download failed: Error: expected error"))
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
						HTTPClient:     httpClient,
						BatchDownloader: batchDownloader,
						Ranger:         ranger,
						Logger:         logger,
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
})
