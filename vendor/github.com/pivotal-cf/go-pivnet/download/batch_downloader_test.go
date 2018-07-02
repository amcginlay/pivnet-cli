package download_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/go-pivnet/download"
	"github.com/pivotal-cf/go-pivnet/download/fakes"
	"fmt"
	"time"
)

var _ = Describe("WaitForComplete", func() {
	var (
		result download.ErrorDownload
		timeoutDuration time.Duration
	)

	BeforeEach(func() {
		timeoutDuration = 1 * time.Second
	})

	Context("when everything is downloaded", func() {
		BeforeEach(func() {
			channel := make(chan download.IProxyResponse)
			go func() {
				fakeResponse := fakes.ProxyResponse{}
				fakeResponse.ErrStub = func() error {
					return nil
				}
				channel <- &fakeResponse
				close(channel)
			}()
			result = download.WaitForComplete(channel, timeoutDuration)
		})

		It("there are no errors", func() {
			Expect(result.Error).NotTo(HaveOccurred())
		})

		It("cannot retry", func() {
			Expect(result.CanRetry).To(BeFalse())
		})
	})

	Context( "When there is an error in the download", func() {
		BeforeEach(func() {
			channel := make(chan download.IProxyResponse)
			go func() {
				fakeResponse := fakes.ProxyResponse{}
				fakeResponse.ErrStub = func() error {
					return fmt.Errorf("Error")
				}
				channel <- &fakeResponse
				channel <- &fakeResponse
				close(channel)
			}()
			result = download.WaitForComplete(channel, timeoutDuration)
		})

		It("there are errors", func() {
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error).To(MatchError("Error\nError"))
		})

		It("cannot retry", func() {
			Expect(result.CanRetry).To(BeTrue())
		})
	})

	Context( "When the download has timed out", func() {
		var (
			fakeResponse fakes.ProxyResponse
		)

		BeforeEach(func() {
			channel := make(chan download.IProxyResponse)
			timeoutDuration := 250 * time.Millisecond
			fakeResponse = fakes.ProxyResponse{}
			fakeResponse.IsCompleteStub = func() bool {
				return false
			}
			fakeResponse.BytesPerSecondStub = func() float64 {
				return 0
			}
			fakeResponse.FilenameStub = func() string {
				return "some/file/name.txt"
			}
			fakeResponse.DoneStub = func() {
				close(channel)
			}
			fakeResponse.ErrStub = func() error {
				return nil
			}

			go func() {
				channel <- &fakeResponse
			}()
			result = download.WaitForComplete(channel, timeoutDuration)
		})

		It("there are errors", func() {
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error).To(MatchError(fmt.Sprintf("a download timed out for chunk: %s", fakeResponse.Filename())))
		})

		It("cannot retry", func() {
			Expect(result.CanRetry).To(BeFalse())
		})
	})
})
