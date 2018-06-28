package download_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf/go-pivnet/download"
	"github.com/cavaliercoder/grab"
	"fmt"
)

var _ = Describe("BatchDownloader", func() {
	Describe("WaitForComplete", func() {
		It("works", func() {
			responseChannel := make(chan(*grab.Response))
			go func(){
				responseChannel <- nil
			}()
			err := download.WaitForComplete(responseChannel)

			Expect(err.Error).NotTo(HaveOccurred())
		})
	})

	Describe("MapToErrorDownload", func() {
		It("maps failed responses", func() {
			goodResponse := download.TestableGrabResponse{
				Request: &grab.Request{
					Filename: "good_file",
				},
				Error: nil,
			}
			failedResponse := download.TestableGrabResponse{
				Request: &grab.Request{
					Filename: "failed_file",
				},
				Error: fmt.Errorf("failed"),
			}

			responses := []download.TestableGrabResponse {goodResponse, failedResponse, failedResponse}
			result := download.MapToErrorDownload(responses)

			Expect(result.Requests).To(HaveLen(2))
			Expect(result.Requests).To(ContainElement(failedResponse.Request))
			Expect(result.Requests).NotTo(ContainElement(goodResponse.Request))
			Expect(result.Error).To(MatchError("failed\nfailed"))
		})
	})
})
