// This file was generated by counterfeiter
package releasefakes

import (
	"sync"

	go_pivnet "github.com/pivotal-cf/go-pivnet"
	"github.com/pivotal-cf/pivnet-cli/commands/release"
)

type FakePivnetClient struct {
	ReleasesForProductSlugStub        func(productSlug string) ([]go_pivnet.Release, error)
	releasesForProductSlugMutex       sync.RWMutex
	releasesForProductSlugArgsForCall []struct {
		productSlug string
	}
	releasesForProductSlugReturns struct {
		result1 []go_pivnet.Release
		result2 error
	}
	ReleaseForVersionStub        func(productSlug string, releaseVersion string) (go_pivnet.Release, error)
	releaseForVersionMutex       sync.RWMutex
	releaseForVersionArgsForCall []struct {
		productSlug    string
		releaseVersion string
	}
	releaseForVersionReturns struct {
		result1 go_pivnet.Release
		result2 error
	}
	CreateReleaseStub        func(config go_pivnet.CreateReleaseConfig) (go_pivnet.Release, error)
	createReleaseMutex       sync.RWMutex
	createReleaseArgsForCall []struct {
		config go_pivnet.CreateReleaseConfig
	}
	createReleaseReturns struct {
		result1 go_pivnet.Release
		result2 error
	}
	DeleteReleaseStub        func(productSlug string, release go_pivnet.Release) error
	deleteReleaseMutex       sync.RWMutex
	deleteReleaseArgsForCall []struct {
		productSlug string
		release     go_pivnet.Release
	}
	deleteReleaseReturns struct {
		result1 error
	}
	ReleaseFingerprintStub        func(productSlug string, releaseID int) (string, error)
	releaseFingerprintMutex       sync.RWMutex
	releaseFingerprintArgsForCall []struct {
		productSlug string
		releaseID   int
	}
	releaseFingerprintReturns struct {
		result1 string
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakePivnetClient) ReleasesForProductSlug(productSlug string) ([]go_pivnet.Release, error) {
	fake.releasesForProductSlugMutex.Lock()
	fake.releasesForProductSlugArgsForCall = append(fake.releasesForProductSlugArgsForCall, struct {
		productSlug string
	}{productSlug})
	fake.recordInvocation("ReleasesForProductSlug", []interface{}{productSlug})
	fake.releasesForProductSlugMutex.Unlock()
	if fake.ReleasesForProductSlugStub != nil {
		return fake.ReleasesForProductSlugStub(productSlug)
	} else {
		return fake.releasesForProductSlugReturns.result1, fake.releasesForProductSlugReturns.result2
	}
}

func (fake *FakePivnetClient) ReleasesForProductSlugCallCount() int {
	fake.releasesForProductSlugMutex.RLock()
	defer fake.releasesForProductSlugMutex.RUnlock()
	return len(fake.releasesForProductSlugArgsForCall)
}

func (fake *FakePivnetClient) ReleasesForProductSlugArgsForCall(i int) string {
	fake.releasesForProductSlugMutex.RLock()
	defer fake.releasesForProductSlugMutex.RUnlock()
	return fake.releasesForProductSlugArgsForCall[i].productSlug
}

func (fake *FakePivnetClient) ReleasesForProductSlugReturns(result1 []go_pivnet.Release, result2 error) {
	fake.ReleasesForProductSlugStub = nil
	fake.releasesForProductSlugReturns = struct {
		result1 []go_pivnet.Release
		result2 error
	}{result1, result2}
}

func (fake *FakePivnetClient) ReleaseForVersion(productSlug string, releaseVersion string) (go_pivnet.Release, error) {
	fake.releaseForVersionMutex.Lock()
	fake.releaseForVersionArgsForCall = append(fake.releaseForVersionArgsForCall, struct {
		productSlug    string
		releaseVersion string
	}{productSlug, releaseVersion})
	fake.recordInvocation("ReleaseForVersion", []interface{}{productSlug, releaseVersion})
	fake.releaseForVersionMutex.Unlock()
	if fake.ReleaseForVersionStub != nil {
		return fake.ReleaseForVersionStub(productSlug, releaseVersion)
	} else {
		return fake.releaseForVersionReturns.result1, fake.releaseForVersionReturns.result2
	}
}

func (fake *FakePivnetClient) ReleaseForVersionCallCount() int {
	fake.releaseForVersionMutex.RLock()
	defer fake.releaseForVersionMutex.RUnlock()
	return len(fake.releaseForVersionArgsForCall)
}

func (fake *FakePivnetClient) ReleaseForVersionArgsForCall(i int) (string, string) {
	fake.releaseForVersionMutex.RLock()
	defer fake.releaseForVersionMutex.RUnlock()
	return fake.releaseForVersionArgsForCall[i].productSlug, fake.releaseForVersionArgsForCall[i].releaseVersion
}

func (fake *FakePivnetClient) ReleaseForVersionReturns(result1 go_pivnet.Release, result2 error) {
	fake.ReleaseForVersionStub = nil
	fake.releaseForVersionReturns = struct {
		result1 go_pivnet.Release
		result2 error
	}{result1, result2}
}

func (fake *FakePivnetClient) CreateRelease(config go_pivnet.CreateReleaseConfig) (go_pivnet.Release, error) {
	fake.createReleaseMutex.Lock()
	fake.createReleaseArgsForCall = append(fake.createReleaseArgsForCall, struct {
		config go_pivnet.CreateReleaseConfig
	}{config})
	fake.recordInvocation("CreateRelease", []interface{}{config})
	fake.createReleaseMutex.Unlock()
	if fake.CreateReleaseStub != nil {
		return fake.CreateReleaseStub(config)
	} else {
		return fake.createReleaseReturns.result1, fake.createReleaseReturns.result2
	}
}

func (fake *FakePivnetClient) CreateReleaseCallCount() int {
	fake.createReleaseMutex.RLock()
	defer fake.createReleaseMutex.RUnlock()
	return len(fake.createReleaseArgsForCall)
}

func (fake *FakePivnetClient) CreateReleaseArgsForCall(i int) go_pivnet.CreateReleaseConfig {
	fake.createReleaseMutex.RLock()
	defer fake.createReleaseMutex.RUnlock()
	return fake.createReleaseArgsForCall[i].config
}

func (fake *FakePivnetClient) CreateReleaseReturns(result1 go_pivnet.Release, result2 error) {
	fake.CreateReleaseStub = nil
	fake.createReleaseReturns = struct {
		result1 go_pivnet.Release
		result2 error
	}{result1, result2}
}

func (fake *FakePivnetClient) DeleteRelease(productSlug string, release go_pivnet.Release) error {
	fake.deleteReleaseMutex.Lock()
	fake.deleteReleaseArgsForCall = append(fake.deleteReleaseArgsForCall, struct {
		productSlug string
		release     go_pivnet.Release
	}{productSlug, release})
	fake.recordInvocation("DeleteRelease", []interface{}{productSlug, release})
	fake.deleteReleaseMutex.Unlock()
	if fake.DeleteReleaseStub != nil {
		return fake.DeleteReleaseStub(productSlug, release)
	} else {
		return fake.deleteReleaseReturns.result1
	}
}

func (fake *FakePivnetClient) DeleteReleaseCallCount() int {
	fake.deleteReleaseMutex.RLock()
	defer fake.deleteReleaseMutex.RUnlock()
	return len(fake.deleteReleaseArgsForCall)
}

func (fake *FakePivnetClient) DeleteReleaseArgsForCall(i int) (string, go_pivnet.Release) {
	fake.deleteReleaseMutex.RLock()
	defer fake.deleteReleaseMutex.RUnlock()
	return fake.deleteReleaseArgsForCall[i].productSlug, fake.deleteReleaseArgsForCall[i].release
}

func (fake *FakePivnetClient) DeleteReleaseReturns(result1 error) {
	fake.DeleteReleaseStub = nil
	fake.deleteReleaseReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakePivnetClient) ReleaseFingerprint(productSlug string, releaseID int) (string, error) {
	fake.releaseFingerprintMutex.Lock()
	fake.releaseFingerprintArgsForCall = append(fake.releaseFingerprintArgsForCall, struct {
		productSlug string
		releaseID   int
	}{productSlug, releaseID})
	fake.recordInvocation("ReleaseFingerprint", []interface{}{productSlug, releaseID})
	fake.releaseFingerprintMutex.Unlock()
	if fake.ReleaseFingerprintStub != nil {
		return fake.ReleaseFingerprintStub(productSlug, releaseID)
	} else {
		return fake.releaseFingerprintReturns.result1, fake.releaseFingerprintReturns.result2
	}
}

func (fake *FakePivnetClient) ReleaseFingerprintCallCount() int {
	fake.releaseFingerprintMutex.RLock()
	defer fake.releaseFingerprintMutex.RUnlock()
	return len(fake.releaseFingerprintArgsForCall)
}

func (fake *FakePivnetClient) ReleaseFingerprintArgsForCall(i int) (string, int) {
	fake.releaseFingerprintMutex.RLock()
	defer fake.releaseFingerprintMutex.RUnlock()
	return fake.releaseFingerprintArgsForCall[i].productSlug, fake.releaseFingerprintArgsForCall[i].releaseID
}

func (fake *FakePivnetClient) ReleaseFingerprintReturns(result1 string, result2 error) {
	fake.ReleaseFingerprintStub = nil
	fake.releaseFingerprintReturns = struct {
		result1 string
		result2 error
	}{result1, result2}
}

func (fake *FakePivnetClient) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.releasesForProductSlugMutex.RLock()
	defer fake.releasesForProductSlugMutex.RUnlock()
	fake.releaseForVersionMutex.RLock()
	defer fake.releaseForVersionMutex.RUnlock()
	fake.createReleaseMutex.RLock()
	defer fake.createReleaseMutex.RUnlock()
	fake.deleteReleaseMutex.RLock()
	defer fake.deleteReleaseMutex.RUnlock()
	fake.releaseFingerprintMutex.RLock()
	defer fake.releaseFingerprintMutex.RUnlock()
	return fake.invocations
}

func (fake *FakePivnetClient) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ release.PivnetClient = new(FakePivnetClient)