package provider

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

// simpleProvider carries the state and configuration shared by the non-AWS,
// region-list based providers (DigitalOcean, Dreamhost, Linode, Scaleway,
// Wasabi and the fully custom provider). Those providers differ only in a few
// values - their name, address style, whether TLS verification is skipped and
// how a region maps to an endpoint URL - so the common behavior lives here and
// is reused via the helper functions below.
type simpleProvider struct {
	clients *clientmap.ClientMap
}

func (sp *simpleProvider) getRegionClient(region string) *s3.Client {
	return sp.clients.Get(region, false)
}

// buildClients constructs a ClientMap containing one anonymous client per
// region returned by regionsFor. endpointFor maps a region to the endpoint URL
// used for that region's client. It is the shared implementation behind every
// provider's newClients method.
func buildClients(p StorageProvider, regionsFor func() []string, endpointFor func(region string) string) (*clientmap.ClientMap, error) {
	regions := regionsFor()
	clients := clientmap.WithCapacity(len(regions))
	for _, r := range regions {
		client, err := newNonAWSClient(p, endpointFor(r))
		if err != nil {
			return nil, err
		}
		clients.Set(r, false, client)
	}
	return clients, nil
}

// applyExistsResult records the outcome of an existence check on b using the
// convention shared by every provider: mark the bucket as existing (and record
// its region) when exists is true, otherwise mark it as not existing.
func applyExistsResult(b *bucket.Bucket, exists bool, region string) {
	if exists {
		b.Exists = bucket.BucketExists
		b.Region = region
	} else {
		b.Exists = bucket.BucketNotExist
	}
}

// checkBucketExists runs the shared region-fan-out existence check for b against
// clients, sets b.Provider to providerName and records the result on b. It is
// the common body of the providers whose BucketExists delegates to bucketExists.
func checkBucketExists(clients *clientmap.ClientMap, providerName string, b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = providerName
	exists, region, err := bucketExists(clients, b)
	if err != nil {
		return b, err
	}
	applyExistsResult(b, exists, region)
	return b, nil
}

// enumerateBucket runs the shared object enumeration for b. It refuses to
// enumerate a bucket that is not known to exist and otherwise lists objects via
// the region client returned by clientFor.
func enumerateBucket(clientFor func(region string) *s3.Client, b *bucket.Bucket) error {
	if b.Exists != bucket.BucketExists {
		return errors.New("bucket might not exist")
	}
	return enumerateListObjectsV2(clientFor(b.Region), b)
}

// endpointFormatter returns an endpointFor function that plugs a region into the
// given fmt format string. The format string must contain exactly one %s verb.
func endpointFormatter(format string) func(region string) string {
	return func(region string) string {
		return fmt.Sprintf(format, region)
	}
}

// clientSetter is implemented by every simple provider (via the embedded
// simpleProvider) so newSimpleProvider can build its clients and store them
// back on the concrete provider value.
type clientSetter interface {
	newClients() (*clientmap.ClientMap, error)
	setClients(*clientmap.ClientMap)
}

func (sp *simpleProvider) setClients(clients *clientmap.ClientMap) {
	sp.clients = clients
}

// newSimpleProvider constructs p's clients and stores them on p, returning p.
// It is the shared body of the region-list providers' constructors. On error the
// partially-initialized provider is returned alongside the error, matching the
// original constructors' behavior.
func newSimpleProvider[T clientSetter](p T) (T, error) {
	clients, err := p.newClients()
	if err != nil {
		return p, err
	}
	p.setClients(clients)
	return p, nil
}
