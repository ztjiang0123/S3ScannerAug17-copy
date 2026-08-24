package provider

import (
	"strings"

	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

// Dreamhost responds strangely if you attempt to access a bucket named 'auth'
var forbiddenBuckets = []string{"auth"}

type Dreamhost struct {
	simpleProvider
}

func (p Dreamhost) Insecure() bool {
	return false
}

func (Dreamhost) Name() string {
	return "dreamhost"
}

func (p Dreamhost) AddressStyle() int {
	return PathStyle
}

func (p Dreamhost) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	// Check for forbidden name
	for _, fb := range forbiddenBuckets {
		if strings.ToLower(b.Name) == fb {
			b.Exists = bucket.BucketNotExist
			return b, nil
		}
	}

	return checkBucketExists(p.clients, p.Name(), b)
}

func (p Dreamhost) Scan(bucket *bucket.Bucket, doDestructiveChecks bool) error {
	client := p.getRegionClient(bucket.Region)
	return checkPermissions(client, bucket, doDestructiveChecks)
}

func (p Dreamhost) Enumerate(b *bucket.Bucket) error {
	return enumerateBucket(p.getRegionClient, b)
}

func (p *Dreamhost) newClients() (*clientmap.ClientMap, error) {
	return buildClients(p,
		func() []string { return ProviderRegions[p.Name()] },
		endpointFormatter("https://objects-%s.dream.io"))
}

func NewProviderDreamhost() (*Dreamhost, error) {
	return newSimpleProvider(new(Dreamhost))
}
