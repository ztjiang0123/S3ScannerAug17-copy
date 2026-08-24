package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type DigitalOcean struct {
	simpleProvider
}

func (pdo DigitalOcean) Insecure() bool {
	return false
}

func (pdo DigitalOcean) Name() string {
	return "digitalocean"
}

func (pdo DigitalOcean) AddressStyle() int {
	return PathStyle
}

func (pdo DigitalOcean) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	return checkBucketExists(pdo.clients, pdo.Name(), b)
}

func (pdo DigitalOcean) Scan(bucket *bucket.Bucket, doDestructiveChecks bool) error {
	client := pdo.getRegionClient(bucket.Region)
	return checkPermissions(client, bucket, doDestructiveChecks)
}

func (pdo DigitalOcean) Enumerate(b *bucket.Bucket) error {
	return enumerateBucket(pdo.getRegionClient, b)
}

func (pdo *DigitalOcean) newClients() (*clientmap.ClientMap, error) {
	return buildClients(pdo,
		func() []string { return ProviderRegions[pdo.Name()] },
		endpointFormatter("https://%s.digitaloceanspaces.com"))
}

func NewDigitalOcean() (*DigitalOcean, error) {
	return newSimpleProvider(new(DigitalOcean))
}
