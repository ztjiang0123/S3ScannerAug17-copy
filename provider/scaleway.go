package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type Scaleway struct {
	simpleProvider
}

func NewProviderScaleway() (*Scaleway, error) {
	return newSimpleProvider(new(Scaleway))
}

func (sc *Scaleway) newClients() (*clientmap.ClientMap, error) {
	return buildClients(sc,
		func() []string { return ProviderRegions[sc.Name()] },
		endpointFormatter("https://s3.%s.scw.cloud"))
}

func (sc *Scaleway) Scan(b *bucket.Bucket, doDestructiveChecks bool) error {
	client := sc.getRegionClient(b.Region)
	return checkPermissions(client, b, doDestructiveChecks)
}

func (*Scaleway) Insecure() bool {
	return false
}

func (*Scaleway) Name() string {
	return "scaleway"
}

func (*Scaleway) AddressStyle() int {
	return PathStyle
}

func (sc *Scaleway) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	return checkBucketExists(sc.clients, sc.Name(), b)
}

func (sc *Scaleway) Enumerate(b *bucket.Bucket) error {
	return enumerateBucket(sc.getRegionClient, b)
}
