package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type Linode struct {
	simpleProvider
}

func NewProviderLinode() (*Linode, error) {
	return newSimpleProvider(new(Linode))
}

func (pl *Linode) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	return checkBucketExists(pl.clients, pl.Name(), b)
}

func (pl *Linode) Enumerate(b *bucket.Bucket) error {
	return enumerateBucket(pl.getRegionClient, b)
}

func (pl *Linode) newClients() (*clientmap.ClientMap, error) {
	return buildClients(pl,
		func() []string { return ProviderRegions[pl.Name()] },
		endpointFormatter("https://%s.linodeobjects.com"))
}

func (pl *Linode) Scan(b *bucket.Bucket, doDestructiveChecks bool) error {
	client := pl.getRegionClient(b.Region)
	return checkPermissions(client, b, doDestructiveChecks)
}

func (*Linode) Insecure() bool {
	return false
}

func (*Linode) Name() string {
	return "linode"
}

func (*Linode) AddressStyle() int {
	return VirtualHostStyle
}
