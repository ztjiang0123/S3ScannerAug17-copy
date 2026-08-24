package provider

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type Wasabi struct {
	simpleProvider
	existsClient *s3.Client
}

func (w *Wasabi) Insecure() bool {
	return false
}

func (w *Wasabi) AddressStyle() int {
	return PathStyle
}

func (w *Wasabi) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = w.Name()
	exists, region, err := bucketExists301(w.existsClient, "us-east-1", b)
	if err != nil {
		return b, err
	}
	applyExistsResult(b, exists, region)
	return b, nil
}

func (w *Wasabi) Scan(bucket *bucket.Bucket, doDestructiveChecks bool) error {
	client := w.getRegionClient(bucket.Region)
	return checkPermissions(client, bucket, doDestructiveChecks)
}

func (w *Wasabi) Enumerate(b *bucket.Bucket) error {
	return enumerateBucket(w.getRegionClient, b)
}

func (w *Wasabi) newExistsClient() (*s3.Client, error) {
	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { // don't follow redirects
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
		},
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithHTTPClient(client),
		config.WithRegion("auto"),
	)
	if err != nil {
		return nil, err
	}

	cfg.BaseEndpoint = aws.String("https://s3.wasabisys.com")
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), nil
}

func NewProviderWasabi() (*Wasabi, error) {
	w, err := newSimpleProvider(new(Wasabi))
	if err != nil {
		return w, err
	}

	c, cErr := w.newExistsClient()
	if cErr != nil {
		return w, cErr
	}
	w.existsClient = c
	return w, nil
}

func (w *Wasabi) newClients() (*clientmap.ClientMap, error) {
	return buildClients(w,
		func() []string { return ProviderRegions[w.Name()] },
		endpointFormatter("https://s3.%s.wasabisys.com"))
}

func (w *Wasabi) Name() string { return "wasabi" }
