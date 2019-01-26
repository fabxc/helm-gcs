package gcs

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

// NewClient creates a new gcs client.
// Use Application Default Credentials if serviceAccount is empty.
func NewClient(serviceAccountPath string) (*storage.Client, error) {
	opts := []option.ClientOption{}
	if serviceAccountPath != "" {
		opts = append(opts, option.WithCredentialsFile(serviceAccountPath))
	}
	client, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, errors.Wrap(err, "new client")
	}
	return client, err
}

// Object retourne a new object handle for the given path
func Object(client *storage.Client, path string) (*storage.ObjectHandle, error) {
	bucket, path, err := splitPath(path)
	if err != nil {
		return nil, errors.Wrap(err, "split path")
	}
	return client.Bucket(bucket).Object(path), nil
}

func splitPath(gcsurl string) (bucket string, path string, err error) {
	u, err := url.Parse(gcsurl)
	if err != nil {
		return
	}
	switch u.Scheme {
	case "gs", "gcs":
		bucket = u.Host
		path = u.Path[1:]
	case "https":
		if u.Host != "storage.cloud.google.com" {
			return "", "", fmt.Errorf("unexpected host %q in https URL, expected storage.cloud.google.com", u.Host)
		}
		u.Path = strings.TrimPrefix(u.Path, "/")
		parts := strings.SplitN(u.Path, "/", 2)
		switch len(parts) {
		case 0:
			return "", "", errors.New("no bucket specified in URL")
		case 1:
			bucket = parts[0]
		case 2:
			bucket, path = parts[0], parts[1]
		}
	default:
		return "", "", fmt.Errorf("unexpected scheme %s, expected gs or https", u.Scheme)
	}
	return
}
