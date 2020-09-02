// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"io"
	"time"
)

var defaultTimeout = Timeout{
	// TODO(kakkoyun): Determinte defaults.
	Iter:       30 * time.Minute,
	Get:        30 * time.Minute,
	GetRange:   30 * time.Minute,
	Exists:     30 * time.Minute,
	Upload:     30 * time.Minute,
	Delete:     30 * time.Minute,
	Attributes: 30 * time.Minute,
}

type Timeout struct {
	Iter       time.Duration `yaml:"iter"`
	Get        time.Duration `yaml:"get"`
	GetRange   time.Duration `yaml:"get_range"`
	Exists     time.Duration `yaml:"exists"`
	Upload     time.Duration `yaml:"upload"`
	Delete     time.Duration `yaml:"delete"`
	Attributes time.Duration `yaml:"attributes"`
}

// BucketWithTimeout wraps a bucket and constraints execution time of an operation.
func BucketWithTimeout(b Bucket, _ Timeout) *timeoutBucket {
	// TODO(kakkoyun): Fallback to defaults.
	return &timeoutBucket{b, defaultTimeout}
}

type timeoutBucket struct {
	Bucket

	timeout Timeout
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *timeoutBucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.Iter)
	defer cancel()

	return b.Bucket.Iter(ctx, dir, f)
}

// Get returns a reader for the given object name.
func (b *timeoutBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.Get)
	defer cancel()

	return b.Bucket.Get(ctx, name)
}

// GetRange returns a new range reader for the given object name and range.
func (b *timeoutBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.GetRange)
	defer cancel()

	return b.Bucket.GetRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *timeoutBucket) Exists(ctx context.Context, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.Exists)
	defer cancel()

	return b.Bucket.Exists(ctx, name)
}

// Upload the contents of the reader as an object into the bucket.
// Upload should be idempotent.
func (b *timeoutBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.Upload)
	defer cancel()

	return b.Bucket.Upload(ctx, name, r)
}

// Delete removes the object with the given name.
// If object does not exists in the moment of deletion, Delete should throw error.
func (b *timeoutBucket) Delete(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.Delete)
	defer cancel()

	return b.Bucket.Delete(ctx, name)
}

// Attributes returns information about the specified object.
func (b *timeoutBucket) Attributes(ctx context.Context, name string) (ObjectAttributes, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout.Attributes)
	defer cancel()

	return b.Bucket.Attributes(ctx, name)
}