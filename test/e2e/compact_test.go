// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestCompact(t *testing.T) {
	t.Parallel()
	l := log.NewLogfmtLogger(os.Stdout)

	// blockDesc describes a recipe to generate blocks from the given series and external labels.
	type blockDesc struct {
		series           []labels.Labels
		extLset          labels.Labels
		mint             int64
		maxt             int64
		samplesPerSeries int
	}

	type retention struct {
		resRaw string
		res5m  string
		res1h  string
	}

	delay := 30 * time.Minute
	now := time.Now()

	for i, tcase := range []struct {
		name                string
		blocks              []blockDesc
		replicaLabels       []string
		downsamplingEnabled bool
		retention           *retention
		query               string

		expected          []model.Metric
		expectOfModBlocks float64
		expectOfBlocks    uint64
		expectOfSamples   uint64
		expectOfSeries    uint64
		expectOfChunks    uint64
	}{
		{
			name: "(full) vertically overlapping blocks with replica labels",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "2"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "rule_replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
			},
			replicaLabels:       []string{"replica", "rule_replica"},
			downsamplingEnabled: true,
			query:               "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			expectOfModBlocks: 3,
			expectOfBlocks:    1,
			expectOfSamples:   120,
			expectOfSeries:    1,
			expectOfChunks:    2,
		},
		{
			name: "(full) vertically overlapping blocks with replica labels, downsampling disabled",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "2"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "rule_replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
			},
			replicaLabels:       []string{"replica", "rule_replica"},
			downsamplingEnabled: false,
			query:               "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			expectOfModBlocks: 3,
			expectOfBlocks:    1,
			expectOfSamples:   120,
			expectOfSeries:    1,
			expectOfChunks:    2,
		},
		{
			name: "(partial) vertically overlapping blocks with replica labels",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "2"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(1 * time.Hour)),
					samplesPerSeries: 60,
				},
			},
			replicaLabels:       []string{"replica"},
			downsamplingEnabled: true,
			query:               "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			expectOfModBlocks: 2,
			expectOfBlocks:    1,
			expectOfSamples:   179, // TODO(kakkoyun): ?
			expectOfSeries:    1,
			expectOfChunks:    2,
		},
		{
			name: "(contains) vertically overlapping blocks with replica labels",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "1"),
					mint:             timestamp.FromTime(now.Add(30 * time.Minute)),
					maxt:             timestamp.FromTime(now.Add(1 * time.Hour)),
					samplesPerSeries: 90,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "2"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
			},
			replicaLabels:       []string{"replica"},
			downsamplingEnabled: true,
			query:               "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			expectOfModBlocks: 2,
			expectOfBlocks:    1,
			expectOfSamples:   210, // TODO(kakkoyun): ?
			expectOfSeries:    1,
			expectOfChunks:    2,
		},
		{
			name: "(shifted) vertically overlapping blocks with replica labels",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "1"),
					mint:             timestamp.FromTime(now.Add(30 * time.Minute)),
					maxt:             timestamp.FromTime(now.Add(150 * time.Minute)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "2"),
					mint:             timestamp.FromTime(now.Add(-30 * time.Minute)),
					maxt:             timestamp.FromTime(now.Add(90 * time.Minute)),
					samplesPerSeries: 120,
				},
			},
			replicaLabels:       []string{"replica"},
			downsamplingEnabled: true,
			query:               "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			expectOfModBlocks: 2,
			expectOfBlocks:    1,
			expectOfSamples:   240, // TODO(kakkoyun): ?
			expectOfSeries:    1,
			expectOfChunks:    2,
		},
		{
			name: "(full) vertically overlapping blocks with replica labels, retention specified",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "replica", "2"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1", "rule_replica", "1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 120,
				},
			},
			replicaLabels:       []string{"replica", "rule_replica"},
			downsamplingEnabled: true,
			retention: &retention{
				resRaw: "0d",
				res5m:  "5m",
				res1h:  "5m",
			},
			query: "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
			expectOfModBlocks: 3,
			expectOfBlocks:    1,
			expectOfSamples:   120,
			expectOfSeries:    1,
			expectOfChunks:    2,
		},
		{
			name: "(full) vertically overlapping blocks without replica labels",
			blocks: []blockDesc{
				{
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value1"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 2,
				}, {
					series:           []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
					extLset:          labels.FromStrings("ext1", "value2"),
					mint:             timestamp.FromTime(now),
					maxt:             timestamp.FromTime(now.Add(2 * time.Hour)),
					samplesPerSeries: 2,
				},
			},
			replicaLabels:       []string{"replica"},
			downsamplingEnabled: true,
			query:               "{a=\"1\"}",

			expected: []model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value2",
				},
			},
			expectOfModBlocks: 0,
			expectOfBlocks:    2,
			expectOfSamples:   4,
			expectOfSeries:    2,
			expectOfChunks:    2,
		},
	} {
		i := i
		tcase := tcase
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			s, err := e2e.NewScenario("e2e_test_compact_" + strconv.Itoa(i))
			testutil.Ok(t, err)
			defer s.Close()

			dir := filepath.Join(s.SharedDir(), "tmp_"+strconv.Itoa(i))
			testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), dir), os.ModePerm))

			bucket := "thanos_" + strconv.Itoa(i)

			m := e2edb.NewMinio(8080+i, bucket)
			testutil.Ok(t, s.StartAndWaitReady(m))

			bkt, err := s3.NewBucketWithConfig(l, s3.Config{
				Bucket:    bucket,
				AccessKey: e2edb.MinioAccessKey,
				SecretKey: e2edb.MinioSecretKey,
				Endpoint:  m.HTTPEndpoint(), // We need separate client config, when connecting to minio from outside.
				Insecure:  true,
			}, "test-feed")
			testutil.Ok(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()

			var rawBlockIds []ulid.ULID
			for _, b := range tcase.blocks {
				id, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, b.series, b.samplesPerSeries, b.mint, b.maxt, delay, b.extLset, 0)
				testutil.Ok(t, err)
				testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id.String()), id.String()))
				rawBlockIds = append(rawBlockIds, id)
			}

			dedupFlags := make([]string, 0, len(tcase.replicaLabels))
			for _, l := range tcase.replicaLabels {
				dedupFlags = append(dedupFlags, "--deduplication.replica-label="+l)
			}

			retenFlags := make([]string, 0, 3)
			if tcase.retention != nil {
				retenFlags = append(retenFlags, "--retention.resolution-raw="+tcase.retention.resRaw)
				retenFlags = append(retenFlags, "--retention.resolution-5m="+tcase.retention.res5m)
				retenFlags = append(retenFlags, "--retention.resolution-1h="+tcase.retention.res1h)
			}

			cmpt, err := e2ethanos.NewCompactor(s.SharedDir(), strconv.Itoa(i), client.BucketConfig{
				Type: client.S3,
				Config: s3.Config{
					Bucket:    bucket,
					AccessKey: e2edb.MinioAccessKey,
					SecretKey: e2edb.MinioSecretKey,
					Endpoint:  m.NetworkHTTPEndpoint(),
					Insecure:  true,
				},
			},
				nil, // relabel configs.
				tcase.downsamplingEnabled,
				append(dedupFlags, retenFlags...)...,
			)

			testutil.Ok(t, err)
			testutil.Ok(t, s.StartAndWaitReady(cmpt))
			testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIds))), "thanos_blocks_meta_synced"))
			testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
			testutil.Ok(t, cmpt.WaitSumMetrics(e2e.Equals(tcase.expectOfModBlocks), "thanos_blocks_meta_modified"))

			str, err := e2ethanos.NewStoreGW(s.SharedDir(), "compact_"+strconv.Itoa(i), client.BucketConfig{
				Type: client.S3,
				Config: s3.Config{
					Bucket:    bucket,
					AccessKey: e2edb.MinioAccessKey,
					SecretKey: e2edb.MinioSecretKey,
					Endpoint:  m.NetworkHTTPEndpoint(),
					Insecure:  true,
				},
			})
			testutil.Ok(t, err)
			testutil.Ok(t, s.StartAndWaitReady(str))
			testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(tcase.expectOfBlocks)), "thanos_blocks_meta_synced"))
			testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

			q, err := e2ethanos.NewQuerier(s.SharedDir(), "compact_"+strconv.Itoa(i), []string{str.GRPCNetworkEndpoint()}, nil)
			testutil.Ok(t, err)
			testutil.Ok(t, s.StartAndWaitReady(q))

			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			queryAndAssert(t, ctx, q.HTTPEndpoint(),
				tcase.query,
				promclient.QueryOptions{
					Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
				},
				tcase.expected,
			)

			var actualOfBlocks uint64
			var actualOfChunks uint64
			var actualOfSeries uint64
			var actualOfSamples uint64
			var sources []ulid.ULID
			testutil.Ok(t, bkt.Iter(ctx, "", func(n string) error {
				id, ok := block.IsBlockDir(n)
				if !ok {
					return nil
				}

				actualOfBlocks += 1

				meta, err := block.DownloadMeta(ctx, l, bkt, id)
				if err != nil {
					return err
				}

				actualOfChunks += meta.Stats.NumChunks
				actualOfSeries += meta.Stats.NumSeries
				actualOfSamples += meta.Stats.NumSamples
				sources = append(sources, meta.Compaction.Sources...)
				return nil
			}))

			// Make sure only necessary amount of blocks fetched from store, to observe affects of offline deduplication.
			testutil.Equals(t, tcase.expectOfBlocks, actualOfBlocks)
			if len(rawBlockIds) < int(tcase.expectOfBlocks) { // check sources only if compacted.
				testutil.Equals(t, rawBlockIds, sources)
			}
			testutil.Equals(t, tcase.expectOfChunks, actualOfChunks)
			testutil.Equals(t, tcase.expectOfSeries, actualOfSeries)
			testutil.Equals(t, tcase.expectOfSamples, actualOfSamples)
		})
	}
}
