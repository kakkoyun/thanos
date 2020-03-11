// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type syncMetrics struct {
	syncs        prometheus.Counter
	syncFailures prometheus.Counter
	syncDuration prometheus.Histogram

	synced   *extprom.TxGaugeVec
	modified *extprom.TxGaugeVec
}

const (
	syncMetricSubSys = "blocks_meta"

	corruptedMeta = "corrupted-meta-json"
	noMeta        = "no-meta-json"
	loadedMeta    = "loaded"
	failedMeta    = "failed"

	// Filter's label values.
	labelExcludedMeta  = "label-excluded"
	timeExcludedMeta   = "time-excluded"
	tooFreshMeta       = "too-fresh"
	duplicateMeta      = "duplicate"
	replicaRemovedMeta = "replica-label-removed"

	// Blocks that are marked for deletion can be loaded as well. This is done to make sure that we load blocks that are meant to be deleted,
	// but don't have a replacement block yet.
	markedForDeletionMeta = "marked-for-deletion"
)

func newSyncMetrics(reg prometheus.Registerer) *syncMetrics {
	var m syncMetrics

	m.syncs = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: syncMetricSubSys,
		Name:      "syncs_total",
		Help:      "Total blocks metadata synchronization attempts",
	})
	m.syncFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: syncMetricSubSys,
		Name:      "sync_failures_total",
		Help:      "Total blocks metadata synchronization failures",
	})
	m.syncDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Subsystem: syncMetricSubSys,
		Name:      "sync_duration_seconds",
		Help:      "Duration of the blocks metadata synchronization in seconds",
		Buckets:   []float64{0.01, 1, 10, 100, 1000},
	})
	m.synced = extprom.NewTxGaugeVec(reg, prometheus.GaugeOpts{
		Subsystem: syncMetricSubSys,
		Name:      "synced",
		Help:      "Number of block metadata synced",
	},
		[]string{"state"},
		[]string{corruptedMeta},
		[]string{noMeta},
		[]string{loadedMeta},
		[]string{tooFreshMeta},
		[]string{failedMeta},
		[]string{labelExcludedMeta},
		[]string{timeExcludedMeta},
		[]string{duplicateMeta},
		[]string{markedForDeletionMeta},
	)
	m.modified = extprom.NewTxGaugeVec(reg, prometheus.GaugeOpts{
		Subsystem: syncMetricSubSys,
		Name:      "modified",
		Help:      "Number of block metadata that modified",
	},
		[]string{"modified"},
		[]string{replicaRemovedMeta},
	)
	return &m
}

type MetadataFetcher interface {
	Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error)
}

type MetaFetcherFilter func(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, metrics *syncMetrics, incompleteView bool) error

// MetaFetcher is a struct that synchronizes filtered metadata of all block in the object storage with the local state.
// Not go-routine safe.
type MetaFetcher struct {
	logger      log.Logger
	concurrency int
	bkt         objstore.BucketReader

	// Optional local directory to cache meta.json files.
	cacheDir string
	metrics  *syncMetrics

	filters []MetaFetcherFilter

	cached map[ulid.ULID]*metadata.Meta
}

// NewMetaFetcher constructs MetaFetcher.
func NewMetaFetcher(logger log.Logger, concurrency int, bkt objstore.BucketReader, dir string, r prometheus.Registerer, filters ...MetaFetcherFilter) (*MetaFetcher, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	cacheDir := ""
	if dir != "" {
		cacheDir = filepath.Join(dir, "meta-syncer")
		if err := os.MkdirAll(cacheDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	return &MetaFetcher{
		logger:      log.With(logger, "component", "block.MetaFetcher"),
		concurrency: concurrency,
		bkt:         bkt,
		cacheDir:    cacheDir,
		metrics:     newSyncMetrics(r),
		filters:     filters,
		cached:      map[ulid.ULID]*metadata.Meta{},
	}, nil
}

var (
	ErrorSyncMetaNotFound  = errors.New("meta.json not found")
	ErrorSyncMetaCorrupted = errors.New("meta.json corrupted")
)

// loadMeta returns metadata from object storage or error.
// It returns `ErrorSyncMetaNotFound` and `ErrorSyncMetaCorrupted` sentinel errors in those cases.
func (s *MetaFetcher) loadMeta(ctx context.Context, id ulid.ULID) (*metadata.Meta, error) {
	var (
		metaFile       = path.Join(id.String(), MetaFilename)
		cachedBlockDir = filepath.Join(s.cacheDir, id.String())
	)

	// TODO(bwplotka): If that causes problems (obj store rate limits), add longer ttl to cached items.
	// For 1y and 100 block sources this generates ~1.5-3k HEAD RPM. AWS handles 330k RPM per prefix.
	// TODO(bwplotka): Consider filtering by consistency delay here (can't do until compactor healthyOverride work).
	ok, err := s.bkt.Exists(ctx, metaFile)
	if err != nil {
		return nil, errors.Wrapf(err, "meta.json file exists: %v", metaFile)
	}
	if !ok {
		return nil, ErrorSyncMetaNotFound
	}

	if m, seen := s.cached[id]; seen {
		return m, nil
	}

	// Best effort load from local dir.
	if s.cacheDir != "" {
		m, err := metadata.Read(cachedBlockDir)
		if err == nil {
			return m, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			level.Warn(s.logger).Log("msg", "best effort read of the local meta.json failed; removing cached block dir", "dir", cachedBlockDir, "err", err)
			if err := os.RemoveAll(cachedBlockDir); err != nil {
				level.Warn(s.logger).Log("msg", "best effort remove of cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
			}
		}
	}

	r, err := s.bkt.Get(ctx, metaFile)
	if s.bkt.IsObjNotFoundErr(err) {
		// Meta.json was deleted between bkt.Exists and here.
		return nil, errors.Wrapf(ErrorSyncMetaNotFound, "%v", err)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "get meta file: %v", metaFile)
	}

	defer runutil.CloseWithLogOnErr(s.logger, r, "close bkt meta get")

	metaContent, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta file: %v", metaFile)
	}

	m := &metadata.Meta{}
	if err := json.Unmarshal(metaContent, m); err != nil {
		return nil, errors.Wrapf(ErrorSyncMetaCorrupted, "meta.json %v unmarshal: %v", metaFile, err)
	}

	if m.Version != metadata.MetaVersion1 {
		return nil, errors.Errorf("unexpected meta file: %s version: %d", metaFile, m.Version)
	}

	// Best effort cache in local dir.
	if s.cacheDir != "" {
		if err := os.MkdirAll(cachedBlockDir, os.ModePerm); err != nil {
			level.Warn(s.logger).Log("msg", "best effort mkdir of the meta.json block dir failed; ignoring", "dir", cachedBlockDir, "err", err)
		}

		if err := metadata.Write(s.logger, cachedBlockDir, m); err != nil {
			level.Warn(s.logger).Log("msg", "best effort save of the meta.json to local dir failed; ignoring", "dir", cachedBlockDir, "err", err)
		}
	}
	return m, nil
}

// Fetch returns all block metas as well as partial blocks (blocks without or with corrupted meta file) from the bucket.
// It's caller responsibility to not change the returned metadata files. Maps can be modified.
//
// Returned error indicates a failure in fetching metadata. Returned meta can be assumed as correct, with some blocks missing.
func (s *MetaFetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error) {
	start := time.Now()
	defer func() {
		s.metrics.syncDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			s.metrics.syncFailures.Inc()
		}
	}()
	s.metrics.syncs.Inc()

	metas = make(map[ulid.ULID]*metadata.Meta)
	partial = make(map[ulid.ULID]error)

	var (
		wg  sync.WaitGroup
		ch  = make(chan ulid.ULID, s.concurrency)
		mtx sync.Mutex

		metaErrs tsdberrors.MultiError
	)

	s.metrics.synced.ResetTx()

	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range ch {
				meta, err := s.loadMeta(ctx, id)
				if err == nil {
					mtx.Lock()
					metas[id] = meta
					mtx.Unlock()
					continue
				}

				switch errors.Cause(err) {
				default:
					s.metrics.synced.WithLabelValues(failedMeta).Inc()
					mtx.Lock()
					metaErrs.Add(err)
					mtx.Unlock()
					continue
				case ErrorSyncMetaNotFound:
					s.metrics.synced.WithLabelValues(noMeta).Inc()
				case ErrorSyncMetaCorrupted:
					s.metrics.synced.WithLabelValues(corruptedMeta).Inc()
				}

				mtx.Lock()
				partial[id] = err
				mtx.Unlock()
			}
		}()
	}

	// Workers scheduled, distribute blocks.
	err = s.bkt.Iter(ctx, "", func(name string) error {
		id, ok := IsBlockDir(name)
		if !ok {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- id:
		}

		return nil
	})
	close(ch)

	wg.Wait()
	if err != nil {
		return nil, nil, errors.Wrap(err, "MetaFetcher: iter bucket")
	}

	incompleteView := len(metaErrs) > 0

	// Only for complete view of blocks update the cache.
	if !incompleteView {
		cached := make(map[ulid.ULID]*metadata.Meta, len(metas))
		for id, m := range metas {
			cached[id] = m
		}
		s.cached = cached

		// Best effort cleanup of disk-cached metas.
		if s.cacheDir != "" {
			names, err := fileutil.ReadDir(s.cacheDir)
			if err != nil {
				level.Warn(s.logger).Log("msg", "best effort remove of not needed cached dirs failed; ignoring", "err", err)
			} else {
				for _, n := range names {
					id, ok := IsBlockDir(n)
					if !ok {
						continue
					}

					if _, ok := metas[id]; ok {
						continue
					}

					cachedBlockDir := filepath.Join(s.cacheDir, id.String())

					// No such block loaded, remove the local dir.
					if err := os.RemoveAll(cachedBlockDir); err != nil {
						level.Warn(s.logger).Log("msg", "best effort remove of not needed cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
					}
				}
			}
		}
	}

	for _, f := range s.filters {
		// NOTE: filter can update synced metric accordingly to the reason of the exclude.
		if err := f(ctx, metas, s.metrics, incompleteView); err != nil {
			return nil, nil, errors.Wrap(err, "filter metas")
		}
	}

	s.metrics.synced.WithLabelValues(loadedMeta).Set(float64(len(metas)))
	s.metrics.synced.Submit()
	s.metrics.modified.Submit()

	if incompleteView {
		return metas, partial, errors.Wrap(metaErrs, "incomplete view")
	}

	level.Debug(s.logger).Log("msg", "successfully fetched block metadata", "duration", time.Since(start).String(), "cached", len(s.cached), "returned", len(metas), "partial", len(partial))
	return metas, partial, nil
}

var _ MetaFetcherFilter = (&TimePartitionMetaFilter{}).Filter

// TimePartitionMetaFilter is a MetaFetcher filter that filters out blocks that are outside of specified time range.
// Not go-routine safe.
type TimePartitionMetaFilter struct {
	minTime, maxTime model.TimeOrDurationValue
}

// NewTimePartitionMetaFilter creates TimePartitionMetaFilter.
func NewTimePartitionMetaFilter(MinTime, MaxTime model.TimeOrDurationValue) *TimePartitionMetaFilter {
	return &TimePartitionMetaFilter{minTime: MinTime, maxTime: MaxTime}
}

// Filter filters out blocks that are outside of specified time range.
func (f *TimePartitionMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, metrics *syncMetrics, _ bool) error {
	for id, m := range metas {
		if m.MaxTime >= f.minTime.PrometheusTimestamp() && m.MinTime <= f.maxTime.PrometheusTimestamp() {
			continue
		}
		metrics.synced.WithLabelValues(timeExcludedMeta).Inc()
		delete(metas, id)
	}
	return nil
}

var _ MetaFetcherFilter = (&LabelShardedMetaFilter{}).Filter

// LabelShardedMetaFilter represents struct that allows sharding.
// Not go-routine safe.
type LabelShardedMetaFilter struct {
	relabelConfig []*relabel.Config
}

// NewLabelShardedMetaFilter creates LabelShardedMetaFilter.
func NewLabelShardedMetaFilter(relabelConfig []*relabel.Config) *LabelShardedMetaFilter {
	return &LabelShardedMetaFilter{relabelConfig: relabelConfig}
}

// Special label that will have an ULID of the meta.json being referenced to.
const blockIDLabel = "__block_id"

// Filter filters out blocks that have no labels after relabelling of each block external (Thanos) labels.
func (f *LabelShardedMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, metrics *syncMetrics, _ bool) error {
	var lbls labels.Labels
	for id, m := range metas {
		lbls = lbls[:0]
		lbls = append(lbls, labels.Label{Name: blockIDLabel, Value: id.String()})
		for k, v := range m.Thanos.Labels {
			lbls = append(lbls, labels.Label{Name: k, Value: v})
		}

		if processedLabels := relabel.Process(lbls, f.relabelConfig...); len(processedLabels) == 0 {
			metrics.synced.WithLabelValues(labelExcludedMeta).Inc()
			delete(metas, id)
		}
	}
	return nil
}

// DeduplicateFilter is a MetaFetcher filter that filters out older blocks that have exactly the same data.
// Not go-routine safe.
type DeduplicateFilter struct {
	duplicateIDs []ulid.ULID
}

// NewDeduplicateFilter creates DeduplicateFilter.
func NewDeduplicateFilter() *DeduplicateFilter {
	return &DeduplicateFilter{}
}

// Filter filters out duplicate blocks that can be formed
// from two or more overlapping blocks that fully submatches the source blocks of the older blocks.
func (f *DeduplicateFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, metrics *syncMetrics, _ bool) error {
	var wg sync.WaitGroup

	metasByResolution := make(map[int64][]*metadata.Meta)
	for _, meta := range metas {
		res := meta.Thanos.Downsample.Resolution
		metasByResolution[res] = append(metasByResolution[res], meta)
	}

	for res := range metasByResolution {
		wg.Add(1)
		go func(res int64) {
			defer wg.Done()
			f.filterForResolution(NewNode(&metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID: ulid.MustNew(uint64(0), nil),
				},
			}), metasByResolution[res], metas, res, metrics.synced)
		}(res)
	}

	wg.Wait()

	return nil
}

func (f *DeduplicateFilter) filterForResolution(root *Node, metaSlice []*metadata.Meta, metas map[ulid.ULID]*metadata.Meta, res int64, synced *extprom.TxGaugeVec) {
	sort.Slice(metaSlice, func(i, j int) bool {
		ilen := len(metaSlice[i].Compaction.Sources)
		jlen := len(metaSlice[j].Compaction.Sources)

		if ilen == jlen {
			return metaSlice[i].ULID.Compare(metaSlice[j].ULID) < 0
		}

		return ilen-jlen > 0
	})

	for _, meta := range metaSlice {
		addNodeBySources(root, NewNode(meta))
	}

	duplicateULIDs := getNonRootIDs(root)
	for _, id := range duplicateULIDs {
		if metas[id] != nil {
			f.duplicateIDs = append(f.duplicateIDs, id)
		}
		synced.WithLabelValues(duplicateMeta).Inc()
		delete(metas, id)
	}
}

// DuplicateIDs returns slice of block ids that are filtered out by DeduplicateFilter.
func (f *DeduplicateFilter) DuplicateIDs() []ulid.ULID {
	return f.duplicateIDs
}

func addNodeBySources(root *Node, add *Node) bool {
	var rootNode *Node
	for _, node := range root.Children {
		parentSources := node.Compaction.Sources
		childSources := add.Compaction.Sources

		// Block exists with same sources, add as child.
		if contains(parentSources, childSources) && contains(childSources, parentSources) {
			node.Children = append(node.Children, add)
			return true
		}

		// Block's sources are present in other block's sources, add as child.
		if contains(parentSources, childSources) {
			rootNode = node
			break
		}
	}

	// Block cannot be attached to any child nodes, add it as child of root.
	if rootNode == nil {
		root.Children = append(root.Children, add)
		return true
	}

	return addNodeBySources(rootNode, add)
}

func contains(s1 []ulid.ULID, s2 []ulid.ULID) bool {
	for _, a := range s2 {
		found := false
		for _, e := range s1 {
			if a.Compare(e) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ReplicaLabelRemover is a MetaFetcher modifier modifies external labels of existing blocks, it removes given replica labels from the metadata of blocks that have it.
type ReplicaLabelRemover struct {
	logger log.Logger

	replicaLabels []string
}

// NewReplicaLabelRemover creates a ReplicaLabelRemover.
func NewReplicaLabelRemover(logger log.Logger, replicaLabels []string) *ReplicaLabelRemover {
	return &ReplicaLabelRemover{logger: logger, replicaLabels: replicaLabels}
}

// Modify modifies external labels of existing blocks, it removes given replica labels from the metadata of blocks that have it.
func (r *ReplicaLabelRemover) Modify(_ context.Context, metas map[ulid.ULID]*metadata.Meta, metrics *syncMetrics, view bool) error {
	for u, meta := range metas {
		labels := meta.Thanos.Labels
		for _, replicaLabel := range r.replicaLabels {
			if _, exists := labels[replicaLabel]; exists {
				level.Debug(r.logger).Log("msg", "replica label removed", "label", replicaLabel)
				delete(labels, replicaLabel)
				metrics.modified.WithLabelValues(replicaRemovedMeta).Inc()
			}
		}
		metas[u].Thanos.Labels = labels
	}
	return nil
}

// ConsistencyDelayMetaFilter is a MetaFetcher filter that filters out blocks that are created before a specified consistency delay.
// Not go-routine safe.
type ConsistencyDelayMetaFilter struct {
	logger           log.Logger
	consistencyDelay time.Duration
}

// NewConsistencyDelayMetaFilter creates ConsistencyDelayMetaFilter.
func NewConsistencyDelayMetaFilter(logger log.Logger, consistencyDelay time.Duration, reg prometheus.Registerer) *ConsistencyDelayMetaFilter {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "consistency_delay_seconds",
		Help: "Configured consistency delay in seconds.",
	}, func() float64 {
		return consistencyDelay.Seconds()
	})

	return &ConsistencyDelayMetaFilter{
		logger:           logger,
		consistencyDelay: consistencyDelay,
	}
}

// Filter filters out blocks that filters blocks that have are created before a specified consistency delay.
func (f *ConsistencyDelayMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, metrics *syncMetrics, _ bool) error {
	for id, meta := range metas {
		// TODO(khyatisoneji): Remove the checks about Thanos Source
		//  by implementing delete delay to fetch metas.
		// TODO(bwplotka): Check consistency delay based on file upload / modification time instead of ULID.
		if ulid.Now()-id.Time() < uint64(f.consistencyDelay/time.Millisecond) &&
			meta.Thanos.Source != metadata.BucketRepairSource &&
			meta.Thanos.Source != metadata.CompactorSource &&
			meta.Thanos.Source != metadata.CompactorRepairSource {

			level.Debug(f.logger).Log("msg", "block is too fresh for now", "block", id)
			metrics.synced.WithLabelValues(tooFreshMeta).Inc()
			delete(metas, id)
		}
	}

	return nil
}

// IgnoreDeletionMarkFilter is a filter that filters out the blocks that are marked for deletion after a given delay.
// The delay duration is to make sure that the replacement block can be fetched before we filter out the old block.
// Delay is not considered when computing DeletionMarkBlocks map.
// Not go-routine safe.
type IgnoreDeletionMarkFilter struct {
	logger          log.Logger
	delay           time.Duration
	bkt             objstore.BucketReader
	deletionMarkMap map[ulid.ULID]*metadata.DeletionMark
}

// NewIgnoreDeletionMarkFilter creates IgnoreDeletionMarkFilter.
func NewIgnoreDeletionMarkFilter(logger log.Logger, bkt objstore.BucketReader, delay time.Duration) *IgnoreDeletionMarkFilter {
	return &IgnoreDeletionMarkFilter{
		logger: logger,
		bkt:    bkt,
		delay:  delay,
	}
}

// DeletionMarkBlocks returns block ids that were marked for deletion.
func (f *IgnoreDeletionMarkFilter) DeletionMarkBlocks() map[ulid.ULID]*metadata.DeletionMark {
	return f.deletionMarkMap
}

// Filter filters out blocks that are marked for deletion after a given delay.
// It also returns the blocks that can be deleted since they were uploaded delay duration before current time.
func (f *IgnoreDeletionMarkFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, m *syncMetrics, _ bool) error {
	f.deletionMarkMap = make(map[ulid.ULID]*metadata.DeletionMark)

	for id := range metas {
		deletionMark, err := metadata.ReadDeletionMark(ctx, f.bkt, f.logger, id.String())
		if err == metadata.ErrorDeletionMarkNotFound {
			continue
		}
		if errors.Cause(err) == metadata.ErrorUnmarshalDeletionMark {
			level.Warn(f.logger).Log("msg", "found partial deletion-mark.json; if we will see it happening often for the same block, consider manually deleting deletion-mark.json from the object storage", "block", id, "err", err)
			continue
		}
		if err != nil {
			return err
		}
		f.deletionMarkMap[id] = deletionMark
		if time.Since(time.Unix(deletionMark.DeletionTime, 0)).Seconds() > f.delay.Seconds() {
			m.synced.WithLabelValues(markedForDeletionMeta).Inc()
			delete(metas, id)
		}
	}
	return nil
}
