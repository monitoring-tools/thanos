package tracing

import (
	"fmt"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type SeriesStats struct {
	span opentracing.Span

	mu         sync.RWMutex
	firstSent  bool
	seriesSent int64
	Raw        int64
	Count      int64
	Sum        int64
	Min        int64
	Max        int64
	Counter    int64
}

func NewSeriesStats(s opentracing.Span) *SeriesStats {
	return &SeriesStats{span: s}
}

func (ss *SeriesStats) LogRequest(r *storepb.SeriesRequest) {
	ss.span.SetTag("page.type", "thanos.query")
	ss.span.LogKV(
		"min_time", r.MinTime,
		"max_time", r.MaxTime,
		"duration", fmt.Sprintf("%ds", (r.MaxTime-r.MinTime)/1000),
		"max_resolution_window", r.MaxResolutionWindow,
		"matchers", r.Matchers,
		"aggregates", r.Aggregates,
		"partial_response_strategy", r.PartialResponseStrategy,
		"skip_chunks", r.SkipChunks,
	)
}

func (ss *SeriesStats) Observe(s storepb.Series) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if !ss.firstSent {
		ss.span.LogEvent("first series sent")
		ss.firstSent = true
	}

	ss.seriesSent++

	for _, chunk := range s.Chunks {
		if chunk.Raw != nil {
			ss.Raw += int64(len(chunk.Raw.Data))
		}

		if chunk.Count != nil {
			ss.Count += int64(len(chunk.Count.Data))
		}

		if chunk.Sum != nil {
			ss.Sum += int64(len(chunk.Sum.Data))
		}

		if chunk.Min != nil {
			ss.Min += int64(len(chunk.Min.Data))
		}

		if chunk.Max != nil {
			ss.Max += int64(len(chunk.Max.Data))
		}

		if chunk.Counter != nil {
			ss.Counter += int64(len(chunk.Counter.Data))
		}
	}
}

func (ss *SeriesStats) Report() {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	ss.span.LogKV(
		"total_sent", ByteCountIEC(ss.Raw+ss.Count+ss.Sum+ss.Min+ss.Max+ss.Counter),
		"raw_aggr_sent", ByteCountIEC(ss.Raw),
		"count_aggr_sent", ByteCountIEC(ss.Count),
		"sum_aggr_sent", ByteCountIEC(ss.Sum),
		"min_aggr_sent", ByteCountIEC(ss.Min),
		"max_aggr_sent", ByteCountIEC(ss.Max),
		"counter_aggr_sent", ByteCountIEC(ss.Counter),
		"series_sent", ss.seriesSent,
	)
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

type SeriesServer struct {
	stats *SeriesStats
	storepb.Store_SeriesServer
}

func NewSeriesServer(server storepb.Store_SeriesServer, req *storepb.SeriesRequest, span opentracing.Span) (srv *SeriesServer, reportFn func()) {
	stats := NewSeriesStats(span)
	stats.LogRequest(req)

	return &SeriesServer{
		Store_SeriesServer: server,
		stats:              stats,
	}, stats.Report
}

func (ss *SeriesServer) Send(r *storepb.SeriesResponse) error {
	ss.stats.Observe(*r.GetSeries())

	return ss.Store_SeriesServer.Send(r)
}
