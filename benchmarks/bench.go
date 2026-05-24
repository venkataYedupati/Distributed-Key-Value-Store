package benchmarks

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	benchmarkBaseURL     = "http://localhost:8080"
	benchmarkKeyCount    = 100000
	benchmarkResultsFile = "benchmarks/results.txt"
)

var benchmarkClient = &http.Client{Timeout: 2 * time.Second}

func TestMain(m *testing.M) {
	if flag.Lookup("test.bench") != nil && flag.Lookup("test.bench").Value.String() != "" {
		if err := prepopulateKeys(); err != nil {
			fmt.Fprintf(os.Stderr, "prepopulate failed: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func BenchmarkGet(b *testing.B) {
	runBenchmark(b, "Sequential GET", func(i int) error {
		key := fmt.Sprintf("bench-key-%d", i%benchmarkKeyCount)
		resp, err := benchmarkClient.Get(benchmarkBaseURL + "/kv/" + key)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		return nil
	})
}

func BenchmarkSet(b *testing.B) {
	runBenchmark(b, "Sequential SET", func(i int) error {
		key := fmt.Sprintf("bench-set-%d", i)
		payload, _ := json.Marshal(map[string]any{"value": randomValue(), "ttl": 60})
		req, _ := http.NewRequest(http.MethodPut, benchmarkBaseURL+"/kv/"+key, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := benchmarkClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		return nil
	})
}

func BenchmarkParallelGet(b *testing.B) {
	hist := newHistogram()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			start := time.Now()
			key := fmt.Sprintf("bench-key-%d", rand.Intn(benchmarkKeyCount))
			resp, err := benchmarkClient.Get(benchmarkBaseURL + "/kv/" + key)
			if err == nil {
				_, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			hist.RecordValue(time.Since(start).Microseconds())
		}
	})
	reportBenchmark(b, "Parallel GET", hist)
}

func BenchmarkParallelSet(b *testing.B) {
	hist := newHistogram()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			start := time.Now()
			key := fmt.Sprintf("bench-parallel-set-%d", rand.Int())
			payload, _ := json.Marshal(map[string]any{"value": randomValue(), "ttl": 60})
			req, _ := http.NewRequest(http.MethodPut, benchmarkBaseURL+"/kv/"+key, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, err := benchmarkClient.Do(req)
			if err == nil {
				_, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			hist.RecordValue(time.Since(start).Microseconds())
		}
	})
	reportBenchmark(b, "Parallel SET", hist)
}

func BenchmarkMixedReadWrite(b *testing.B) {
	hist := newHistogram()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			start := time.Now()
			if rand.Intn(10) < 8 {
				key := fmt.Sprintf("bench-key-%d", rand.Intn(benchmarkKeyCount))
				resp, err := benchmarkClient.Get(benchmarkBaseURL + "/kv/" + key)
				if err == nil {
					_, _ = io.ReadAll(resp.Body)
					resp.Body.Close()
				}
			} else {
				key := fmt.Sprintf("bench-mixed-set-%d", rand.Int())
				payload, _ := json.Marshal(map[string]any{"value": randomValue(), "ttl": 60})
				req, _ := http.NewRequest(http.MethodPut, benchmarkBaseURL+"/kv/"+key, bytes.NewReader(payload))
				req.Header.Set("Content-Type", "application/json")
				resp, err := benchmarkClient.Do(req)
				if err == nil {
					_, _ = io.ReadAll(resp.Body)
					resp.Body.Close()
				}
			}
			hist.RecordValue(time.Since(start).Microseconds())
		}
	})
	reportBenchmark(b, "Mixed 80/20", hist)
}

func runBenchmark(b *testing.B, name string, fn func(i int) error) {
	hist := newHistogram()
	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opStart := time.Now()
		if err := fn(i); err != nil {
			b.Fatalf("%s failed: %v", name, err)
		}
		hist.RecordValue(time.Since(opStart).Microseconds())
	}
	b.StopTimer()
	reportBenchmark(b, name, hist)
	b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "ops/sec")
}

func reportBenchmark(b *testing.B, name string, hist *hdrhistogram.Histogram) {
	if hist == nil || hist.TotalCount() == 0 {
		return
	}
	p50 := time.Duration(hist.ValueAtQuantile(50)) * time.Microsecond
	p95 := time.Duration(hist.ValueAtQuantile(95)) * time.Microsecond
	p99 := time.Duration(hist.ValueAtQuantile(99)) * time.Microsecond
	b.ReportMetric(float64(hist.TotalCount()), "ops")
	b.ReportMetric(float64(p50.Microseconds()), "p50-us")
	b.ReportMetric(float64(p95.Microseconds()), "p95-us")
	b.ReportMetric(float64(p99.Microseconds()), "p99-us")
	_ = appendResult(name, b.N, p50, p95, p99)
}

func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(1, int64(10*time.Second/time.Microsecond), 3)
}

func appendResult(name string, ops int, p50, p95, p99 time.Duration) error {
	if err := os.MkdirAll(filepath.Dir(benchmarkResultsFile), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(benchmarkResultsFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%s ops=%d p50=%s p95=%s p99=%s\n", name, ops, p50, p95, p99)
	return err
}

func prepopulateKeys() error {
	for i := 0; i < benchmarkKeyCount; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		payload, _ := json.Marshal(map[string]any{"value": randomValue(), "ttl": 600})
		req, _ := http.NewRequest(http.MethodPut, benchmarkBaseURL+"/kv/"+key, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := benchmarkClient.Do(req)
		if err != nil {
			return err
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	return nil
}

func randomValue() string {
	return fmt.Sprintf("value-%08x", rand.Uint32())
}

func sortDurations(values []time.Duration) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
}
