package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

type kvRequest struct {
	Value string `json:"value,omitempty"`
	TTL   int64  `json:"ttl,omitempty"`
}

type clientConfig struct {
	nodes   []string
	timeout time.Duration
}

func main() {
	if err := newRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	cfg := &clientConfig{nodes: []string{"http://127.0.0.1:8080"}, timeout: 2 * time.Second}
	root := &cobra.Command{Use: "kv", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().StringSliceVar(&cfg.nodes, "nodes", cfg.nodes, "cluster endpoints (comma-separated or repeated)")
	root.PersistentFlags().DurationVar(&cfg.timeout, "timeout", cfg.timeout, "request timeout")
	root.AddCommand(newGetCommand(cfg), newSetCommand(cfg), newDeleteCommand(cfg), newStatusCommand(cfg), newBenchCommand(cfg))
	return root
}

func newGetCommand(cfg *clientConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Get a value",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body, err := failoverRequest(cfg, http.MethodGet, "/kv/"+args[0], nil)
			if err != nil {
				return err
			}
			fmt.Println(string(body))
			return nil
		},
	}
}

func newSetCommand(cfg *clientConfig) *cobra.Command {
	var ttlSeconds int64
	cmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			payload, _ := json.Marshal(kvRequest{Value: args[1], TTL: ttlSeconds})
			body, err := failoverRequest(cfg, http.MethodPut, "/kv/"+args[0], payload)
			if err != nil {
				return err
			}
			fmt.Println(string(body))
			return nil
		},
	}
	cmd.Flags().Int64Var(&ttlSeconds, "ttl", 0, "TTL in seconds")
	return cmd
}

func newDeleteCommand(cfg *clientConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body, err := failoverRequest(cfg, http.MethodDelete, "/kv/"+args[0], nil)
			if err != nil {
				return err
			}
			fmt.Println(string(body))
			return nil
		},
	}
}

func newStatusCommand(cfg *clientConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			body, err := failoverRequest(cfg, http.MethodGet, "/admin/status", nil)
			if err != nil {
				return err
			}
			fmt.Println(string(body))
			return nil
		},
	}
}

func newBenchCommand(cfg *clientConfig) *cobra.Command {
	var ops int
	var concurrency int
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run a client benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			if ops <= 0 {
				ops = 100000
			}
			if concurrency <= 0 {
				concurrency = 50
			}
			durations := runBenchmark(cfg, ops, concurrency)
			if len(durations) == 0 {
				return errors.New("benchmark did not execute any operations")
			}
			sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
			opsPerSec := float64(ops) / durations[len(durations)-1].Seconds()
			fmt.Printf("ops=%d concurrency=%d ops/sec=%.2f p50=%s p95=%s p99=%s\n", ops, concurrency, opsPerSec, percentile(durations, 50), percentile(durations, 95), percentile(durations, 99))
			return nil
		},
	}
	cmd.Flags().IntVar(&ops, "ops", 100000, "number of SET operations")
	cmd.Flags().IntVar(&concurrency, "concurrency", 50, "number of workers")
	return cmd
}

func failoverRequest(cfg *clientConfig, method, path string, payload []byte) ([]byte, error) {
	var lastErr error
	nodes := normalizeNodes(cfg.nodes)
	for _, node := range nodes {
		body, statusCode, location, err := doRequest(node, cfg.timeout, method, path, payload)
		if err != nil {
			lastErr = err
			continue
		}
		if statusCode >= 200 && statusCode < 300 {
			return body, nil
		}
		if statusCode >= 300 && statusCode < 400 && location != "" {
			redirectURL := resolveLocation(node, location)
			redirectBody, redirectStatus, _, err := doRequest(redirectURL, cfg.timeout, method, path, payload)
			if err == nil && redirectStatus >= 200 && redirectStatus < 300 {
				return redirectBody, nil
			}
			lastErr = err
			continue
		}
		lastErr = fmt.Errorf("%s returned %d: %s", node, statusCode, strings.TrimSpace(string(body)))
	}
	if lastErr == nil {
		lastErr = errors.New("no nodes responded")
	}
	return nil, lastErr
}

func doRequest(baseURL string, timeout time.Duration, method, path string, payload []byte) ([]byte, int, string, error) {
	req, err := http.NewRequest(method, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, "", err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, resp.Header.Get("Location"), nil
}

func normalizeNodes(nodes []string) []string {
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		if !strings.Contains(node, "://") {
			node = "http://" + node
		}
		out = append(out, node)
	}
	if len(out) == 0 {
		return []string{"http://127.0.0.1:8080"}
	}
	return out
}

func resolveLocation(baseURL, location string) string {
	if strings.Contains(location, "://") {
		return location
	}
	return strings.TrimRight(baseURL, "/") + location
}

func runBenchmark(cfg *clientConfig, ops, concurrency int) []time.Duration {
	durations := make([]time.Duration, 0, ops)
	var mu sync.Mutex
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for opIndex := range jobs {
				key := fmt.Sprintf("bench-%d-%d", id, opIndex)
				payload, _ := json.Marshal(kvRequest{Value: randomValue()})
				start := time.Now()
				_, _ = failoverRequest(cfg, http.MethodPut, "/kv/"+key, payload)
				mu.Lock()
				durations = append(durations, time.Since(start))
				mu.Unlock()
			}
		}(worker)
	}
	for i := 0; i < ops; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return durations
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int((p / 100.0) * float64(len(values)-1))
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func randomValue() string {
	return fmt.Sprintf("value-%08x", rand.Uint32())
}
