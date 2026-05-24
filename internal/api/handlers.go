package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/hash"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Handlers struct {
	cfg    config.Config
	node   *raft.Node
	engine *store.Engine
	ring   *hash.Ring
}

func NewHandlers(cfg config.Config, node *raft.Node, engine *store.Engine, ring *hash.Ring) *Handlers {
	return &Handlers{cfg: cfg, node: node, engine: engine, ring: ring}
}

func (h *Handlers) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)
	mux.HandleFunc("/admin/status", h.handleStatus)
	mux.HandleFunc("/admin/distribution", h.handleDistribution)
	mux.HandleFunc("/kv/", h.handleKV)
	return mux
}

type kvPayload struct {
	Value string `json:"value"`
	TTL   string `json:"ttl,omitempty"`
}

func (h *Handlers) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		h.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing key"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		entry, err := h.node.Get(r.Context(), key)
		if err != nil {
			if err == store.ErrNotFound {
				h.writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
				return
			}
			h.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		resp := map[string]any{"key": entry.Key, "value": entry.Value}
		if entry.ExpiresAt != nil {
			resp["expires_at"] = entry.ExpiresAt.Format(time.RFC3339Nano)
		}
		h.writeJSON(w, http.StatusOK, resp)
	case http.MethodPut:
		var payload kvPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			h.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
			return
		}
		ttl, err := time.ParseDuration(payload.TTL)
		if payload.TTL != "" && err != nil {
			h.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid ttl"})
			return
		}
		if err := h.node.Write(r.Context(), key, payload.Value, ttl); err != nil {
			h.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		h.writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		if err := h.node.Delete(r.Context(), key); err != nil {
			h.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		h.writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "node": h.cfg.NodeID})
}

func (h *Handlers) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := h.node.Status()
	status["ring"] = h.ring.Stats()
	engineStats := h.engine.Stats()
	if keys, ok := engineStats["keys"].(int); ok {
		h.node.Metrics().SetStoreKeys(keys)
	}
	status["engine"] = engineStats
	h.writeJSON(w, http.StatusOK, status)
}

func (h *Handlers) handleDistribution(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, h.ring.Stats())
}

func (h *Handlers) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
