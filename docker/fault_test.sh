#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# Robust fault-injection test for the 4-step sequence using container-internal HTTP
# Uses `docker compose exec -T` and pipes JSON via stdin to avoid quoting/TTY issues.

info() { echo "[INFO] $*"; }
err() { echo "[ERROR] $*" >&2; }

# helper: get status JSON from node (returns {} on error)
node_status() {
  docker compose exec -T "node${1}" curl -s "http://localhost:$((8080+${1}))/admin/status" || echo "{}"
}

# find leader (wait up to 30s)
leader=""
for t in $(seq 1 30); do
  for i in 0 1 2 3 4; do
    role=$(node_status "$i" | jq -r '.role // empty' 2>/dev/null || echo "")
    if [ "$role" = "leader" ]; then leader=$i; break; fi
  done
  if [ -n "$leader" ]; then break; fi
  sleep 1
done
if [ -z "$leader" ]; then err "no leader found after 30s"; exit 2; fi
info "leader is node${leader}"

# wait for all nodes to respond to /admin/status AND for exactly one leader
# This ensures a clean cluster state before running tests.
wait_all_nodes() {
  info "waiting for all nodes to respond and exactly one leader (up to 60s)"
  for t in $(seq 1 60); do
    all_ok=true
    leader_count=0
    leader_node=""
    for i in 0 1 2 3 4; do
      s=$(docker compose exec -T "node${i}" curl -s http://localhost:$((8080+i))/admin/status || echo "")
      if [ -z "$s" ]; then
        all_ok=false
        break
      fi
      role=$(echo "$s" | jq -r '.role // empty' 2>/dev/null || echo "")
      if [ "$role" = "leader" ]; then
        leader_count=$((leader_count+1))
        leader_node=$i
      fi
    done
    if [ "$all_ok" = true ] && [ "$leader_count" -eq 1 ]; then
      info "all nodes responsive and single leader node${leader_node}"
      leader=$leader_node
      return 0
    fi
    sleep 1
  done
  err "not all nodes responsive with a single leader after wait"
  return 1
}

wait_all_nodes || true
# STEP 1: baseline writes/reads
info "STEP 1: baseline writes/reads (15 keys)"
passes=0
fails=0
for i in $(seq 1 15); do
  writer=$((i % 5))
  reader=$(((i + 1) % 5))
  key="k${i}"
  val="v${i}"
  info "PUT $key via node${writer}"
  # Use jq on the host to safely construct JSON and pipe into container curl
  jq -n --arg v "$val" '{value:$v, ttl: ""}' | docker compose exec -T "node${writer}" sh -c "cat | curl -s -X PUT -H 'Content-Type: application/json' -d @- http://localhost:$((8080+writer))/kv/$key" >/dev/null || true
  got=$(docker compose exec -T "node${reader}" curl -s "http://localhost:$((8080+reader))/kv/$key" || echo "")
  if echo "$got" | grep -q "\"value\":\"${val}\""; then
    passes=$((passes+1))
  else
    err "mismatch for $key (got: $got)"
    fails=$((fails+1))
  fi
done
info "STEP1 results: pass=${passes} fail=${fails}"

# STEP 2: stop one non-leader follower and write
info "STEP 2: stop one non-leader follower and write"
stop_node=""
for i in 0 1 2 3 4; do
  if [ "$i" -ne "$leader" ]; then stop_node=$i; break; fi
done
if [ -z "$stop_node" ]; then err "no follower to stop"; exit 2; fi
info "stopping node${stop_node}"
docker compose stop "node${stop_node}"
sleep 1
  info "writing down-test to current leader node${leader}"
  jq -n --arg v "down-value" '{value:$v, ttl: ""}' | docker compose exec -T "node${leader}" sh -c "cat | curl -s -X PUT -H 'Content-Type: application/json' -d @- http://localhost:$((8080+leader))/kv/down-test" >/dev/null || true
readback=$(docker compose exec -T "node${leader}" curl -s "http://localhost:$((8080+leader))/kv/down-test" || echo "")
info "GET down-test from leader -> $readback"

# STEP 3: stop leader and verify failover
info "STEP 3: stop leader node${leader} and wait for a new leader"
docker compose stop "node${leader}"
sleep 1
new_leader=""
for t in $(seq 1 12); do
  for i in 0 1 2 3 4; do
    role=$(node_status "$i" | jq -r '.role // empty' 2>/dev/null || echo "")
    if [ "$role" = "leader" ]; then new_leader=$i; break; fi
  done
  if [ -n "$new_leader" ]; then break; fi
  sleep 1
done
if [ -z "$new_leader" ]; then err "no new leader elected"; else info "new leader is node${new_leader}"; fi
if [ -n "$new_leader" ]; then
  info "writing leader-failover to node${new_leader}"
  jq -n --arg v "leader-ok" '{value:$v, ttl: ""}' | docker compose exec -T "node${new_leader}" sh -c "cat | curl -s -X PUT -H 'Content-Type: application/json' -d @- http://localhost:$((8080+new_leader))/kv/leader-failover" >/dev/null || true
  gotlf=$(docker compose exec -T "node${new_leader}" curl -s "http://localhost:$((8080+new_leader))/kv/leader-failover" || echo "")
  info "GET leader-failover -> $gotlf"
fi

# STEP 4: restart stopped nodes and wait for catch-up
info "STEP 4: restart previously stopped nodes (node${stop_node} and node${leader})"
docker compose start "node${stop_node}" || true
docker compose start "node${leader}" || true
sleep 2
info "waiting for nodes to catch up (polling)"
for t in $(seq 1 12); do
  info "poll $t"
  cluster_commit=$(docker compose exec -T node0 curl -s http://localhost:8080/admin/status | jq -r '.commit_index // 0' || echo 0)
  info "cluster_commit=$cluster_commit"
  all_ok=true
  for i in 0 1 2 3 4; do
    status=$(docker compose exec -T "node${i}" curl -s "http://localhost:$((8080+i))/admin/status" || echo "{}")
    idx=$(echo "$status" | jq -r '.commit_index // 0' 2>/dev/null || echo 0)
    info "node${i} commit_index=$idx"
    if [ "$idx" -lt "$cluster_commit" ]; then all_ok=false; fi
  done
  if [ "$all_ok" = true ]; then info "All nodes caught up"; break; fi
  sleep 2
done

# Final verification: GET keys from node0
info "Final GET checks (from node0)"
for k in $(seq 1 15); do
  echo "k${k} -> $(docker compose exec -T node0 curl -s http://localhost:8080/kv/k${k} || true)"
done
for key in down-test leader-failover; do
  echo "$key -> $(docker compose exec -T node0 curl -s http://localhost:8080/kv/$key || true)"
done

info "Fault test script completed"
exit 0
