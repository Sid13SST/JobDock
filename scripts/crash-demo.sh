#!/usr/bin/env bash
# Crash demo: enqueue a 20s job, SIGKILL the worker container running it, watch another worker
# recover and finish it. Needs: Docker with Compose v2, curl, jq, Java 21 (to build the jar).
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:8080}
SECONDS_TO_RUN=${SECONDS_TO_RUN:-20}

step() { printf '\n== %s\n' "$*"; }
job() { curl -fsS "$API/jobs/$1"; }
deadline_exceeded() { [ "$(date +%s)" -ge "$1" ]; }

step "Build the jar and start Postgres, the API and 2 workers"
./mvnw -q -B -DskipTests package
docker compose up -d --build --wait
docker compose ps

step "Enqueue a demo.sleep job that runs for ${SECONDS_TO_RUN}s"
id=$(curl -fsS -X POST "$API/jobs" -H 'Content-Type: application/json' \
  -d "{\"type\":\"demo.sleep\",\"payload\":{\"seconds\":${SECONDS_TO_RUN}},\"maxAttempts\":3}" | jq -r .id)
echo "job id: $id"

step "Wait until a worker has claimed it"
deadline=$(( $(date +%s) + 60 ))
owner=""
until [ -n "$owner" ]; do
  deadline_exceeded "$deadline" && { echo "job was not claimed within 60s" >&2; exit 1; }
  owner=$(job "$id" | jq -r 'select(.status == "RUNNING") | .lockedBy')
  [ -n "$owner" ] || sleep 0.5
done
# Worker ids are <hostname>-<pid>-<random>-<thread>; a container's hostname is its short id.
container=${owner%%-*}
echo "RUNNING, attempt $(job "$id" | jq .attempts), owner $owner (container $container)"
sleep 3   # let the handler get properly into its sleep

step "kill -9 the worker container that holds the job"
docker kill --signal KILL "$container" >/dev/null
echo "killed $container; status in the database is still: $(job "$id" | jq -r .status) (lease not expired yet)"

step "Watch the job: lease expires -> reaper requeues -> the other worker runs it again"
deadline=$(( $(date +%s) + SECONDS_TO_RUN + 60 ))
last=""
while :; do
  deadline_exceeded "$deadline" && { echo "job did not finish in time" >&2; exit 1; }
  j=$(job "$id")
  now=$(jq -r '"\(.status) attempt=\(.attempts) owner=\(.lockedBy)"' <<<"$j")
  [ "$now" != "$last" ] && { echo "$(date +%T) $now"; last=$now; }
  [ "$(jq -r .status <<<"$j")" = "SUCCEEDED" ] && break
  sleep 0.5
done

step "Final job state"
job "$id" | jq '{id, status, attempts, lastError, startedAt, finishedAt}'

step "Worker logs for this job"
docker compose logs --no-color worker 2>/dev/null | grep -E "$id" || true

step "Metrics"
curl -fsS "$API/metrics" | jq .

cat <<MSG

The killed worker container stays stopped. Restart it with: docker compose up -d --wait
Tear everything down with:                                  docker compose down -v
MSG
