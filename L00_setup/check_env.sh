#!/usr/bin/env bash
# L00 sanity check -- run from anywhere:  bash labs/L00_setup/check_env.sh
# Exit code 0 = ready, 1 = at least one FAIL.  WARNs are advisory.
#
# Works on macOS (Apple Silicon / Intel), Linux and Windows+WSL2.
set -uo pipefail

LABS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAILS=0; WARNS=0
if [ -t 1 ]; then G=$'\e[32m'; Y=$'\e[33m'; R=$'\e[31m'; N=$'\e[0m'; else G=; Y=; R=; N=; fi
ok()   { printf "  ${G}[ OK ]${N} %s\n" "$*"; }
warn() { printf "  ${Y}[WARN]${N} %s\n" "$*"; WARNS=$((WARNS+1)); }
fail() { printf "  ${R}[FAIL]${N} %s\n" "$*"; FAILS=$((FAILS+1)); }

# Ports used by the labs (only ONE lab stack runs at a time, so a port only
# matters for the lab you are about to start).
PORTS="8020:HDFS-RPC(L01) 9870:NameNode-UI(L01) 8088:YARN-UI(L01) 8042:NodeManager-UI(L01) \
19888:JobHistory-UI(L01) 10000:HiveServer2(L03) 10002:HiveServer2-UI(L03) 9083:Metastore(L03) \
5432:Postgres(L04) 4040:Spark-UI(L04-L06) 9092:Kafka(L07) 8080:Web-UIs(L07+)"

echo "== 1. Host"
OS=$(uname -s); ARCH=$(uname -m)
ok "OS=${OS} arch=${ARCH}"
[ "${ARCH}" = "arm64" ] || [ "${ARCH}" = "aarch64" ] && ok "ARM64 host: all course images are native arm64"

if [ "${OS}" = "Darwin" ]; then
  TOTAL_MB=$(( $(sysctl -n hw.memsize) / 1024 / 1024 ))
  PAGE=$(sysctl -n hw.pagesize)
  FREE_PAGES=$(vm_stat | awk '/Pages free/ {f=$3} /Pages inactive/ {i=$3} /Pages speculative/ {s=$3} END {gsub("\\.","",f); gsub("\\.","",i); gsub("\\.","",s); print f+i+s}')
  AVAIL_MB=$(( FREE_PAGES * PAGE / 1024 / 1024 ))
else
  TOTAL_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
  AVAIL_MB=$(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo)
fi
if [ "${TOTAL_MB}" -ge 7500 ]; then ok "Host RAM ${TOTAL_MB} MB"; else warn "Host RAM ${TOTAL_MB} MB (< 8 GB: expect swapping)"; fi
if [ "${AVAIL_MB}" -ge 3000 ]; then ok "Available RAM ~${AVAIL_MB} MB"
else warn "Available RAM ~${AVAIL_MB} MB -- close browsers/IDEs before starting a lab stack"; fi

DISK_FREE_GB=$(df -Pk "${LABS_DIR}" | awk 'NR==2 {print int($4/1024/1024)}')
if [ "${DISK_FREE_GB}" -ge 20 ]; then ok "Free disk ${DISK_FREE_GB} GB"
elif [ "${DISK_FREE_GB}" -ge 10 ]; then warn "Free disk ${DISK_FREE_GB} GB (images for all labs need ~12-15 GB; remove old lab images when done)"
else fail "Free disk ${DISK_FREE_GB} GB (need >= 10 GB)"; fi

echo "== 2. Tools"
for t in git curl python3; do
  if command -v "$t" >/dev/null 2>&1; then ok "$t: $($t --version 2>&1 | head -1)"; else warn "$t not found"; fi
done
if command -v python3 >/dev/null 2>&1; then
  PYV=$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')
  python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' \
    && ok "python3 ${PYV} (>= 3.11)" || warn "python3 ${PYV} (< 3.11; host Python is only needed for small helper scripts)"
fi

echo "== 3. Docker"
if ! command -v docker >/dev/null 2>&1; then
  fail "docker CLI not found -- install Docker Desktop (macOS/Windows) or Docker Engine (Linux)"
else
  ok "docker CLI: $(docker --version)"
  if docker info >/dev/null 2>&1; then
    ok "Docker daemon is running (server $(docker info --format '{{.ServerVersion}}'))"
    DMEM_MB=$(( $(docker info --format '{{.MemTotal}}') / 1024 / 1024 ))
    DCPU=$(docker info --format '{{.NCPU}}')
    if [ "${DMEM_MB}" -ge 5500 ]; then ok "Docker memory ${DMEM_MB} MB, ${DCPU} CPUs"
    elif [ "${DMEM_MB}" -ge 3800 ]; then warn "Docker memory ${DMEM_MB} MB -- labs L01-L06 fit, but 6 GB is recommended (Docker Desktop > Settings > Resources)"
    else fail "Docker memory ${DMEM_MB} MB -- raise to >= 4 GB (6 GB recommended)"; fi
    DARCH=$(docker info --format '{{.Architecture}}')
    ok "Docker VM architecture: ${DARCH}"
  else
    fail "Docker daemon not reachable -- start Docker Desktop (macOS: open -a Docker) and retry"
  fi
  if docker compose version >/dev/null 2>&1; then
    ok "Compose: $(docker compose version --short 2>/dev/null || docker compose version)"
  else
    fail "'docker compose' (v2 plugin) not available -- the old 'docker-compose' v1 is NOT supported"
  fi
fi

echo "== 4. Ports (in use = another program already listens there)"
port_busy() {
  if command -v nc >/dev/null 2>&1; then nc -z -w 1 127.0.0.1 "$1" >/dev/null 2>&1
  else (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1; fi
}
for entry in ${PORTS}; do
  p=${entry%%:*}; name=${entry#*:}
  if port_busy "$p"; then
    owner=""
    command -v lsof >/dev/null 2>&1 && owner=$(lsof -nP -iTCP:"$p" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $1}')
    warn "port $p ($name) is in use ${owner:+by $owner} -- stop it before that lab (or it is a lab stack you left running)"
  else
    ok "port $p ($name) free"
  fi
done

echo "== 5. Datasets"
TAXI="${LABS_DIR}/datasets/data/taxi"
if ls "${TAXI}"/yellow_tripdata_*.parquet >/dev/null 2>&1; then
  ok "taxi Parquet: $(ls "${TAXI}"/yellow_tripdata_*.parquet | xargs -n1 basename | tr '\n' ' ')"
else
  warn "no taxi Parquet yet -- run: bash labs/datasets/download_taxi.sh"
fi
[ -s "${TAXI}/taxi_zone_lookup.csv" ] && ok "taxi_zone_lookup.csv present" || warn "taxi_zone_lookup.csv missing -- run: bash labs/datasets/download_taxi.sh"
[ -s "${LABS_DIR}/datasets/olist/init/02_seed.sql" ] && ok "Olist seed present" \
  || fail "labs/datasets/olist/init/02_seed.sql missing -- run: python3 labs/datasets/olist/generate_seed.py"

echo "== 6. Running lab containers"
if docker info >/dev/null 2>&1; then
  RUNNING=$(docker ps --format '{{.Names}}' | grep -Ei '^(l0[0-9]|l1[0-2]|cap)' || true)
  if [ -n "${RUNNING}" ]; then
    warn "lab containers still running (run 'docker compose down' in that lab): $(echo ${RUNNING})"
  else
    ok "no lab stack running"
  fi
fi

echo
if [ "${FAILS}" -eq 0 ]; then
  echo "${G}READY${N}: ${WARNS} warning(s), 0 failures."
  exit 0
else
  echo "${R}NOT READY${N}: ${FAILS} failure(s), ${WARNS} warning(s). Fix the FAIL lines above."
  exit 1
fi
