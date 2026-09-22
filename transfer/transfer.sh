#!/bin/bash
set -euo pipefail

DATABASE_POSTGRES_USER=postgres
DATABASE_POSTGRES_PASSWORD=postgres
DATABASE_DB=openaqdev
DATABASE_HOST=localhost
DATABASE_PORT=5777

SOURCE_URI="${TRANSFER_URI:-}"
LOCAL_URI="postgresql://${DATABASE_POSTGRES_USER}:${DATABASE_POSTGRES_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DB}"

SQL_DIR="${SQL_DIR:-$(dirname "$0")}"

DEFAULT_TABLES=(
    entities
    providers
    measurands
    sensor_nodes
    instruments
    sensor_systems
    sensors
)

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Transfer tables between databases via direct stream, local CSV files,
or S3-hosted CSV files.

Actions:
  --action ACTION    'direct' (default): stream source -> local db
                     'dump':   source -> CSV files (local or S3)
                     'load':   CSV files (local or S3) -> local db

Options:
  --target WHERE     'local' (default) or 's3'; used with dump/load
  --path PATH        Local directory or s3://bucket/prefix
                     (default local: ./snapshots)
  --snapshot NAME    Subfolder under --path (default: UTC timestamp)
  --uri URI          Source database URI (or TRANSFER_URI env var)
                     Required for 'direct' and 'dump' actions.
  --local-uri URI    Local database URI
  --tables LIST      Comma-separated tables (default: parents to children)
  --mode MODE        'append' (default) or 'replace' (truncates first)
  -h, --help         Show this help and exit

Examples:
  # Original behavior: stream prod -> local
  TRANSFER_URI=postgresql://... $(basename "$0")

  # Dump prod to S3
  $(basename "$0") --action dump --target s3 \\
    --path s3://openaq-snapshots --snapshot 2025-01-15 \\
    --uri postgresql://...

  # Load from S3 into local
  $(basename "$0") --action load --target s3 \\
    --path s3://openaq-snapshots --snapshot 2025-01-15 \\
    --mode replace

  # Dump to local files for inspection
  $(basename "$0") --action dump --target local \\
    --path ./snapshots --uri postgresql://...
EOF
}

ACTION=direct
TARGET=local
PATH_ARG=""
SNAPSHOT=""
TABLES=()
MODE=append

while [[ $# -gt 0 ]]; do
    case "$1" in
        --action)     ACTION="$2"; shift 2;;
        --target)     TARGET="$2"; shift 2;;
        --path)       PATH_ARG="$2"; shift 2;;
        --snapshot)   SNAPSHOT="$2"; shift 2;;
        --tables)     IFS=',' read -ra TABLES <<< "$2"; shift 2;;
        --mode)       MODE="$2"; shift 2;;
        --uri)        SOURCE_URI="$2"; shift 2;;
        --local-uri)  LOCAL_URI="$2"; shift 2;;
        -h|--help)    usage; exit 0;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

[[ ${#TABLES[@]} -eq 0 ]] && TABLES=("${DEFAULT_TABLES[@]}")
[[ -z "$SNAPSHOT" ]] && SNAPSHOT="$(date -u +%Y%m%dT%H%M%SZ)"

case "$ACTION" in
    direct|dump|load) ;;
    *) echo "Unknown action: $ACTION"; exit 1;;
esac
case "$TARGET" in
    local|s3) ;;
    *) echo "Unknown target: $TARGET"; exit 1;;
esac
case "$MODE" in
    append|replace) ;;
    *) echo "Unknown mode: $MODE"; exit 1;;
esac

redact() { echo "$1" | sed -E 's|://[^:]+:[^@]+@|://**:**@|'; }

# ---------------------------------------------------------------------------
# Helpers for local vs S3 IO
# ---------------------------------------------------------------------------

# Full path/URI to a table file, e.g. ./snapshots/foo/sensors.csv.gz
snapshot_root() {
    if [[ "$TARGET" == "s3" ]]; then
        echo "${PATH_ARG%/}/${SNAPSHOT}"
    else
        echo "${PATH_ARG:-./snapshots}/${SNAPSHOT}"
    fi
}

table_file() {
    local t="$1"
    echo "$(snapshot_root)/${t}.csv.gz"
}

manifest_file() {
    echo "$(snapshot_root)/manifest.json"
}

# Write stdin to a destination (local file or s3://)
write_to() {
    local dest="$1"
    if [[ "$dest" == s3://* ]]; then
        aws s3 cp - "$dest" --only-show-errors
    else
        mkdir -p "$(dirname "$dest")"
        cat > "$dest"
    fi
}

# Read a destination (local file or s3://) to stdout
read_from() {
    local src="$1"
    if [[ "$src" == s3://* ]]; then
        aws s3 cp "$src" - --only-show-errors
    else
        cat "$src"
    fi
}

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------

need_source() {
    : "${SOURCE_URI:?Set TRANSFER_URI env var or pass --uri}"
    if ! psql "$SOURCE_URI" -c '\q' >/dev/null 2>&1; then
        echo "Error: cannot connect to source database at $(redact "$SOURCE_URI")" >&2
        exit 1
    fi
}

need_local() {
    if ! psql "$LOCAL_URI" -c '\q' >/dev/null 2>&1; then
        echo "Error: cannot connect to local database at $(redact "$LOCAL_URI")" >&2
        exit 1
    fi
}

confirm_truncate() {
    local list="$1"
    read -p "This will TRUNCATE ${list} CASCADE on $(redact "$LOCAL_URI"). Continue? [y/N] " ans
    [[ "$ans" == "y" ]] || exit 1
}

reset_sequences() {
    echo
    echo "Resetting sequences..."
    for t in "${TABLES[@]}"; do
        psql "$LOCAL_URI" -tA -v ON_ERROR_STOP=1 -c "
            SELECT setval(
                pg_get_serial_sequence('public.${t}', a.attname),
                COALESCE((SELECT MAX(${t}_id) FROM public.${t}), 1),
                true
            )
            FROM pg_attribute a
            WHERE a.attrelid = 'public.${t}'::regclass
              AND pg_get_serial_sequence('public.${t}', a.attname) IS NOT NULL
            LIMIT 1;
        " >/dev/null || echo "  (no sequence for ${t})"
    done
}

# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

do_direct() {
    need_source
    need_local

    local dump_args=()
    for t in "${TABLES[@]}"; do
        dump_args+=(--table="public.${t}")
        dump_args+=(--exclude-table-data="public.${t}_*_seq")
    done

    local truncate_list
    truncate_list=$(IFS=,; echo "${TABLES[*]}")

    local pre_sql="-- append mode: existing rows preserved"
    if [[ "$MODE" == "replace" ]]; then
        confirm_truncate "$truncate_list"
        pre_sql="TRUNCATE ${truncate_list} RESTART IDENTITY CASCADE;"
    fi

    echo "Streaming pg_dump -> psql ..."
    pg_dump "$SOURCE_URI" --data-only "${dump_args[@]}" \
      | psql "$LOCAL_URI" \
            --single-transaction \
            -v ON_ERROR_STOP=1 \
            -c "SET search_path = public" \
            -c "$pre_sql" \
            -f -

    reset_sequences
}

do_dump() {
    need_source

    local root
    root="$(snapshot_root)"
    echo "Dumping to: ${root}"

    # Manifest we'll accumulate
    local tmp_manifest
    tmp_manifest="$(mktemp)"

    {
        echo "{"
        echo "  \"snapshot\": \"${SNAPSHOT}\","
        echo "  \"source\": \"$(redact "$SOURCE_URI")\","
        echo "  \"created_utc\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
        echo "  \"tables\": ["
    } >> "$tmp_manifest"

    local first=1
    for t in "${TABLES[@]}"; do
        local dest
        dest="$(table_file "$t")"
        echo "  ${t} -> ${dest}"

        # Count rows for the manifest (cheap: one query)
        local n
        n=$(psql "$SOURCE_URI" -tA -c "SELECT count(*) FROM public.${t}")

        psql "$SOURCE_URI" -v ON_ERROR_STOP=1 \
             -c "\COPY public.${t} TO STDOUT WITH (FORMAT CSV, HEADER)" \
          | gzip \
          | write_to "$dest"

        if [[ "$first" -eq 1 ]]; then first=0; else echo "," >> "$tmp_manifest"; fi
        printf '    {"table": "%s", "rows": %s, "file": "%s.csv.gz"}' \
               "$t" "$n" "$t" >> "$tmp_manifest"
    done

    {
        echo ""
        echo "  ]"
        echo "}"
    } >> "$tmp_manifest"

    cat "$tmp_manifest" | write_to "$(manifest_file)"
    rm -f "$tmp_manifest"
    echo "Wrote manifest: $(manifest_file)"

}

do_load() {
    need_local

    local root
    root="$(snapshot_root)"
    echo "Loading from: ${root}"
    echo "Using SQL from: ${SQL_DIR}"

    local truncate_list
    truncate_list=$(IFS=,; echo "${TABLES[*]}")

    if [[ "$MODE" == "replace" ]]; then
        confirm_truncate "$truncate_list"
        psql "$LOCAL_URI" -v ON_ERROR_STOP=1 \
             -c "TRUNCATE ${truncate_list} RESTART IDENTITY CASCADE;"
    fi

    for t in "${TABLES[@]}"; do
        local src sql_file
        src="$(table_file "$t")"
        sql_file="${SQL_DIR}/load_${t}.sql"

        if [[ -f "$sql_file" ]]; then
            echo "  ${src} -> stage_${t} -> public.${t}  (via ${sql_file##*/})"
            read_from "$src" | gunzip | psql "$LOCAL_URI" \
                --single-transaction \
                -v ON_ERROR_STOP=1 \
                -f "$sql_file"
        else
            echo "  ${src} -> public.${t}  (generic COPY, no SQL file)"
            read_from "$src" | gunzip | psql "$LOCAL_URI" \
                --single-transaction \
                -v ON_ERROR_STOP=1 \
                -c "CREATE TEMP TABLE stage_${t} (LIKE public.${t} INCLUDING DEFAULTS);" \
                -c "\COPY stage_${t} FROM STDIN WITH (FORMAT CSV, HEADER)" \
                -c "INSERT INTO public.${t} SELECT * FROM stage_${t};"
        fi
    done

    reset_sequences
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "Action: ${ACTION}"
echo "Tables (in order): ${TABLES[*]}"
if [[ "$ACTION" != "direct" ]]; then
    echo "Target: ${TARGET}"
    echo "Snapshot: ${SNAPSHOT}"
fi
if [[ "$ACTION" != "load" ]]; then
    echo "Source: $(redact "${SOURCE_URI:-<unset>}")"
fi
echo "Mode: ${MODE}"
echo

case "$ACTION" in
    direct) do_direct;;
    dump)   do_dump;;
    load)   do_load;;
esac

echo "Done."
