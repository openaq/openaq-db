#!/bin/bash
set -euo pipefail


DATABASE_POSTGRES_USER=postgres
DATABASE_POSTGRES_PASSWORD=postgres
DATABASE_DB=openaqdev
DATABASE_HOST=localhost
DATABASE_PORT=5777

SOURCE_URI="${TRANSFER_URI:-}"
LOCAL_URI="postgresql://${DATABASE_POSTGRES_USER}:${DATABASE_POSTGRES_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DB}"

# Default set: parents first, children last
DEFAULT_TABLES=(
    providers
    entities
    measurands
    sensor_nodes
    instruments
    sensor_systems
    sensors
    #exported_public_measurements_log
)

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Copy a dependency-ordered set of tables from a source Postgres database
into the local development database.

Options:
  --uri URI          Source database connection URI
                     (or set TRANSFER_URI env var)
  --local-uri URI    Local database connection URI
                     (default: postgres@localhost:5777/openaqdev)
  --tables LIST      Comma-separated list of tables to transfer
                     (default: ${DEFAULT_TABLES[*]})
  --mode MODE        'append' (default) preserves existing rows,
                     'replace' truncates target tables first
  -h, --help         Show this help and exit

Examples:
  TRANSFER_URI=postgresql://user:pass@host/db $(basename "$0")
  $(basename "$0") --uri postgresql://... --mode replace
  $(basename "$0") --tables providers,sensor_nodes --mode append
EOF
}

TABLES=()
MODE=append   # replace | append

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tables) IFS=',' read -ra TABLES <<< "$2"; shift 2;;
        --mode)   MODE="$2"; shift 2;;
        --uri)    SOURCE_URI="$2"; shift 2;;
        --local-uri) LOCAL_URI="$2"; shift 2;;
        -h|--help) usage; exit 0;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

: "${SOURCE_URI:?Set TRANSFER_URI env var or pass --uri}"

[[ ${#TABLES[@]} -eq 0 ]] && TABLES=("${DEFAULT_TABLES[@]}")

echo "Source: $(echo "$SOURCE_URI" | sed -E 's|://[^:]+:[^@]+@|://**:**@|')"
echo "Tables (in order): ${TABLES[*]}"
echo "Mode: ${MODE}"

if ! psql "$LOCAL_URI" -c '\q' >/dev/null 2>&1; then
    echo "Error: cannot connect to local database at $LOCAL_URI. Make sure its up and running and the settings are correct" >&2
    exit 1
fi

if ! psql "$SOURCE_URI" -c '\q' >/dev/null 2>&1; then
    echo "Error: cannot connect to source database at $SOURCE_URI. Make sure its up and running and the settings are correct" >&2
    exit 1
fi

# Build --table args for pg_dump
DUMP_ARGS=()
for t in "${TABLES[@]}"; do
    DUMP_ARGS+=(--table="public.${t}")
    DUMP_ARGS+=(--exclude-table-data="public.${t}_*_seq")
done

# Build TRUNCATE statement (reverse order so children are truncated first)
TRUNCATE_LIST=$(IFS=,; echo "${TABLES[*]}")

# Pre-script: handle existing data based on mode
PRE_SQL=""
case "$MODE" in
    replace)
        PRE_SQL="TRUNCATE ${TRUNCATE_LIST} RESTART IDENTITY CASCADE;"
        ;;
    append)
        PRE_SQL="-- append mode: existing rows preserved"
        ;;
    *)
        echo "Unknown mode: $MODE"; exit 1;;
esac

if [[ "$MODE" == "replace" ]]; then
    read -p "This will TRUNCATE ${TRUNCATE_LIST} CASCADE on $(echo "$LOCAL_URI" | sed -E 's|://[^:]+:[^@]+@|://**:**@|'). Continue? [y/N] " ans
    [[ "$ans" == "y" ]] || exit 1
fi


echo "Streaming pg_dump -> psql ..."
echo $DUMP_ARGS

pg_dump "$SOURCE_URI" \
    --data-only \
    "${DUMP_ARGS[@]}" \
| psql "$LOCAL_URI" \
    --single-transaction \
    -v ON_ERROR_STOP=1 \
    -c "SET search_path = public" \
    -c "$PRE_SQL" \
    -f -

# Reset sequences to max(id)+1 for each table
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

echo "Done."
