#!/usr/bin/env bash
# =============================================================================
# Data Vault 2.0 DQ Framework — Deploy Script
# Usage: ./scripts/deploy.sh --connection <name> [options]
#
# Options:
#   --connection   <name>   Snowflake CLI connection name       (required)
#   --database     <name>   Target database for DMFs + app      (default: DV_DQ)
#   --schema       <name>   Target schema                       (default: DQ)
#   --edw-database <name>   EDW database with DV tables         (default: — skips attach)
#   --warehouse    <name>   Warehouse to create/use             (default: DV_DQ_WH)
#   --role         <name>   Role for setup + deploy             (default: ACCOUNTADMIN)
#   --skip-setup            Skip infrastructure setup (re-deploy code only)
#   --skip-dmfs             Skip DMF creation (just deploy the Streamlit app)
#   --skip-attach           Skip DMF attachment to DV tables
#   --prune                 Remove stale files from stage after deploy
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONNECTION=""
DATABASE="DV_DQ"
SCHEMA="DQ"
EDW_DATABASE=""
WAREHOUSE="DV_DQ_WH"
ROLE="ACCOUNTADMIN"
SKIP_SETUP=false
SKIP_DMFS=false
SKIP_ATTACH=false
PRUNE_FLAG=""

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection)    CONNECTION="$2";    shift 2 ;;
        --database)      DATABASE="$2";      shift 2 ;;
        --schema)        SCHEMA="$2";        shift 2 ;;
        --edw-database)  EDW_DATABASE="$2";  shift 2 ;;
        --warehouse)     WAREHOUSE="$2";     shift 2 ;;
        --role)          ROLE="$2";          shift 2 ;;
        --skip-setup)    SKIP_SETUP=true;    shift   ;;
        --skip-dmfs)     SKIP_DMFS=true;     shift   ;;
        --skip-attach)   SKIP_ATTACH=true;   shift   ;;
        --prune)         PRUNE_FLAG="--prune"; shift ;;
        -h|--help)
            sed -n '2,16p' "$0"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$CONNECTION" ]]; then
    echo "Error: --connection is required."
    echo "Run ./scripts/deploy.sh --help for usage."
    exit 1
fi

STEPS_TOTAL=4
STEP=0

echo ""
echo "=============================================="
echo "  Data Vault 2.0 DQ Framework — Deployment"
echo "=============================================="
echo "  Connection   : $CONNECTION"
echo "  Role         : $ROLE"
echo "  Database     : $DATABASE"
echo "  Schema       : $SCHEMA"
echo "  EDW Database : ${EDW_DATABASE:-"(not set — will skip attach)"}"
echo "  Warehouse    : $WAREHOUSE"
echo "=============================================="
echo ""

run_sql_file() {
    local file="$1"
    shift
    snow sql \
        --connection "$CONNECTION" \
        --filename "$file" \
        --variable "database=$DATABASE" \
        --variable "schema=$SCHEMA" \
        --variable "warehouse=$WAREHOUSE" \
        --variable "role=$ROLE" \
        --variable "edw_database=${EDW_DATABASE:-__NONE__}" \
        "$@"
}

# ── 1. Infrastructure setup ──────────────────────────────────────────────────
STEP=$((STEP + 1))
if [[ "$SKIP_SETUP" == false ]]; then
    echo "→ [$STEP/$STEPS_TOTAL] Setting up infrastructure..."
    run_sql_file "$SCRIPT_DIR/setup.sql"
    echo "  ✓ Infrastructure ready."
else
    echo "→ [$STEP/$STEPS_TOTAL] Skipping infrastructure setup (--skip-setup)."
fi

echo ""

# ── 2. Create DMFs ───────────────────────────────────────────────────────────
STEP=$((STEP + 1))
if [[ "$SKIP_DMFS" == false ]]; then
    echo "→ [$STEP/$STEPS_TOTAL] Creating Data Metric Functions..."
    run_sql_file "$REPO_ROOT/sql/01_create_dq_schema_and_dmfs.sql"
    echo "  ✓ DMFs created in $DATABASE.$SCHEMA."
else
    echo "→ [$STEP/$STEPS_TOTAL] Skipping DMF creation (--skip-dmfs)."
fi

echo ""

# ── 3. Attach DMFs to DV tables (requires --edw-database) ───────────────────
STEP=$((STEP + 1))
if [[ "$SKIP_ATTACH" == false && -n "$EDW_DATABASE" ]]; then
    echo "→ [$STEP/$STEPS_TOTAL] Attaching DMFs to DV tables in $EDW_DATABASE..."
    run_sql_file "$REPO_ROOT/sql/02_attach_dmfs_to_dv_tables.sql"
    echo "  ✓ DMFs attached."
else
    if [[ -z "$EDW_DATABASE" ]]; then
        echo "→ [$STEP/$STEPS_TOTAL] Skipping DMF attachment (--edw-database not provided)."
        echo "  ℹ  To attach DMFs later, run:"
        echo "     ./scripts/deploy.sh --connection $CONNECTION --database $DATABASE --edw-database YOUR_EDW_DB --skip-setup --skip-dmfs"
    else
        echo "→ [$STEP/$STEPS_TOTAL] Skipping DMF attachment (--skip-attach)."
    fi
fi

echo ""

# ── 4. Deploy Streamlit app ─────────────────────────────────────────────────
STEP=$((STEP + 1))
echo "→ [$STEP/$STEPS_TOTAL] Deploying Streamlit dashboard..."

sed \
    -e "s|__DATABASE__|$DATABASE|g" \
    -e "s|__SCHEMA__|$SCHEMA|g" \
    -e "s|__WAREHOUSE__|$WAREHOUSE|g" \
    "$REPO_ROOT/snowflake.yml.template" > "$REPO_ROOT/snowflake.yml"
echo "  ✓ snowflake.yml written."

snow streamlit deploy \
    --connection "$CONNECTION" \
    --replace \
    $PRUNE_FLAG
echo "  ✓ Streamlit app deployed."

echo ""
echo "=============================================="
echo "  Done!"
echo ""
echo "  Open the app in Snowsight:"
echo "  Snowsight → Streamlit Apps → DV_DMF_METRICS"
echo "=============================================="
echo ""
