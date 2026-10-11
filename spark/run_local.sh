#!/bin/bash
# =============================================================================
# Local Spark Job Runner
# =============================================================================
# This script sets up the environment and runs the landing-to-bronze job locally.
#
# USAGE:
#   ./run_local.sh              # Run with defaults (local, overwrite)
#   ./run_local.sh --mode append    # Incremental mode
#   ./run_local.sh --dry-run        # Validate only
#   ./run_local.sh --env floci      # Use floci emulator
#
# PREREQUISITES:
#   1. Activate your conda/venv environment
#   2. Ensure Java is installed (java -version)
#   3. Ensure .env file exists with AWS credentials
#   4. For floci: run `make up-floci` first
#
# =============================================================================

set -e  # Exit on error

# -----------------------------------------------------------------------------
# Configure paths
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
JOBS_DIR="$SCRIPT_DIR/jobs"

# -----------------------------------------------------------------------------
# Check prerequisites
# -----------------------------------------------------------------------------
echo "🔍 Checking prerequisites..."

# Check Java is installed
if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed. Please install Java 8 or 11."
    exit 1
fi
echo "✅ Java: $(java -version 2>&1 | head -n 1)"

# Check Python environment
if ! command -v python &> /dev/null; then
    echo "❌ Python is not found. Activate your conda/venv environment."
    exit 1
fi
echo "✅ Python: $(python --version)"

# Check PySpark is installed
if ! python -c "import pyspark" 2>/dev/null; then
    echo "❌ PySpark is not installed. Run: pip install -r spark/requirements.txt"
    exit 1
fi
echo "✅ PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"

# Check .env file exists
ENV_FILE="$PROJECT_ROOT/.env"
if [[ ! -f "$ENV_FILE" ]]; then
    echo "⚠️  .env file not found at $ENV_FILE"
    echo "   Create it with AWS credentials or use --env floci"
else
    echo "✅ .env file found"
fi

# -----------------------------------------------------------------------------
# Set environment variables for Spark
# -----------------------------------------------------------------------------
export PYSPARK_PYTHON="$(which python)"
export PYSPARK_DRIVER_PYTHON="$(which python)"

# -----------------------------------------------------------------------------
# Run the job
# -----------------------------------------------------------------------------
echo ""
echo "🚀 Starting Landing to Bronze job..."
echo "   Working directory: $JOBS_DIR"
echo "   Arguments: $@"
echo ""

cd "$JOBS_DIR"
python landing_to_bronze.py "$@"

# -----------------------------------------------------------------------------
# Report status
# -----------------------------------------------------------------------------
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 0 ]]; then
    echo ""
    echo "✅ Job completed successfully!"
else
    echo ""
    echo "❌ Job failed with exit code: $EXIT_CODE"
fi
exit $EXIT_CODE
