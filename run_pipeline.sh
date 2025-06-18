#!/bin/bash

# E-Commerce Data Pipeline Execution Script
# This script runs the complete data pipeline with monitoring and error handling

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
LOG_DIR="logs"
PIPELINE_LOG="$LOG_DIR/pipeline_execution.log"
ERROR_LOG="$LOG_DIR/pipeline_errors.log"
REPORT_DIR="reports"

# Logging functions
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$PIPELINE_LOG"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$ERROR_LOG" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$PIPELINE_LOG"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$PIPELINE_LOG"
}

# Create required directories
setup_directories() {
    log "Setting up directories..."
    mkdir -p "$LOG_DIR" "$REPORT_DIR"
}

# Check if virtual environment exists and activate it
activate_virtual_env() {
    log "Checking virtual environment..."
    
    if [[ -d "venv" ]]; then
        if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || -n "$WSL_DISTRO_NAME" ]]; then
            source venv/Scripts/activate
        else
            source venv/bin/activate
        fi
        success "Virtual environment activated"
    else
        warning "Virtual environment not found. Using system Python."
    fi
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        error "Python 3 not found"
        exit 1
    fi
    
    # Check required files
    required_files=(
        "src/main.py"
        "config/pipeline.yaml"
        "config/database.yaml"
        "requirements.txt"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            error "Required file not found: $file"
            exit 1
        fi
    done
    
    # Check data directory
    if [[ ! -d "data/raw" ]]; then
        error "Data directory not found. Run ./scripts/setup.sh first."
        exit 1
    fi
    
    success "Prerequisites check passed"
}

# Monitor system resources
monitor_resources() {
    log "System resources at pipeline start:"
    
    # Memory usage (cross-platform)
    if command -v free &> /dev/null; then
        free -h | grep -E "(Mem|Swap)" | tee -a "$PIPELINE_LOG"
    elif command -v vm_stat &> /dev/null; then
        # macOS
        vm_stat | head -4 | tee -a "$PIPELINE_LOG"
    fi
    
    # Disk space
    if command -v df &> /dev/null; then
        df -h . | tee -a "$PIPELINE_LOG"
    fi
    
    # Load average (Unix-like systems)
    if command -v uptime &> /dev/null; then
        uptime | tee -a "$PIPELINE_LOG"
    fi
}

# Pre-pipeline validation
validate_data_sources() {
    log "Validating data sources..."
    
    data_files=(
        "data/raw/sales_data.csv"
        "data/raw/customers.json"
        "data/raw/products.csv"
    )
    
    for file in "${data_files[@]}"; do
        if [[ -f "$file" ]]; then
            file_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "unknown")
            log "Found: $file (size: $file_size bytes)"
        else
            warning "Data file not found: $file"
        fi
    done
}

# Execute the pipeline
run_pipeline() {
    log "Starting data pipeline execution..."
    
    # Set Python path
    export PYTHONPATH="${PWD}:${PYTHONPATH}"
    
    # Record start time
    start_time=$(date +%s)
    
    # Run the pipeline with timeout (30 minutes max)
    if timeout 1800 python3 src/main.py; then
        # Calculate execution time
        end_time=$(date +%s)
        execution_time=$((end_time - start_time))
        
        success "Pipeline completed successfully in ${execution_time} seconds"
        
        # Generate execution report
        generate_execution_report "$execution_time" "SUCCESS"
        
        return 0
    else
        # Calculate execution time
        end_time=$(date +%s)
        execution_time=$((end_time - start_time))
        
        error "Pipeline failed after ${execution_time} seconds"
        
        # Generate execution report
        generate_execution_report "$execution_time" "FAILED"
        
        return 1
    fi
}

# Generate execution report
generate_execution_report() {
    local execution_time=$1
    local status=$2
    
    log "Generating execution report..."
    
    report_file="$REPORT_DIR/pipeline_execution_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$report_file" << EOF
E-Commerce Data Pipeline Execution Report
=========================================

Execution Details:
- Date: $(date)
- Status: $status
- Execution Time: ${execution_time} seconds
- Pipeline Version: 1.0.0

System Information:
- OS: $OSTYPE
- Python Version: $(python3 --version 2>&1)
- Working Directory: $(pwd)

Files Processed:
EOF
    
    # Add file information
    for file in data/raw/*.csv data/raw/*.json; do
        if [[ -f "$file" ]]; then
            file_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "unknown")
            echo "- $file (size: $file_size bytes)" >> "$report_file"
        fi
    done
    
    # Add log summary
    echo "" >> "$report_file"
    echo "Log Summary:" >> "$report_file"
    echo "- Pipeline Log: $PIPELINE_LOG" >> "$report_file"
    echo "- Error Log: $ERROR_LOG" >> "$report_file"
    
    if [[ -f "$PIPELINE_LOG" ]]; then
        echo "- Log Lines: $(wc -l < "$PIPELINE_LOG")" >> "$report_file"
    fi
    
    if [[ -f "$ERROR_LOG" ]]; then
        error_count=$(wc -l < "$ERROR_LOG" 2>/dev/null || echo "0")
        echo "- Error Count: $error_count" >> "$report_file"
    fi
    
    # Add performance metrics if available
    if [[ "$status" == "SUCCESS" ]]; then
        echo "" >> "$report_file"
        echo "Performance Metrics:" >> "$report_file"
        echo "- Average execution time: ${execution_time}s" >> "$report_file"
        echo "- Status: All phases completed successfully" >> "$report_file"
    fi
    
    success "Execution report generated: $report_file"
}

# Post-pipeline cleanup
cleanup() {
    log "Performing cleanup..."
    
    # Clean up temporary files
    if [[ -d "temp" ]]; then
        rm -rf temp/*
        log "Temporary files cleaned"
    fi
    
    # Compress old log files (keep last 10)
    if command -v gzip &> /dev/null; then
        find "$LOG_DIR" -name "*.log" -type f -mtime +7 -exec gzip {} \;
        find "$LOG_DIR" -name "*.gz" -type f | head -n -10 | xargs -r rm
        log "Old log files compressed and cleaned"
    fi
    
    # Archive old reports (keep last 30)
    find "$REPORT_DIR" -name "*.txt" -type f -mtime +30 -delete 2>/dev/null || true
    log "Old reports archived"
}

# Error handling
handle_error() {
    local exit_code=$?
    error "Pipeline execution failed with exit code: $exit_code"
    
    # Create error report
    error_report="$REPORT_DIR/error_report_$(date +%Y%m%d_%H%M%S).txt"
    cat > "$error_report" << EOF
Pipeline Error Report
====================

Date: $(date)
Exit Code: $exit_code
Working Directory: $(pwd)

Recent Log Entries:
EOF
    
    if [[ -f "$PIPELINE_LOG" ]]; then
        tail -20 "$PIPELINE_LOG" >> "$error_report"
    fi
    
    if [[ -f "$ERROR_LOG" ]]; then
        echo "" >> "$error_report"
        echo "Error Log:" >> "$error_report"
        cat "$ERROR_LOG" >> "$error_report"
    fi
    
    error "Error report generated: $error_report"
    
    # Send notification (placeholder for actual notification system)
    log "Error notification would be sent here (email, Slack, etc.)"
    
    exit $exit_code
}

# Main execution function
main() {
    # Set up error handling
    trap handle_error ERR
    
    log "E-Commerce Data Pipeline Starting..."
    log "================================================"
    
    # Pipeline execution steps
    setup_directories
    activate_virtual_env
    check_prerequisites
    monitor_resources
    validate_data_sources
    
    # Run the actual pipeline
    if run_pipeline; then
        success "Pipeline execution completed successfully!"
        
        # Optional: Run additional reports
        if [[ "${GENERATE_REPORTS:-true}" == "true" ]]; then
            log "Generating additional reports..."
            if [[ -x "./scripts/generate_reports.sh" ]]; then
                ./scripts/generate_reports.sh
            else
                warning "Report generation script not found or not executable"
            fi
        fi
        
        cleanup
        exit 0
    else
        error "Pipeline execution failed!"
        cleanup
        exit 1
    fi
}

# Display usage information
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help          Show this help message
    -v, --verbose       Enable verbose logging
    --no-reports        Skip generating additional reports
    --dry-run          Validate setup but don't run pipeline

Examples:
    $0                  Run the pipeline normally
    $0 --verbose        Run with verbose logging
    $0 --dry-run        Validate setup only

Environment Variables:
    GENERATE_REPORTS    Set to 'false' to skip report generation (default: true)
    LOG_LEVEL          Set logging level (DEBUG, INFO, WARNING, ERROR)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -v|--verbose)
            set -x  # Enable verbose mode
            shift
            ;;
        --no-reports)
            export GENERATE_REPORTS="false"
            shift
            ;;
        --dry-run)
            log "Dry run mode - validating setup only"
            setup_directories
            activate_virtual_env
            check_prerequisites
            validate_data_sources
            success "Dry run completed - setup is valid"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Run main function
main "$@" 