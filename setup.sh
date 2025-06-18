#!/bin/bash

# E-Commerce Data Pipeline Setup Script
# This script sets up the complete development environment

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if running on Windows (Git Bash/WSL)
is_windows() {
    [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || -n "$WSL_DISTRO_NAME" ]]
}

# Check system requirements
check_requirements() {
    log "Checking system requirements..."
    
    # Check Python version
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
        log "Python version: $PYTHON_VERSION"
        
        # Check if Python version is 3.8+
        if python3 -c 'import sys; exit(0 if sys.version_info >= (3, 8) else 1)'; then
            success "Python version requirement met (3.8+)"
        else
            error "Python 3.8+ required. Current version: $PYTHON_VERSION"
            exit 1
        fi
    else
        error "Python 3 not found. Please install Python 3.8+"
        exit 1
    fi
    
    # Check pip
    if ! command -v pip3 &> /dev/null; then
        error "pip3 not found. Please install pip"
        exit 1
    fi
    
    # Check Git
    if ! command -v git &> /dev/null; then
        warning "Git not found. Version control features will be limited."
    fi
    
    # Check PostgreSQL client (optional)
    if command -v psql &> /dev/null; then
        success "PostgreSQL client found"
    else
        warning "PostgreSQL client not found. Database operations may be limited."
    fi
}

# Create project directories
create_directories() {
    log "Creating project directories..."
    
    directories=(
        "data/raw"
        "data/processed"
        "data/output"
        "logs"
        "reports"
        "tests/unit"
        "tests/integration"
        "docs"
        "temp"
    )
    
    for dir in "${directories[@]}"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            log "Created directory: $dir"
        fi
    done
    
    success "Directory structure created"
}

# Setup Python virtual environment
setup_virtual_env() {
    log "Setting up Python virtual environment..."
    
    if [[ ! -d "venv" ]]; then
        python3 -m venv venv
        success "Virtual environment created"
    else
        log "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    if is_windows; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    log "Virtual environment activated"
    
    # Upgrade pip
    pip install --upgrade pip
    success "pip upgraded"
}

# Install Python dependencies
install_dependencies() {
    log "Installing Python dependencies..."
    
    if [[ -f "requirements.txt" ]]; then
        pip install -r requirements.txt
        success "Python dependencies installed"
    else
        error "requirements.txt not found"
        exit 1
    fi
}

# Setup configuration files
setup_config() {
    log "Setting up configuration files..."
    
    # Copy environment template if .env doesn't exist
    if [[ ! -f ".env" ]]; then
        if [[ -f "env.example" ]]; then
            cp env.example .env
            log "Created .env from template"
            warning "Please update .env with your actual configuration values"
        else
            warning "env.example not found. Please create .env manually"
        fi
    else
        log ".env already exists"
    fi
    
    # Create logging configuration
    cat > config/logging.yaml << 'EOF'
version: 1
disable_existing_loggers: false

formatters:
  detailed:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
  simple:
    format: '%(levelname)s - %(message)s'

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: simple
    stream: ext://sys.stdout
  
  file:
    class: logging.handlers.RotatingFileHandler
    level: DEBUG
    formatter: detailed
    filename: logs/pipeline.log
    maxBytes: 10485760  # 10MB
    backupCount: 5

loggers:
  src:
    level: DEBUG
    handlers: [console, file]
    propagate: false

root:
  level: INFO
  handlers: [console, file]
EOF
    
    success "Configuration files setup complete"
}

# Generate sample data
generate_sample_data() {
    log "Generating sample data files..."
    
    # Create sample sales data CSV
    cat > data/raw/sales_data.csv << 'EOF'
order_id,order_line_id,customer_id,product_id,order_date,quantity,unit_price,discount_amount,total_amount,channel_id
ORD-001,LINE-001,CUST-001,PROD-001,2024-01-15,2,29.99,5.99,53.99,CHN-001
ORD-001,LINE-002,CUST-001,PROD-002,2024-01-15,1,15.99,0.00,15.99,CHN-001
ORD-002,LINE-003,CUST-002,PROD-003,2024-01-16,3,45.50,13.65,123.15,CHN-002
ORD-003,LINE-004,CUST-003,PROD-001,2024-01-17,1,29.99,2.99,26.99,CHN-001
ORD-004,LINE-005,CUST-004,PROD-004,2024-01-18,2,89.99,17.99,161.99,CHN-003
EOF
    
    # Create sample customer data JSON
    cat > data/raw/customers.json << 'EOF'
[
  {
    "customer_id": "CUST-001",
    "first_name": "John",
    "last_name": "Smith",
    "email": "john.smith@email.com",
    "phone": "555-0101",
    "date_of_birth": "1985-03-15",
    "gender": "M",
    "address_line1": "123 Main St",
    "city": "New York",
    "state": "NY",
    "postal_code": "10001",
    "country": "USA",
    "registration_date": "2023-06-15"
  },
  {
    "customer_id": "CUST-002",
    "first_name": "Jane",
    "last_name": "Doe",
    "email": "jane.doe@email.com",
    "phone": "555-0102",
    "date_of_birth": "1990-07-22",
    "gender": "F",
    "address_line1": "456 Oak Ave",
    "city": "Los Angeles",
    "state": "CA",
    "postal_code": "90210",
    "country": "USA",
    "registration_date": "2023-08-10"
  }
]
EOF
    
    # Create sample product data CSV
    cat > data/raw/products.csv << 'EOF'
product_id,product_name,brand,category,subcategory,unit_cost,unit_price,supplier,color,size,weight
PROD-001,Wireless Headphones,TechBrand,Electronics,Audio,19.99,29.99,Supplier A,Black,Medium,0.5
PROD-002,Phone Case,AccessoryCorp,Electronics,Accessories,5.99,15.99,Supplier B,Blue,Large,0.1
PROD-003,Running Shoes,SportsCo,Footwear,Athletic,35.00,45.50,Supplier C,Red,10,0.8
PROD-004,Coffee Maker,KitchenPro,Appliances,Kitchen,65.00,89.99,Supplier D,Silver,Large,3.2
EOF
    
    success "Sample data files created"
}

# Create basic test files
create_tests() {
    log "Creating test files..."
    
    # Create basic unit test
    cat > tests/unit/test_data_extractor.py << 'EOF'
import unittest
import pandas as pd
import tempfile
import os
from src.extract.data_extractor import CSVExtractor, DataQualityChecker

class TestDataExtractor(unittest.TestCase):
    
    def setUp(self):
        # Create temporary CSV file for testing
        self.test_data = """id,name,value
1,Test1,100
2,Test2,200
3,Test3,300"""
        
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_file.write(self.test_data)
        self.temp_file.close()
    
    def tearDown(self):
        os.unlink(self.temp_file.name)
    
    def test_csv_extraction(self):
        extractor = CSVExtractor(self.temp_file.name)
        self.assertTrue(extractor.validate_source())
        
        df = extractor.extract()
        self.assertEqual(len(df), 3)
        self.assertIn('id', df.columns)
        self.assertIn('name', df.columns)
        self.assertIn('value', df.columns)
    
    def test_data_quality_checker(self):
        df = pd.DataFrame({
            'id': [1, 2, 3, 3],  # Duplicate
            'name': ['A', 'B', None, 'D'],  # Null value
            'value': [100, 200, 300, 400]
        })
        
        checker = DataQualityChecker()
        
        # Test completeness
        completeness = checker.check_completeness(df, ['id', 'name', 'value'])
        self.assertEqual(completeness['total_records'], 4)
        
        # Test duplicates
        duplicates = checker.check_duplicates(df, subset=['id'])
        self.assertEqual(duplicates['duplicate_count'], 1)

if __name__ == '__main__':
    unittest.main()
EOF
    
    success "Test files created"
}

# Main setup function
main() {
    log "Starting E-Commerce Data Pipeline setup..."
    
    check_requirements
    create_directories
    setup_virtual_env
    install_dependencies
    setup_config
    generate_sample_data
    create_tests
    
    success "Setup completed successfully!"
    
    # Print next steps
    echo
    log "Next steps:"
    echo "1. Update .env file with your database credentials"
    echo "2. Run: ./scripts/init_database.sh (if using PostgreSQL)"
    echo "3. Run: python src/main.py (to execute the pipeline)"
    echo "4. Run: ./scripts/generate_reports.sh (to generate reports)"
    echo
    log "For development, activate the virtual environment:"
    if is_windows; then
        echo "  source venv/Scripts/activate"
    else
        echo "  source venv/bin/activate"
    fi
    echo
    warning "Remember to update configuration files with your actual values!"
}

# Run main function
main "$@" 