#!/usr/bin/env python3
"""
Basic test script to validate the E-Commerce Data Pipeline project.
This tests the project structure and basic functionality without external dependencies.
"""

import os
import sys
import importlib.util
from pathlib import Path

def test_project_structure():
    """Test that all required files and directories exist."""
    print("🔍 Testing project structure...")
    
    required_files = [
        "README.md",
        "requirements.txt", 
        "PROJECT_SUMMARY.md",
        "config/database.yaml",
        "config/pipeline.yaml",
        "src/main.py",
        "src/utils/database.py", 
        "src/extract/data_extractor.py",
        "sql/ddl/01_create_schema.sql",
        "sql/analytics/customer_segmentation.sql",
        "scripts/setup.sh",
        "scripts/run_pipeline.sh"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
        else:
            print(f"  ✅ {file_path}")
    
    if missing_files:
        print(f"  ❌ Missing files: {missing_files}")
        return False
    else:
        print("  ✅ All required files present!")
        return True

def test_python_syntax():
    """Test that Python files have valid syntax."""
    print("\n🐍 Testing Python syntax...")
    
    python_files = [
        "src/main.py",
        "src/utils/database.py",
        "src/extract/data_extractor.py"
    ]
    
    syntax_errors = []
    for file_path in python_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Try to compile the code
            compile(content, file_path, 'exec')
            print(f"  ✅ {file_path} - Valid syntax")
            
        except SyntaxError as e:
            syntax_errors.append(f"{file_path}: {e}")
            print(f"  ❌ {file_path} - Syntax error: {e}")
        except Exception as e:
            print(f"  ⚠️  {file_path} - Could not read: {e}")
    
    if syntax_errors:
        print(f"  ❌ Syntax errors found: {len(syntax_errors)}")
        return False
    else:
        print("  ✅ All Python files have valid syntax!")
        return True

def test_sql_files():
    """Test that SQL files exist and are readable."""
    print("\n🗄️  Testing SQL files...")
    
    sql_files = [
        "sql/ddl/01_create_schema.sql",
        "sql/analytics/customer_segmentation.sql"
    ]
    
    for file_path in sql_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Basic SQL validation - check for common keywords
            sql_keywords = ['CREATE', 'SELECT', 'FROM', 'TABLE']
            keywords_found = [kw for kw in sql_keywords if kw in content.upper()]
            
            if keywords_found:
                print(f"  ✅ {file_path} - Contains SQL keywords: {keywords_found}")
            else:
                print(f"  ⚠️  {file_path} - May not contain valid SQL")
                
        except Exception as e:
            print(f"  ❌ {file_path} - Error reading: {e}")

def test_configuration_files():
    """Test that configuration files are valid YAML."""
    print("\n⚙️  Testing configuration files...")
    
    try:
        import yaml
        yaml_available = True
    except ImportError:
        yaml_available = False
        print("  ⚠️  PyYAML not available - skipping YAML validation")
    
    config_files = [
        "config/database.yaml",
        "config/pipeline.yaml"
    ]
    
    for file_path in config_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if yaml_available:
                try:
                    yaml.safe_load(content)
                    print(f"  ✅ {file_path} - Valid YAML")
                except yaml.YAMLError as e:
                    print(f"  ❌ {file_path} - YAML error: {e}")
            else:
                # Basic check - just ensure file is readable
                if content.strip():
                    print(f"  ✅ {file_path} - Readable")
                else:
                    print(f"  ⚠️  {file_path} - Empty file")
                
        except Exception as e:
            print(f"  ❌ {file_path} - Error: {e}")

def test_script_files():
    """Test that shell scripts exist."""
    print("\n📜 Testing shell scripts...")
    
    script_files = [
        "scripts/setup.sh",
        "scripts/run_pipeline.sh"
    ]
    
    for file_path in script_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for shebang and basic shell commands
            if content.startswith('#!/bin/bash'):
                print(f"  ✅ {file_path} - Valid bash script")
            else:
                print(f"  ⚠️  {file_path} - No bash shebang found")
                
        except Exception as e:
            print(f"  ❌ {file_path} - Error: {e}")

def create_sample_data():
    """Create sample data files for testing."""
    print("\n📊 Creating sample data...")
    
    # Create data directories
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    
    # Create sample CSV
    sales_data = """order_id,order_line_id,customer_id,product_id,order_date,quantity,unit_price,discount_amount,total_amount,channel_id
ORD-001,LINE-001,CUST-001,PROD-001,2024-01-15,2,29.99,5.99,53.99,CHN-001
ORD-002,LINE-002,CUST-002,PROD-002,2024-01-16,1,15.99,0.00,15.99,CHN-002
ORD-003,LINE-003,CUST-003,PROD-001,2024-01-17,3,29.99,0.00,89.97,CHN-001"""
    
    with open("data/raw/sales_data.csv", "w") as f:
        f.write(sales_data)
    print("  ✅ Created sample sales data")
    
    # Create sample JSON
    customer_data = """{
    "customers": [
        {"customer_id": "CUST-001", "name": "John Doe", "email": "john@example.com"},
        {"customer_id": "CUST-002", "name": "Jane Smith", "email": "jane@example.com"}
    ]
}"""
    
    with open("data/raw/customers.json", "w") as f:
        f.write(customer_data)
    print("  ✅ Created sample customer data")

def main():
    """Run all tests."""
    print("🚀 E-Commerce Data Pipeline - Basic Validation Test")
    print("=" * 60)
    
    tests = [
        test_project_structure,
        test_python_syntax, 
        test_sql_files,
        test_configuration_files,
        test_script_files
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"  ❌ Test failed with error: {e}")
    
    # Create sample data
    create_sample_data()
    
    print("\n" + "=" * 60)
    print(f"📈 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Project structure is valid.")
        print("\n📋 Next steps to run the full pipeline:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Set up database (optional): PostgreSQL")
        print("3. Configure environment: copy env.example to .env")
        print("4. Run pipeline: python src/main.py")
        return 0
    else:
        print("❌ Some tests failed. Please check the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 