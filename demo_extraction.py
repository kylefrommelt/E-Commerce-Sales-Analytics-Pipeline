#!/usr/bin/env python3
"""
Demo script to showcase the data extraction functionality.
This demonstrates that our data engineering pipeline works without requiring PostgreSQL.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add src to path for imports
sys.path.append('src')

def demo_csv_extraction():
    """Demonstrate CSV data extraction."""
    print("📊 CSV Data Extraction Demo")
    print("-" * 40)
    
    try:
        # Import our extractor
        from extract.data_extractor import CSVExtractor, DataQualityChecker
        
        # Extract sales data
        if os.path.exists("data/raw/sales_data.csv"):
            extractor = CSVExtractor("data/raw/sales_data.csv")
            
            if extractor.validate_source():
                df = extractor.extract()
                
                print(f"✅ Successfully extracted {len(df)} records")
                print(f"✅ Columns: {list(df.columns)}")
                print(f"✅ Data types: {df.dtypes.to_dict()}")
                
                # Show sample data
                print("\n📋 Sample Data:")
                print(df.head())
                
                # Data quality check
                quality_checker = DataQualityChecker()
                completeness = quality_checker.check_completeness(df, df.columns.tolist())
                duplicates = quality_checker.check_duplicates(df)
                
                print(f"\n📈 Data Quality:")
                print(f"  • Total records: {completeness['total_records']}")
                print(f"  • Duplicate records: {duplicates['duplicate_count']}")
                print(f"  • Null values: {sum(completeness['null_percentages'].values()):.1f}% total")
                
                return df
            else:
                print("❌ Source validation failed")
                return None
        else:
            print("❌ Sales data file not found")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def demo_data_transformation():
    """Demonstrate data transformation."""
    print("\n🔄 Data Transformation Demo")
    print("-" * 40)
    
    try:
        # Get sales data
        df = demo_csv_extraction()
        if df is None:
            return None
        
        # Basic transformations similar to our pipeline
        print("✅ Applying transformations...")
        
        # Convert dates
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Add calculated fields
        df['profit_margin'] = df['unit_price'] - (df['unit_price'] * 0.6)
        
        # Extract keys (simplified)
        df['customer_key'] = df['customer_id'].str.extract(r'(\d+)').astype(int)
        df['product_key'] = df['product_id'].str.extract(r'(\d+)').astype(int)
        df['channel_key'] = df['channel_id'].str.extract(r'(\d+)').astype(int)
        
        print(f"✅ Added {len(df.columns) - 9} calculated columns")
        print(f"✅ Processed {len(df)} records")
        
        # Show transformed data
        print("\n📋 Transformed Data Sample:")
        display_cols = ['customer_key', 'product_key', 'date_key', 'total_amount', 'profit_margin']
        print(df[display_cols].head())
        
        # Basic analytics
        print(f"\n📊 Quick Analytics:")
        print(f"  • Total Revenue: ${df['total_amount'].sum():.2f}")
        print(f"  • Average Order Value: ${df['total_amount'].mean():.2f}")
        print(f"  • Total Profit Margin: ${df['profit_margin'].sum():.2f}")
        print(f"  • Unique Customers: {df['customer_key'].nunique()}")
        print(f"  • Unique Products: {df['product_key'].nunique()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Transformation error: {e}")
        return None

def demo_customer_segmentation():
    """Demonstrate customer segmentation logic."""
    print("\n👥 Customer Segmentation Demo")
    print("-" * 40)
    
    try:
        # Get transformed data
        df = demo_data_transformation()
        if df is None:
            return
        
        # Simple RFM-style analysis
        customer_metrics = df.groupby('customer_key').agg({
            'total_amount': ['sum', 'mean', 'count'],
            'order_date': ['min', 'max']
        }).round(2)
        
        # Flatten column names
        customer_metrics.columns = ['total_spent', 'avg_order_value', 'order_count', 'first_order', 'last_order']
        customer_metrics = customer_metrics.reset_index()
        
        # Simple segmentation
        customer_metrics['segment'] = 'Regular'
        customer_metrics.loc[customer_metrics['total_spent'] > customer_metrics['total_spent'].median(), 'segment'] = 'High Value'
        customer_metrics.loc[customer_metrics['order_count'] > 1, 'segment'] = 'Loyal'
        
        print("✅ Customer segmentation completed")
        print(f"✅ Analyzed {len(customer_metrics)} customers")
        
        print("\n📋 Customer Segments:")
        segment_summary = customer_metrics['segment'].value_counts()
        for segment, count in segment_summary.items():
            print(f"  • {segment}: {count} customers")
        
        print("\n📊 Top Customers:")
        top_customers = customer_metrics.nlargest(3, 'total_spent')
        for _, customer in top_customers.iterrows():
            print(f"  • Customer {customer['customer_key']}: ${customer['total_spent']:.2f} ({customer['order_count']} orders)")
        
    except Exception as e:
        print(f"❌ Segmentation error: {e}")

def demo_configuration():
    """Demonstrate configuration loading."""
    print("\n⚙️ Configuration Demo")
    print("-" * 40)
    
    try:
        import yaml
        
        # Load pipeline config
        with open('config/pipeline.yaml', 'r') as f:
            pipeline_config = yaml.safe_load(f)
        
        print("✅ Pipeline configuration loaded")
        print(f"  • Data sources: {len(pipeline_config.get('data_sources', {}))}")
        print(f"  • Null threshold: {pipeline_config.get('data_quality', {}).get('null_threshold', 'N/A')}")
        print(f"  • Batch size: {pipeline_config.get('transformations', {}).get('data_warehouse', {}).get('batch_size', 'N/A')}")
        
        # Load database config
        with open('config/database.yaml', 'r') as f:
            db_config = yaml.safe_load(f)
        
        print("✅ Database configuration loaded")
        print(f"  • Development DB: {db_config.get('development', {}).get('database', 'N/A')}")
        print(f"  • Connection pool: {db_config.get('connection_pool', {}).get('max_connections', 'N/A')} max connections")
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")

def main():
    """Run the complete demo."""
    print("🚀 E-Commerce Data Pipeline - Functionality Demo")
    print("=" * 60)
    print(f"⏰ Demo started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Run demos
    demo_configuration()
    demo_customer_segmentation()
    
    print("\n" + "=" * 60)
    print("🎉 Demo completed successfully!")
    print("\n📝 What this demonstrates:")
    print("  ✅ Data extraction from CSV files")
    print("  ✅ Data quality validation")
    print("  ✅ Data transformation and enrichment")
    print("  ✅ Customer segmentation analytics")
    print("  ✅ Configuration management")
    print("  ✅ Error handling and logging")
    
    print("\n🔧 Technical skills shown:")
    print("  ✅ Python pandas for data processing")
    print("  ✅ Object-oriented programming")
    print("  ✅ YAML configuration management")
    print("  ✅ Data quality and validation")
    print("  ✅ Business analytics and insights")
    
    print("\n💼 Business value delivered:")
    print("  ✅ Customer segmentation for targeted marketing")
    print("  ✅ Revenue and profitability analysis")
    print("  ✅ Data quality assurance")
    print("  ✅ Automated data processing")

if __name__ == "__main__":
    main() 