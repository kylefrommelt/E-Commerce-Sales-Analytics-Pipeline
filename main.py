"""
Main pipeline orchestrator for E-Commerce Data Analytics Pipeline.
Demonstrates end-to-end ETL process with error handling and monitoring.
"""

import os
import sys
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import custom modules
from src.extract.data_extractor import extract_all_sources, DataQualityChecker
from src.utils.database import get_db_manager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataPipelineOrchestrator:
    """
    Main orchestrator for the data pipeline.
    Coordinates extraction, transformation, and loading processes.
    """
    
    def __init__(self, config_path: str = "config/pipeline.yaml"):
        """Initialize the pipeline orchestrator."""
        self.config_path = config_path
        self.config = self._load_config()
        self.db_manager = get_db_manager()
        self.pipeline_start_time = None
        self.pipeline_metrics = {}
        
    def _load_config(self) -> Dict[str, Any]:
        """Load pipeline configuration."""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete data pipeline.
        
        Returns:
            Dictionary with pipeline execution results
        """
        self.pipeline_start_time = datetime.now()
        logger.info("Starting E-Commerce Data Pipeline")
        
        try:
            # Step 1: Extract data from all sources
            extracted_data = self._extract_phase()
            
            # Step 2: Transform and clean data
            transformed_data = self._transform_phase(extracted_data)
            
            # Step 3: Load data into data warehouse
            load_results = self._load_phase(transformed_data)
            
            # Step 4: Generate analytics and reports
            analytics_results = self._analytics_phase()
            
            # Calculate pipeline metrics
            pipeline_end_time = datetime.now()
            execution_time = (pipeline_end_time - self.pipeline_start_time).total_seconds()
            
            results = {
                'status': 'SUCCESS',
                'execution_time_seconds': execution_time,
                'start_time': self.pipeline_start_time.isoformat(),
                'end_time': pipeline_end_time.isoformat(),
                'extraction_results': {
                    'sources_processed': len(extracted_data),
                    'total_records_extracted': sum(len(df) for df in extracted_data.values())
                },
                'transformation_results': {
                    'tables_processed': len(transformed_data),
                    'total_records_transformed': sum(len(df) for df in transformed_data.values())
                },
                'load_results': load_results,
                'analytics_results': analytics_results
            }
            
            self._log_pipeline_summary(results)
            return results
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'execution_time_seconds': (datetime.now() - self.pipeline_start_time).total_seconds()
            }
    
    def _extract_phase(self) -> Dict[str, pd.DataFrame]:
        """Extract data from all configured sources."""
        logger.info("=== EXTRACTION PHASE ===")
        
        try:
            extracted_data = extract_all_sources(self.config_path)
            
            # Data quality validation
            self._validate_extracted_data(extracted_data)
            
            logger.info(f"Extraction completed: {len(extracted_data)} sources processed")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Extraction phase failed: {e}")
            raise
    
    def _transform_phase(self, extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform and clean extracted data."""
        logger.info("=== TRANSFORMATION PHASE ===")
        
        transformed_data = {}
        
        try:
            # Transform sales data
            if 'sales_data' in extracted_data:
                transformed_data['fact_sales'] = self._transform_sales_data(extracted_data['sales_data'])
            
            # Transform customer data
            if 'customer_data' in extracted_data:
                transformed_data['dim_customer'] = self._transform_customer_data(extracted_data['customer_data'])
            
            # Transform product data
            if 'product_catalog' in extracted_data:
                transformed_data['dim_product'] = self._transform_product_data(extracted_data['product_catalog'])
            
            # Generate dimension tables
            transformed_data['dim_date'] = self._generate_date_dimension()
            transformed_data['dim_channel'] = self._generate_channel_dimension()
            transformed_data['dim_geography'] = self._generate_geography_dimension()
            
            # Customer analytics
            if 'fact_sales' in transformed_data and 'dim_customer' in transformed_data:
                transformed_data['customer_metrics'] = self._calculate_customer_metrics(
                    transformed_data['fact_sales'], 
                    transformed_data['dim_customer']
                )
            
            logger.info(f"Transformation completed: {len(transformed_data)} tables prepared")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Transformation phase failed: {e}")
            raise
    
    def _load_phase(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Load transformed data into the data warehouse."""
        logger.info("=== LOADING PHASE ===")
        
        load_results = {}
        
        try:
            for table_name, df in transformed_data.items():
                logger.info(f"Loading {table_name}: {len(df)} records")
                
                # Use upsert strategy for dimension tables, append for fact tables
                if_exists = 'replace' if table_name.startswith('dim_') else 'append'
                
                self.db_manager.write_dataframe(
                    df, 
                    table_name, 
                    if_exists=if_exists
                )
                
                load_results[table_name] = {
                    'records_loaded': len(df),
                    'load_strategy': if_exists
                }
            
            logger.info("Loading phase completed successfully")
            return load_results
            
        except Exception as e:
            logger.error(f"Loading phase failed: {e}")
            raise
    
    def _analytics_phase(self) -> Dict[str, Any]:
        """Generate analytics and insights."""
        logger.info("=== ANALYTICS PHASE ===")
        
        try:
            analytics_results = {}
            
            # Sales performance metrics
            sales_metrics = self._calculate_sales_metrics()
            analytics_results['sales_metrics'] = sales_metrics
            
            # Customer segmentation (simplified version)
            customer_segments = self._calculate_customer_segments()
            analytics_results['customer_segments'] = customer_segments
            
            # Product performance
            product_performance = self._calculate_product_performance()
            analytics_results['product_performance'] = product_performance
            
            logger.info("Analytics phase completed")
            return analytics_results
            
        except Exception as e:
            logger.error(f"Analytics phase failed: {e}")
            return {'error': str(e)}
    
    def _transform_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw sales data into fact table format."""
        logger.info("Transforming sales data...")
        
        # Data cleaning and validation
        df = df.dropna(subset=['order_id', 'customer_id', 'product_id'])
        df['order_date'] = pd.to_datetime(df['order_date'])
        
        # Add calculated fields
        df['profit_margin'] = df['unit_price'] - (df['unit_price'] * 0.6)  # Assumed 60% cost ratio
        df['date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Add surrogate keys (simplified - in real scenario would lookup from dimensions)
        df['customer_key'] = df['customer_id'].str.extract(r'(\d+)').astype(int)
        df['product_key'] = df['product_id'].str.extract(r'(\d+)').astype(int)
        df['channel_key'] = df['channel_id'].str.extract(r'(\d+)').astype(int)
        df['geography_key'] = 1  # Default geography
        
        # Select and rename columns for fact table
        fact_columns = [
            'order_id', 'order_line_id', 'customer_key', 'product_key', 
            'date_key', 'geography_key', 'channel_key', 'quantity',
            'unit_price', 'discount_amount', 'total_amount', 'profit_margin'
        ]
        
        return df[fact_columns].copy()
    
    def _transform_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer data into dimension table format."""
        logger.info("Transforming customer data...")
        
        # Data cleaning
        df = df.dropna(subset=['customer_id'])
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        
        # Add customer key
        df['customer_key'] = df['customer_id'].str.extract(r'(\d+)').astype(int)
        
        # Customer segmentation (simplified)
        df['customer_segment'] = 'Regular'  # Would be calculated based on purchase history
        
        # SCD Type 2 fields
        df['is_current'] = True
        df['effective_date'] = datetime.now().date()
        df['expiry_date'] = pd.to_datetime('9999-12-31').date()
        
        return df
    
    def _transform_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform product data into dimension table format."""
        logger.info("Transforming product data...")
        
        # Data cleaning
        df = df.dropna(subset=['product_id'])
        df['product_key'] = df['product_id'].str.extract(r'(\d+)').astype(int)
        df['is_active'] = True
        
        return df
    
    def _generate_date_dimension(self) -> pd.DataFrame:
        """Generate date dimension table."""
        logger.info("Generating date dimension...")
        
        # Generate dates for current year and next year
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2025, 12, 31)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        date_dim = pd.DataFrame({
            'date_key': dates.strftime('%Y%m%d').astype(int),
            'full_date': dates.date,
            'day_of_week': dates.dayofweek + 1,
            'day_name': dates.strftime('%A'),
            'day_of_month': dates.day,
            'day_of_year': dates.dayofyear,
            'week_of_year': dates.isocalendar().week,
            'month_number': dates.month,
            'month_name': dates.strftime('%B'),
            'quarter': dates.quarter,
            'year': dates.year,
            'is_weekend': dates.dayofweek >= 5,
            'is_holiday': False  # Simplified - would need holiday calendar
        })
        
        return date_dim
    
    def _generate_channel_dimension(self) -> pd.DataFrame:
        """Generate sales channel dimension."""
        logger.info("Generating channel dimension...")
        
        channels = [
            {'channel_key': 1, 'channel_id': 'CHN-001', 'channel_name': 'Website', 'channel_type': 'Online'},
            {'channel_key': 2, 'channel_id': 'CHN-002', 'channel_name': 'Mobile App', 'channel_type': 'Mobile'},
            {'channel_key': 3, 'channel_id': 'CHN-003', 'channel_name': 'Retail Store', 'channel_type': 'Store'},
        ]
        
        return pd.DataFrame(channels)
    
    def _generate_geography_dimension(self) -> pd.DataFrame:
        """Generate geography dimension."""
        logger.info("Generating geography dimension...")
        
        geographies = [
            {'geography_key': 1, 'city': 'New York', 'state': 'NY', 'country': 'USA', 'region': 'Northeast'},
            {'geography_key': 2, 'city': 'Los Angeles', 'state': 'CA', 'country': 'USA', 'region': 'West'},
        ]
        
        return pd.DataFrame(geographies)
    
    def _calculate_customer_metrics(self, sales_df: pd.DataFrame, customer_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate customer-level metrics."""
        logger.info("Calculating customer metrics...")
        
        customer_metrics = sales_df.groupby('customer_key').agg({
            'total_amount': ['sum', 'mean', 'count'],
            'quantity': 'sum',
            'order_id': 'nunique'
        }).round(2)
        
        # Flatten column names
        customer_metrics.columns = ['total_spent', 'avg_order_value', 'total_orders', 'total_items', 'unique_orders']
        customer_metrics = customer_metrics.reset_index()
        
        # Calculate customer lifetime value (simplified)
        customer_metrics['estimated_clv'] = customer_metrics['total_spent'] * 1.5
        
        return customer_metrics
    
    def _calculate_sales_metrics(self) -> Dict[str, Any]:
        """Calculate overall sales performance metrics."""
        try:
            query = """
            SELECT 
                COUNT(*) as total_orders,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                SUM(quantity) as total_items_sold
            FROM fact_sales
            """
            
            result = self.db_manager.execute_query(query)
            return result[0] if result else {}
            
        except Exception as e:
            logger.error(f"Failed to calculate sales metrics: {e}")
            return {}
    
    def _calculate_customer_segments(self) -> Dict[str, Any]:
        """Calculate customer segmentation summary."""
        try:
            query = """
            SELECT 
                customer_segment,
                COUNT(*) as customer_count
            FROM dim_customer
            WHERE is_current = true
            GROUP BY customer_segment
            """
            
            result = self.db_manager.execute_query(query)
            return {row['customer_segment']: row['customer_count'] for row in result} if result else {}
            
        except Exception as e:
            logger.error(f"Failed to calculate customer segments: {e}")
            return {}
    
    def _calculate_product_performance(self) -> Dict[str, Any]:
        """Calculate product performance metrics."""
        try:
            query = """
            SELECT 
                p.category,
                COUNT(DISTINCT f.product_key) as products_sold,
                SUM(f.quantity) as total_quantity,
                SUM(f.total_amount) as total_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.category
            ORDER BY total_revenue DESC
            """
            
            result = self.db_manager.execute_query(query)
            return {row['category']: {
                'products_sold': row['products_sold'],
                'total_quantity': row['total_quantity'],
                'total_revenue': float(row['total_revenue'])
            } for row in result} if result else {}
            
        except Exception as e:
            logger.error(f"Failed to calculate product performance: {e}")
            return {}
    
    def _validate_extracted_data(self, extracted_data: Dict[str, pd.DataFrame]):
        """Validate extracted data quality."""
        logger.info("Validating extracted data quality...")
        
        quality_checker = DataQualityChecker()
        
        for source_name, df in extracted_data.items():
            # Check for minimum record count
            if len(df) == 0:
                logger.warning(f"No data extracted from {source_name}")
                continue
            
            # Check for duplicates
            duplicates = quality_checker.check_duplicates(df)
            if duplicates['duplicate_percentage'] > 5:  # More than 5%
                logger.warning(f"High duplicate rate in {source_name}: {duplicates['duplicate_percentage']:.2f}%")
            
            # Check completeness
            completeness = quality_checker.check_completeness(df, df.columns.tolist())
            high_null_columns = [
                col for col, pct in completeness['null_percentages'].items() 
                if pct > 10  # More than 10% null
            ]
            if high_null_columns:
                logger.warning(f"High null rates in {source_name} columns: {high_null_columns}")
    
    def _log_pipeline_summary(self, results: Dict[str, Any]):
        """Log pipeline execution summary."""
        logger.info("=== PIPELINE EXECUTION SUMMARY ===")
        logger.info(f"Status: {results['status']}")
        logger.info(f"Execution Time: {results['execution_time_seconds']:.2f} seconds")
        logger.info(f"Start Time: {results['start_time']}")
        logger.info(f"End Time: {results['end_time']}")
        logger.info(f"Sources Processed: {results['extraction_results']['sources_processed']}")
        logger.info(f"Total Records Extracted: {results['extraction_results']['total_records_extracted']}")
        logger.info(f"Total Records Transformed: {results['transformation_results']['total_records_transformed']}")
        logger.info("Load Results:")
        for table, info in results['load_results'].items():
            logger.info(f"  {table}: {info['records_loaded']} records ({info['load_strategy']})")
        logger.info("Analytics Results:")
        for metric, value in results['analytics_results'].items():
            logger.info(f"  {metric}: {value}")

def main():
    """Main entry point for the pipeline."""
    try:
        # Initialize and run pipeline
        orchestrator = DataPipelineOrchestrator()
        results = orchestrator.run_pipeline()
        
        # Exit with appropriate code
        exit_code = 0 if results['status'] == 'SUCCESS' else 1
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Pipeline failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 