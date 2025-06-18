"""
Data extraction module supporting multiple data sources.
Demonstrates ETL best practices and data source integration.
"""

import os
import json
import logging
import requests
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import yaml
from abc import ABC, abstractmethod

# Setup logging
logger = logging.getLogger(__name__)

class DataExtractor(ABC):
    """Abstract base class for data extractors."""
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data and return as pandas DataFrame."""
        pass
    
    @abstractmethod
    def validate_source(self) -> bool:
        """Validate data source availability."""
        pass

class CSVExtractor(DataExtractor):
    """Extractor for CSV files."""
    
    def __init__(self, file_path: str, delimiter: str = ',', encoding: str = 'utf-8'):
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding
    
    def validate_source(self) -> bool:
        """Check if CSV file exists and is readable."""
        try:
            return os.path.exists(self.file_path) and os.access(self.file_path, os.R_OK)
        except Exception as e:
            logger.error(f"CSV validation failed: {e}")
            return False
    
    def extract(self) -> pd.DataFrame:
        """Extract data from CSV file."""
        if not self.validate_source():
            raise FileNotFoundError(f"CSV file not accessible: {self.file_path}")
        
        try:
            df = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                parse_dates=True,
                infer_datetime_format=True
            )
            logger.info(f"CSV extracted: {len(df)} records from {self.file_path}")
            return df
            
        except Exception as e:
            logger.error(f"CSV extraction failed: {e}")
            raise

class JSONExtractor(DataExtractor):
    """Extractor for JSON files."""
    
    def __init__(self, file_path: str, encoding: str = 'utf-8'):
        self.file_path = file_path
        self.encoding = encoding
    
    def validate_source(self) -> bool:
        """Check if JSON file exists and is readable."""
        try:
            return os.path.exists(self.file_path) and os.access(self.file_path, os.R_OK)
        except Exception as e:
            logger.error(f"JSON validation failed: {e}")
            return False
    
    def extract(self) -> pd.DataFrame:
        """Extract data from JSON file."""
        if not self.validate_source():
            raise FileNotFoundError(f"JSON file not accessible: {self.file_path}")
        
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as file:
                data = json.load(file)
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                # Check if it's a nested structure with a data key
                if 'data' in data:
                    df = pd.json_normalize(data['data'])
                else:
                    df = pd.json_normalize([data])
            else:
                raise ValueError("Unsupported JSON structure")
            
            logger.info(f"JSON extracted: {len(df)} records from {self.file_path}")
            return df
            
        except Exception as e:
            logger.error(f"JSON extraction failed: {e}")
            raise

class APIExtractor(DataExtractor):
    """Extractor for REST APIs."""
    
    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None, 
                 params: Optional[Dict[str, Any]] = None, auth: Optional[tuple] = None):
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.auth = auth
        self.timeout = 30
    
    def validate_source(self) -> bool:
        """Check if API endpoint is accessible."""
        try:
            response = requests.head(self.url, timeout=self.timeout)
            return response.status_code < 400
        except Exception as e:
            logger.error(f"API validation failed: {e}")
            return False
    
    def extract(self) -> pd.DataFrame:
        """Extract data from API endpoint."""
        try:
            response = requests.get(
                self.url,
                headers=self.headers,
                params=self.params,
                auth=self.auth,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different API response structures
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                # Common API patterns
                for key in ['data', 'results', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        df = pd.json_normalize(data[key])
                        break
                else:
                    # If no common pattern found, try to normalize the entire response
                    df = pd.json_normalize([data])
            else:
                raise ValueError("Unsupported API response structure")
            
            logger.info(f"API extracted: {len(df)} records from {self.url}")
            return df
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"API extraction failed: {e}")
            raise

class DatabaseExtractor(DataExtractor):
    """Extractor for database queries."""
    
    def __init__(self, query: str, db_manager, params: Optional[Dict] = None):
        self.query = query
        self.db_manager = db_manager
        self.params = params or {}
    
    def validate_source(self) -> bool:
        """Check database connectivity."""
        try:
            # Simple connectivity test
            test_query = "SELECT 1 as test"
            result = self.db_manager.execute_query(test_query)
            return result is not None
        except Exception as e:
            logger.error(f"Database validation failed: {e}")
            return False
    
    def extract(self) -> pd.DataFrame:
        """Extract data using SQL query."""
        try:
            df = self.db_manager.read_dataframe(self.query, self.params)
            logger.info(f"Database extracted: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Database extraction failed: {e}")
            raise

class DataExtractorFactory:
    """Factory class for creating data extractors."""
    
    @staticmethod
    def create_extractor(source_config: Dict[str, Any], **kwargs) -> DataExtractor:
        """
        Create appropriate extractor based on source configuration.
        
        Args:
            source_config: Configuration dictionary with source details
            **kwargs: Additional arguments for specific extractors
            
        Returns:
            DataExtractor: Appropriate extractor instance
        """
        source_type = source_config.get('type', '').lower()
        
        if source_type == 'csv':
            return CSVExtractor(
                file_path=source_config['path'],
                delimiter=source_config.get('delimiter', ','),
                encoding=source_config.get('encoding', 'utf-8')
            )
        
        elif source_type == 'json':
            return JSONExtractor(
                file_path=source_config['path'],
                encoding=source_config.get('encoding', 'utf-8')
            )
        
        elif source_type == 'api':
            return APIExtractor(
                url=source_config['url'],
                headers=source_config.get('headers'),
                params=source_config.get('params'),
                auth=source_config.get('auth')
            )
        
        elif source_type == 'database':
            from src.utils.database import get_db_manager
            return DatabaseExtractor(
                query=source_config['query'],
                db_manager=get_db_manager(),
                params=source_config.get('params')
            )
        
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

class DataQualityChecker:
    """Data quality validation for extracted data."""
    
    @staticmethod
    def check_completeness(df: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """Check data completeness."""
        missing_columns = set(required_columns) - set(df.columns)
        null_percentages = (df.isnull().sum() / len(df) * 100).to_dict()
        
        return {
            'missing_columns': list(missing_columns),
            'null_percentages': null_percentages,
            'total_records': len(df),
            'empty_records': len(df[df.isnull().all(axis=1)])
        }
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None) -> Dict[str, Any]:
        """Check for duplicate records."""
        duplicate_count = df.duplicated(subset=subset).sum()
        duplicate_percentage = (duplicate_count / len(df) * 100) if len(df) > 0 else 0
        
        return {
            'duplicate_count': duplicate_count,
            'duplicate_percentage': duplicate_percentage,
            'unique_records': len(df) - duplicate_count
        }
    
    @staticmethod
    def check_data_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> Dict[str, Any]:
        """Check data type consistency."""
        type_issues = {}
        
        for column, expected_type in expected_types.items():
            if column in df.columns:
                actual_type = str(df[column].dtype)
                if expected_type not in actual_type:
                    type_issues[column] = {
                        'expected': expected_type,
                        'actual': actual_type
                    }
        
        return {
            'type_issues': type_issues,
            'columns_checked': len(expected_types)
        }

def extract_all_sources(config_path: str = "config/pipeline.yaml") -> Dict[str, pd.DataFrame]:
    """
    Extract data from all configured sources.
    
    Args:
        config_path: Path to pipeline configuration file
        
    Returns:
        Dictionary of DataFrames keyed by source name
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        data_sources = config.get('data_sources', {})
        extracted_data = {}
        
        for source_name, source_config in data_sources.items():
            try:
                logger.info(f"Extracting data from: {source_name}")
                extractor = DataExtractorFactory.create_extractor(source_config)
                
                if extractor.validate_source():
                    df = extractor.extract()
                    extracted_data[source_name] = df
                    
                    # Basic data quality check
                    quality_checker = DataQualityChecker()
                    completeness = quality_checker.check_completeness(df, df.columns.tolist())
                    duplicates = quality_checker.check_duplicates(df)
                    
                    logger.info(f"Data quality - {source_name}:")
                    logger.info(f"  Records: {completeness['total_records']}")
                    logger.info(f"  Duplicates: {duplicates['duplicate_count']} ({duplicates['duplicate_percentage']:.2f}%)")
                
                else:
                    logger.warning(f"Source validation failed: {source_name}")
                    
            except Exception as e:
                logger.error(f"Failed to extract from {source_name}: {e}")
                continue
        
        logger.info(f"Successfully extracted from {len(extracted_data)} sources")
        return extracted_data
        
    except Exception as e:
        logger.error(f"Failed to extract data sources: {e}")
        raise 