"""
Database utility module for PostgreSQL connections and operations.
Demonstrates connection pooling, error handling, and transaction management.
"""

import os
import yaml
import logging
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Setup logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Comprehensive database manager with connection pooling and error handling.
    """
    
    def __init__(self, config_path: str = "config/database.yaml", environment: str = "development"):
        """
        Initialize database manager with configuration.
        
        Args:
            config_path: Path to database configuration file
            environment: Environment name (development, production, aws_rds)
        """
        self.config = self._load_config(config_path, environment)
        self.connection_pool = None
        self.engine = None
        self._initialize_connections()
    
    def _load_config(self, config_path: str, environment: str) -> Dict[str, Any]:
        """Load database configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                
            # Replace environment variables
            env_config = config.get(environment, {})
            for key, value in env_config.items():
                if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                    env_var = value[2:-1]
                    env_config[key] = os.getenv(env_var, value)
            
            return env_config
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise
    
    def _initialize_connections(self):
        """Initialize connection pool and SQLAlchemy engine."""
        try:
            # Connection pool for direct psycopg2 usage
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=5,
                maxconn=20,
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password']
            )
            
            # SQLAlchemy engine for pandas integration
            connection_string = (
                f"postgresql://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            logger.info("Database connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections with automatic cleanup.
        
        Yields:
            psycopg2.connection: Database connection
        """
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch: bool = True) -> Optional[List[Dict]]:
        """
        Execute a SQL query with parameters.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results as list of dictionaries or None
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                try:
                    cursor.execute(query, params)
                    
                    if fetch:
                        results = cursor.fetchall()
                        return [dict(row) for row in results]
                    else:
                        conn.commit()
                        return None
                        
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Query execution failed: {e}")
                    logger.error(f"Query: {query}")
                    raise
    
    def execute_many(self, query: str, data: List[tuple]) -> None:
        """
        Execute a query with multiple parameter sets (bulk insert/update).
        
        Args:
            query: SQL query string
            data: List of parameter tuples
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.executemany(query, data)
                    conn.commit()
                    logger.info(f"Bulk operation completed: {len(data)} records")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Bulk operation failed: {e}")
                    raise
    
    def read_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters dictionary
            
        Returns:
            pandas.DataFrame: Query results
        """
        try:
            return pd.read_sql(text(query), self.engine, params=params)
        except SQLAlchemyError as e:
            logger.error(f"DataFrame query failed: {e}")
            raise
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, 
                       if_exists: str = 'append', chunksize: int = 10000) -> None:
        """
        Write pandas DataFrame to database table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            chunksize: Number of rows to write at a time
        """
        try:
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists, 
                index=False, 
                chunksize=chunksize,
                method='multi'
            )
            logger.info(f"DataFrame written to {table_name}: {len(df)} records")
            
        except SQLAlchemyError as e:
            logger.error(f"DataFrame write failed: {e}")
            raise
    
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            bool: True if table exists, False otherwise
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
        """
        result = self.execute_query(query, (schema, table_name))
        return result[0]['exists'] if result else False
    
    def get_table_info(self, table_name: str, schema: str = 'public') -> List[Dict]:
        """
        Get table column information.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            List of dictionaries containing column information
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """
        return self.execute_query(query, (schema, table_name))
    
    def close_connections(self):
        """Close all database connections."""
        try:
            if self.connection_pool:
                self.connection_pool.closeall()
            if self.engine:
                self.engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

# Global database manager instance
db_manager = DatabaseManager()

def get_db_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    return db_manager 