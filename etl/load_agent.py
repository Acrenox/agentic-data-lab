"""
Load Agent - Handles data loading with AI assistance using Gemini
"""
import pandas as pd
from typing import Dict, Any, Optional
import google.generativeai as genai
import os
import duckdb
from pathlib import Path
from helpers import get_logger, sanitize_column_name

logger = get_logger(__name__)

class LoadAgent:
    def __init__(self, api_key: Optional[str] = None):
        genai.configure(api_key=api_key or os.getenv("GOOGLE_API_KEY"))
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
        self.conn = None
        
    def connect_duckdb(self, db_path: str = "warehouse.duckdb"):
        """Connect to DuckDB database"""
        try:
            self.conn = duckdb.connect(db_path)
            logger.info(f"Connected to DuckDB: {db_path}")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise
    
    def load_to_duckdb(self, df: pd.DataFrame, table_name: str, mode: str = "replace"):
        """Load DataFrame to DuckDB"""
        if self.conn is None:
            self.connect_duckdb()
        
        try:
            # Sanitize column names
            df.columns = [sanitize_column_name(col) for col in df.columns]
            
            if mode == "replace":
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
            
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Loaded {row_count} rows to table '{table_name}'")
            
        except Exception as e:
            logger.error(f"Failed to load to DuckDB: {e}")
            raise
    
    def load_to_csv(self, df: pd.DataFrame, file_path: str):
        """Load DataFrame to CSV file"""
        try:
            df.to_csv(file_path, index=False)
            logger.info(f"Loaded {len(df)} rows to {file_path}")
        except Exception as e:
            logger.error(f"Failed to load to CSV: {e}")
            raise
    
    def load_to_excel(self, df: pd.DataFrame, file_path: str):
        """Load DataFrame to Excel file"""
        try:
            df.to_excel(file_path, index=False)
            logger.info(f"Loaded {len(df)} rows to {file_path}")
        except Exception as e:
            logger.error(f"Failed to load to Excel: {e}")
            raise
    
    def load_to_parquet(self, df: pd.DataFrame, file_path: str):
        """Load DataFrame to Parquet file"""
        try:
            df.to_parquet(file_path, index=False)
            logger.info(f"Loaded {len(df)} rows to {file_path}")
        except Exception as e:
            logger.error(f"Failed to load to Parquet: {e}")
            raise
    
    def ai_determine_destination(self, user_query: str, df: pd.DataFrame) -> Dict[str, str]:
        """Use AI to determine best destination for data"""
        
        prompt = f"""Based on this user query: "{user_query}"

Data shape: {df.shape[0]} rows, {df.shape[1]} columns

Determine the best destination for storing this data. Options:
- duckdb (for analytical queries, warehouse)
- csv (for simple export, sharing)
- excel (for business users, reports)
- parquet (for efficient storage, large data)

Respond with JSON format:
{{"destination": "duckdb", "table_name": "result_table", "reason": "explanation"}}
or
{{"destination": "csv", "file_path": "results.csv", "reason": "explanation"}}
"""
        
        try:
            response = self.model.generate_content(prompt)
            response_text = response.text
            
            import json
            import re
            json_match = re.search(r'\{.*?\}', response_text, re.DOTALL)
            if json_match:
                destination_info = json.loads(json_match.group())
            else:
                # Default to DuckDB
                destination_info = {
                    "destination": "duckdb",
                    "table_name": "ai_result",
                    "reason": "Default destination"
                }
            
            logger.info(f"AI determined destination: {destination_info}")
            return destination_info
            
        except Exception as e:
            logger.error(f"AI destination determination failed: {e}")
            return {"destination": "duckdb", "table_name": "result", "reason": "Fallback"}
    
    def load(self, df: pd.DataFrame, user_query: str = "", 
             destination: Optional[str] = None, **kwargs) -> str:
        """Load data to appropriate destination"""
        
        if destination is None:
            # Use AI to determine destination
            dest_info = self.ai_determine_destination(user_query, df)
            destination = dest_info.get('destination', 'duckdb')
            kwargs.update(dest_info)
        
        if destination == 'duckdb':
            table_name = kwargs.get('table_name', 'result_table')
            mode = kwargs.get('mode', 'replace')
            self.load_to_duckdb(df, table_name, mode)
            return f"duckdb:{table_name}"
        
        elif destination == 'csv':
            file_path = kwargs.get('file_path', 'output/result.csv')
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            self.load_to_csv(df, file_path)
            return file_path
        
        elif destination == 'excel':
            file_path = kwargs.get('file_path', 'output/result.xlsx')
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            self.load_to_excel(df, file_path)
            return file_path
        
        elif destination == 'parquet':
            file_path = kwargs.get('file_path', 'output/result.parquet')
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            self.load_to_parquet(df, file_path)
            return file_path
        
        else:
            raise ValueError(f"Unsupported destination: {destination}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")