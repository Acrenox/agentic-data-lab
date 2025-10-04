"""
Extract Agent - Handles data extraction with AI assistance using Gemini
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
import google.generativeai as genai
import os
from helpers import get_logger, save_metadata, get_file_list, get_data_summary

logger = get_logger(__name__)

class ExtractAgent:
    def __init__(self, api_key: Optional[str] = None):
        genai.configure(api_key=api_key or os.getenv("GOOGLE_API_KEY"))
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
        self.extracted_data = {}
        
    def detect_sources(self, directory: str = "processed") -> List[Dict[str, str]]:
        """Detect available data sources"""
        sources = []
        extensions = ['csv', 'xlsx', 'xls', 'json', 'parquet']
        
        files = get_file_list(directory, extensions)
        for file in files:
            sources.append({
                'path': str(file),
                'name': file.name,
                'type': file.suffix[1:],
                'size': file.stat().st_size
            })
        
        logger.info(f"Detected {len(sources)} data sources")
        return sources
    
    def extract_file(self, file_path: str) -> pd.DataFrame:
        """Extract data from a file"""
        path = Path(file_path)
        
        try:
            if path.suffix == '.csv':
                df = pd.read_csv(file_path)
            elif path.suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif path.suffix == '.json':
                df = pd.read_json(file_path)
            elif path.suffix == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file type: {path.suffix}")
            
            logger.info(f"Extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error extracting {file_path}: {e}")
            raise
    
    def extract_all(self, directory: str = "processed") -> Dict[str, pd.DataFrame]:
        """Extract all available sources"""
        sources = self.detect_sources(directory)
        
        for source in sources:
            try:
                df = self.extract_file(source['path'])
                source_name = Path(source['path']).stem
                self.extracted_data[source_name] = df
                
                # Save metadata
                metadata = {
                    'source': source['path'],
                    'extracted_at': pd.Timestamp.now().isoformat(),
                    'summary': get_data_summary(df)
                }
                save_metadata({source_name: metadata}, f"metadata_{source_name}.json")
                
            except Exception as e:
                logger.error(f"Failed to extract {source['path']}: {e}")
        
        return self.extracted_data
    
    def ai_assisted_extract(self, user_query: str, directory: str = "processed") -> Dict[str, pd.DataFrame]:
        """Use AI to determine what to extract based on user query"""
        sources = self.detect_sources(directory)
        
        prompt = f"""You are a data extraction assistant. The user wants to: "{user_query}"

Available data sources:
{chr(10).join([f"- {s['name']} ({s['type']}, {s['size']} bytes)" for s in sources])}

Determine which files should be extracted for this analysis. Respond with a JSON array of file names.
Example: ["file1.csv", "file2.xlsx"]
"""
        
        try:
            response = self.model.generate_content(prompt)
            response_text = response.text
            
            # Extract file names from response
            import json
            import re
            json_match = re.search(r'\[.*?\]', response_text, re.DOTALL)
            if json_match:
                files_to_extract = json.loads(json_match.group())
            else:
                files_to_extract = [s['name'] for s in sources]
            
            # Extract selected files
            for source in sources:
                if source['name'] in files_to_extract:
                    df = self.extract_file(source['path'])
                    source_name = Path(source['path']).stem
                    self.extracted_data[source_name] = df
                    logger.info(f"AI selected and extracted: {source['name']}")
            
        except Exception as e:
            logger.error(f"AI extraction failed, extracting all: {e}")
            return self.extract_all(directory)
        
        return self.extracted_data