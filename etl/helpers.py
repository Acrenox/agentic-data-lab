"""
Shared helpers for the agentic ETL pipeline
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance"""
    return logging.getLogger(name)

def load_metadata(metadata_path: str = "metadata.json") -> Dict[str, Any]:
    """Load metadata from JSON file"""
    try:
        with open(metadata_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        get_logger(__name__).error(f"Error decoding metadata: {e}")
        return {}

def save_metadata(metadata: Dict[str, Any], metadata_path: str = "metadata.json"):
    """Save metadata to JSON file"""
    try:
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    except Exception as e:
        get_logger(__name__).error(f"Error saving metadata: {e}")

def update_metadata(key: str, value: Any, metadata_path: str = "metadata.json"):
    """Update a specific key in metadata"""
    metadata = load_metadata(metadata_path)
    metadata[key] = value
    metadata['last_updated'] = datetime.now().isoformat()
    save_metadata(metadata, metadata_path)

def get_file_list(directory: str, extensions: List[str] = None) -> List[Path]:
    """Get list of files in directory with optional extension filter"""
    dir_path = Path(directory)
    if not dir_path.exists():
        get_logger(__name__).warning(f"Directory does not exist: {directory}")
        return []
    
    if extensions:
        files = []
        for ext in extensions:
            files.extend(dir_path.glob(f"*.{ext}"))
        return files
    return list(dir_path.glob("*"))

def infer_schema(df) -> Dict[str, str]:
    """Infer schema from pandas DataFrame"""
    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if 'int' in dtype:
            schema[col] = 'integer'
        elif 'float' in dtype:
            schema[col] = 'float'
        elif 'datetime' in dtype:
            schema[col] = 'datetime'
        elif 'bool' in dtype:
            schema[col] = 'boolean'
        else:
            schema[col] = 'string'
    return schema

def get_data_summary(df) -> Dict[str, Any]:
    """Get summary statistics of DataFrame"""
    return {
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': list(df.columns),
        'schema': infer_schema(df),
        'missing_values': df.isnull().sum().to_dict(),
        'memory_usage': int(df.memory_usage(deep=True).sum())
    }

def clean_code_fences(code: str) -> str:
    """Remove markdown code fences from code string"""
    lines = code.splitlines()
    clean_lines = [line for line in lines if not (line.strip() == '```' or line.strip() == '```python')]
    return '\n'.join(clean_lines)

def sanitize_column_name(col_name: str) -> str:
    """Sanitize column name for SQL compatibility"""
    import re
    # Convert to lowercase and replace spaces and special chars with underscores
    sanitized = re.sub(r'[^\w\s]', '_', str(col_name).lower())
    sanitized = re.sub(r'\s+', '_', sanitized)
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    return sanitized