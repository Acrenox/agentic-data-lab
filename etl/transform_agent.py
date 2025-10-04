"""
Transform Agent - Handles data transformations with AI assistance using Gemini
"""
import pandas as pd
from typing import Dict, Any, Optional
import google.generativeai as genai
import os
from helpers import get_logger, clean_code_fences, get_data_summary

logger = get_logger(__name__)

class TransformAgent:
    def __init__(self, api_key: Optional[str] = None):
        genai.configure(api_key=api_key or os.getenv("GOOGLE_API_KEY"))
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
        self.transformed_data = {}
        
    def generate_transformation_code(self, user_query: str, data_info: Dict[str, Any]) -> str:
        """Generate pandas transformation code using AI"""
        
        prompt = f"""You are a data transformation expert. Generate Python pandas code to transform data based on this request:

User Query: "{user_query}"

Available DataFrames and their structure:
{data_info}

Requirements:
1. Use pandas operations to transform the data
2. Input DataFrames are available in a dict called 'dataframes'
3. CRITICAL: You MUST end your code with: result_df = your_final_dataframe
4. Handle missing values appropriately
5. Add comments explaining the logic
6. Return ONLY the Python code, no explanations

Example format:
import pandas as pd
import numpy as np

# Get data
df = dataframes['source_name']

# Do transformations
df_transformed = df.groupby('column').sum()

# MANDATORY: Assign final result to result_df
result_df = df_transformed
"""
        
        try:
            response = self.model.generate_content(prompt)
            code = response.text
            code = clean_code_fences(code)
            
            # Add result_df assignment if missing
            if 'result_df' not in code:
                logger.warning("Adding missing result_df assignment")
                # Find the last DataFrame variable created
                lines = code.split('\n')
                for i in range(len(lines) - 1, -1, -1):
                    line = lines[i].strip()
                    if '=' in line and not line.startswith('#'):
                        var_name = line.split('=')[0].strip()
                        if var_name and var_name.isidentifier():
                            code += f"\n\n# Assign result\nresult_df = {var_name}\n"
                            logger.info(f"Auto-added: result_df = {var_name}")
                            break
            
            logger.info("Generated transformation code")
            return code
            
        except Exception as e:
            logger.error(f"Failed to generate transformation code: {e}")
            raise
    
    def execute_transformation(self, code: str, dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Execute transformation code"""
        try:
            # Create execution environment
            exec_globals = {
                'pd': pd,
                'dataframes': dataframes,
                'result_df': None
            }
            
            # Import commonly used libraries
            import numpy as np
            exec_globals['np'] = np
            
            # Execute the code
            exec(code, exec_globals)
            
            result_df = exec_globals.get('result_df')
            
            # If result_df wasn't set, try to find any DataFrame variable
            if result_df is None:
                logger.warning("result_df not found, searching for other DataFrames...")
                candidates = []
                for var_name, var_value in exec_globals.items():
                    if isinstance(var_value, pd.DataFrame):
                        if var_name not in ['pd', 'dataframes'] and not var_name.startswith('_'):
                            candidates.append((var_name, var_value))
                            logger.info(f"Found DataFrame candidate: '{var_name}' with shape {var_value.shape}")
                
                # Use the last created DataFrame (most likely the result)
                if candidates:
                    result_df = candidates[-1][1]
                    logger.info(f"Using DataFrame '{candidates[-1][0]}' as result")
            
            if result_df is None:
                # Log what we have for debugging
                logger.error("Available variables:")
                for var_name, var_value in exec_globals.items():
                    if not var_name.startswith('__'):
                        logger.error(f"  {var_name}: {type(var_value)}")
                raise ValueError("Transformation code did not produce 'result_df' or any other DataFrame")
            
            logger.info(f"Transformation executed successfully: {len(result_df)} rows")
            return result_df
            
        except Exception as e:
            logger.error(f"Error executing transformation: {e}")
            # Log the generated code for debugging
            logger.error(f"Generated code was:\n{code}")
            raise
    
    def transform(self, user_query: str, dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Main transformation method"""
        
        # Prepare data info for AI
        data_info = {}
        for name, df in dataframes.items():
            data_info[name] = {
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'shape': df.shape,
                'sample': df.head(2).to_dict()
            }
        
        # Generate transformation code
        code = self.generate_transformation_code(user_query, data_info)
        
        # Execute transformation
        result_df = self.execute_transformation(code, dataframes)
        
        return result_df
    
    def iterative_transform(self, user_query: str, dataframes: Dict[str, pd.DataFrame], 
                           max_iterations: int = 3) -> pd.DataFrame:
        """Try transformation with retry logic"""
        last_error = None
        
        for attempt in range(max_iterations):
            try:
                result_df = self.transform(user_query, dataframes)
                return result_df
            except Exception as e:
                last_error = e
                logger.warning(f"Transformation attempt {attempt + 1} failed: {e}")
                
                if attempt < max_iterations - 1:
                    # Modify query to include error context
                    user_query = f"{user_query}\n\nPrevious attempt failed with error: {str(e)}. Please fix the code."
        
        raise Exception(f"Transformation failed after {max_iterations} attempts. Last error: {last_error}")