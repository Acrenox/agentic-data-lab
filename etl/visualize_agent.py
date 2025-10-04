"""
Visualization Agent - Creates visualizations based on data and user query
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, Optional, List
import google.generativeai as genai
import os
from pathlib import Path
from helpers import get_logger, clean_code_fences

logger = get_logger(__name__)

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

class VisualizeAgent:
    def __init__(self, api_key: Optional[str] = None):
        genai.configure(api_key=api_key or os.getenv("GOOGLE_API_KEY"))
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
    def generate_visualization_code(self, user_query: str, df: pd.DataFrame) -> str:
        """Generate matplotlib/seaborn visualization code using AI"""
        
        # Get data summary for context
        data_info = {
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'shape': df.shape,
            'numeric_columns': list(df.select_dtypes(include=['number']).columns),
            'categorical_columns': list(df.select_dtypes(include=['object', 'category']).columns),
            'sample': df.head(3).to_dict()
        }
        
        prompt = f"""You are a data visualization expert. Generate Python code using matplotlib and seaborn to visualize data based on this request:

User Query: "{user_query}"

DataFrame structure:
- Shape: {data_info['shape']}
- Columns: {', '.join(data_info['columns'])}
- Numeric columns: {', '.join(data_info['numeric_columns'])}
- Categorical columns: {', '.join(data_info['categorical_columns'])}
- Sample data: {data_info['sample']}

Requirements:
1. Use matplotlib (plt) and seaborn (sns) for visualization
2. The DataFrame is available as 'df'
3. Create clear, informative visualizations with proper labels
4. Use appropriate chart types (bar, line, scatter, heatmap, etc.)
5. Add titles, axis labels, and legends
6. Handle any data preparation needed (groupby, aggregations, etc.)
7. Use plt.tight_layout() before plt.savefig()
8. Save the figure to 'output/visualization.png'
9. Return ONLY the Python code, no explanations

Example format:
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Data preparation
# ... your code ...

# Create visualization
plt.figure(figsize=(12, 6))
# ... plotting code ...
plt.title('Your Title')
plt.xlabel('X Label')
plt.ylabel('Y Label')
plt.tight_layout()
plt.savefig('output/visualization.png', dpi=300, bbox_inches='tight')
plt.close()
"""
        
        try:
            response = self.model.generate_content(prompt)
            code = response.text
            code = clean_code_fences(code)
            logger.info("Generated visualization code")
            return code
            
        except Exception as e:
            logger.error(f"Failed to generate visualization code: {e}")
            raise
    
    def execute_visualization(self, code: str, df: pd.DataFrame, output_path: str = "output/visualization.png"):
        """Execute visualization code"""
        try:
            # Ensure output directory exists
            Path(output_path).parent.mkdir(exist_ok=True, parents=True)
            
            # Replace the output path in the generated code to use our specified path
            code = code.replace("'output/visualization.png'", f"'{output_path}'")
            code = code.replace('"output/visualization.png"', f'"{output_path}"')
            
            # Create execution environment
            exec_globals = {
                'pd': pd,
                'df': df,
                'plt': plt,
                'sns': sns
            }
            
            # Import numpy as it's commonly used
            import numpy as np
            exec_globals['np'] = np
            
            # Execute the code
            exec(code, exec_globals)
            
            # Verify the file was created
            if not Path(output_path).exists():
                # Try to find it in the root output folder
                alt_path = "output/visualization.png"
                if Path(alt_path).exists():
                    import shutil
                    shutil.move(alt_path, output_path)
                    logger.info(f"Moved visualization from {alt_path} to {output_path}")
            
            logger.info(f"Visualization saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error executing visualization: {e}")
            raise
    
    def create_visualization(self, user_query: str, df: pd.DataFrame, 
                           output_path: str = "output/visualization.png") -> str:
        """Main visualization method"""
        
        # Generate visualization code
        code = self.generate_visualization_code(user_query, df)
        
        # Log the generated code for debugging
        logger.info(f"Generated code:\n{code}")
        
        # Execute visualization
        viz_path = self.execute_visualization(code, df, output_path)
        
        return viz_path
    
    def create_multiple_visualizations(self, user_query: str, df: pd.DataFrame, 
                                      output_dir: str = "output") -> List[str]:
        """Create multiple related visualizations"""
        
        prompt = f"""You are a data visualization expert. Based on this user query: "{user_query}"

DataFrame info:
- Columns: {', '.join(df.columns)}
- Shape: {df.shape}

Suggest 2-3 different visualizations that would best answer the user's question.
Respond in JSON format:
[
    {{"type": "bar", "title": "Title 1", "description": "What it shows"}},
    {{"type": "line", "title": "Title 2", "description": "What it shows"}}
]
"""
        
        try:
            response = self.model.generate_content(prompt)
            import json
            import re
            json_match = re.search(r'\[.*?\]', response.text, re.DOTALL)
            
            if json_match:
                viz_configs = json.loads(json_match.group())
                viz_paths = []
                
                for i, config in enumerate(viz_configs):
                    modified_query = f"{user_query}. Create a {config['type']} chart showing {config['description']}"
                    output_path = f"{output_dir}/visualization_{i+1}.png"
                    viz_path = self.create_visualization(modified_query, df, output_path)
                    viz_paths.append(viz_path)
                
                return viz_paths
            else:
                # Fallback to single visualization
                return [self.create_visualization(user_query, df)]
                
        except Exception as e:
            logger.error(f"Multiple visualizations failed: {e}")
            return [self.create_visualization(user_query, df)]
    
    def generate_analysis_report(self, user_query: str, df: pd.DataFrame) -> str:
        """Generate text analysis report"""
        
        # Get summary stats
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        desc_stats = df[numeric_cols].describe().to_dict() if numeric_cols else {}
        
        prompt = f"""Analyze this data and provide insights based on the user's query: "{user_query}"

DataFrame summary:
- Shape: {df.shape}
- Columns: {', '.join(df.columns)}
- Descriptive statistics: {desc_stats}
- Missing values: {df.isnull().sum().to_dict()}

Provide a concise analysis with key findings and insights (2-3 paragraphs)."""
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            logger.error(f"Failed to generate analysis: {e}")
            return f"Analysis generation failed: {str(e)}"