"""
Standalone Agentic ETL Pipeline Runner
Run this directly without package installation
"""
import sys
import os
from pathlib import Path

# Ensure all modules can be imported
sys.path.insert(0, str(Path(__file__).parent / "etl"))
sys.path.insert(0, str(Path(__file__).parent / "orchestrator"))

# Import after path is set
from etl.extract_agent import ExtractAgent
from etl.transform_agent import TransformAgent
from etl.load_agent import LoadAgent
from etl.visualize_agent import VisualizeAgent
from etl.helpers import get_logger, save_metadata

logger = get_logger(__name__)

class AgenticPipeline:
    def __init__(self, api_key=None):
        """Initialize the agentic ETL pipeline"""
        self.extract_agent = ExtractAgent(api_key)
        self.transform_agent = TransformAgent(api_key)
        self.load_agent = LoadAgent(api_key)
        self.visualize_agent = VisualizeAgent(api_key)
        
        logger.info("Agentic ETL Pipeline initialized")
    
    def run(self, user_query, data_directory="processed", 
            output_directory="output", create_viz=True):
        """Run the complete pipeline based on user query"""
        
        logger.info(f"Starting pipeline for query: {user_query}")
        
        try:
            # Step 1: EXTRACT
            logger.info("=" * 50)
            logger.info("STEP 1: EXTRACTION")
            logger.info("=" * 50)
            
            extracted_data = self.extract_agent.ai_assisted_extract(user_query, data_directory)
            
            if not extracted_data:
                logger.error("No data extracted. Pipeline terminated.")
                return None
            
            logger.info(f"Extracted {len(extracted_data)} dataset(s)")
            for name, df in extracted_data.items():
                logger.info(f"  - {name}: {df.shape}")
            
            # Step 2: TRANSFORM
            logger.info("=" * 50)
            logger.info("STEP 2: TRANSFORMATION")
            logger.info("=" * 50)
            
            transformed_df = self.transform_agent.iterative_transform(user_query, extracted_data)
            
            logger.info(f"Transformation complete: {transformed_df.shape}")
            logger.info(f"Columns: {', '.join(transformed_df.columns)}")
            
            # Step 3: LOAD
            logger.info("=" * 50)
            logger.info("STEP 3: LOADING")
            logger.info("=" * 50)
            
            self.load_agent.ai_assisted_load(transformed_df, user_query, output_directory)
            
            # Save result as CSV
            Path(output_directory).mkdir(exist_ok=True, parents=True)
            result_path = f"{output_directory}/result.csv"
            transformed_df.to_csv(result_path, index=False)
            logger.info(f"Result saved to: {result_path}")
            
            # Step 4: VISUALIZE
            if create_viz:
                logger.info("=" * 50)
                logger.info("STEP 4: VISUALIZATION & ANALYSIS")
                logger.info("=" * 50)
                
                # Create visualization
                viz_path = self.visualize_agent.create_visualization(
                    user_query, 
                    transformed_df, 
                    f"{output_directory}/visualization.png"
                )
                logger.info(f"Visualization saved to: {viz_path}")
                
                # Generate analysis
                analysis = self.visualize_agent.generate_analysis_report(user_query, transformed_df)
                
                # Save analysis
                analysis_path = f"{output_directory}/analysis.txt"
                with open(analysis_path, 'w') as f:
                    f.write(f"Query: {user_query}\n\n")
                    f.write("=" * 80 + "\n")
                    f.write("ANALYSIS REPORT\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(analysis)
                    f.write("\n\n" + "=" * 80 + "\n")
                    f.write(f"Data Shape: {transformed_df.shape}\n")
                    f.write(f"Columns: {', '.join(transformed_df.columns)}\n")
                
                logger.info(f"Analysis saved to: {analysis_path}")
                
                # Print analysis
                print("\n" + "=" * 80)
                print("ANALYSIS REPORT")
                print("=" * 80)
                print(analysis)
                print("=" * 80 + "\n")
            
            # Save metadata
            pipeline_metadata = {
                'query': user_query,
                'extracted_sources': list(extracted_data.keys()),
                'result_shape': transformed_df.shape,
                'result_columns': list(transformed_df.columns),
                'output_files': {
                    'result': result_path,
                    'visualization': f"{output_directory}/visualization.png" if create_viz else None,
                    'analysis': f"{output_directory}/analysis.txt" if create_viz else None
                }
            }
            save_metadata(pipeline_metadata, f"{output_directory}/pipeline_metadata.json")
            
            logger.info("=" * 50)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 50)
            
            return transformed_df
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            self.load_agent.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Agentic ETL Pipeline')
    parser.add_argument('query', type=str, help='Natural language query')
    parser.add_argument('--data-dir', type=str, default='processed', help='Data directory')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory')
    parser.add_argument('--no-viz', action='store_true', help='Skip visualization')
    
    args = parser.parse_args()
    
    # Check for API key
    if not os.getenv("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY environment variable not set!")
        print("Set it with: $env:GOOGLE_API_KEY='your-key-here'")
        sys.exit(1)
    
    # Run pipeline
    pipeline = AgenticPipeline()
    pipeline.run(
        user_query=args.query,
        data_directory=args.data_dir,
        output_directory=args.output_dir,
        create_viz=not args.no_viz
    )