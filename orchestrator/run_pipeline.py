"""
ETL Pipeline Orchestrator - Manages the complete ETL workflow
"""
import sys
sys.path.append('etl')

from etl.extract_agent import ExtractAgent
from etl.transform_agent import TransformAgent
from etl.load_agent import LoadAgent
from etl.visualize_agent import VisualizeAgent
from etl.helpers import get_logger, save_metadata
from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime

logger = get_logger(__name__)

class ETLPipeline:
    def __init__(self, api_key: Optional[str] = None):
        self.extract_agent = ExtractAgent(api_key)
        self.transform_agent = TransformAgent(api_key)
        self.load_agent = LoadAgent(api_key)
        self.analyze_agent = VisualizeAgent(api_key)
        self.metadata = {}
        
    def run(self, user_query: str, 
            data_directory: str = "processed",
            skip_transform: bool = False,
            skip_load: bool = False,
            analyze: bool = True) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline
        
        Args:
            user_query: Natural language query from user
            data_directory: Directory containing source data files
            skip_transform: Skip transformation step
            skip_load: Skip loading step
            analyze: Run analysis and visualization
            
        Returns:
            Dictionary with pipeline results
        """
        
        logger.info(f"Starting ETL pipeline for query: {user_query}")
        pipeline_start = datetime.now()
        results = {
            'query': user_query,
            'start_time': pipeline_start.isoformat(),
            'stages': {}
        }
        
        try:
            # Stage 1: Extract
            logger.info("=" * 50)
            logger.info("STAGE 1: EXTRACTION")
            logger.info("=" * 50)
            
            extracted_data = self.extract_agent.ai_assisted_extract(
                user_query, 
                data_directory
            )
            
            if not extracted_data:
                raise ValueError("No data extracted. Check data directory and file formats.")
            
            results['stages']['extract'] = {
                'status': 'success',
                'datasets': list(extracted_data.keys()),
                'row_counts': {name: len(df) for name, df in extracted_data.items()}
            }
            logger.info(f"Extracted {len(extracted_data)} datasets")
            
            # Stage 2: Transform
            if not skip_transform:
                logger.info("=" * 50)
                logger.info("STAGE 2: TRANSFORMATION")
                logger.info("=" * 50)
                
                transformed_df = self.transform_agent.iterative_transform(
                    user_query,
                    extracted_data
                )
                
                results['stages']['transform'] = {
                    'status': 'success',
                    'output_shape': transformed_df.shape,
                    'columns': list(transformed_df.columns)
                }
                logger.info(f"Transformed data: {transformed_df.shape}")
            else:
                # Use first dataset if no transformation
                transformed_df = list(extracted_data.values())[0]
                results['stages']['transform'] = {
                    'status': 'skipped'
                }
            
            # Stage 3: Load
            if not skip_load:
                logger.info("=" * 50)
                logger.info("STAGE 3: LOADING")
                logger.info("=" * 50)
                
                load_destination = self.load_agent.load(
                    transformed_df,
                    user_query
                )
                
                results['stages']['load'] = {
                    'status': 'success',
                    'destination': load_destination
                }
                logger.info(f"Loaded to: {load_destination}")
            else:
                results['stages']['load'] = {
                    'status': 'skipped'
                }
            
            # Stage 4: Analyze and Visualize
            if analyze:
                logger.info("=" * 50)
                logger.info("STAGE 4: ANALYSIS & VISUALIZATION")
                logger.info("=" * 50)
                
                analysis_results = self.analyze_agent.iterative_analyze(
                    user_query,
                    transformed_df
                )
                
                # Generate summary
                summary = self.analyze_agent.generate_summary(
                    user_query,
                    transformed_df,
                    analysis_results
                )
                
                results['stages']['analyze'] = {
                    'status': 'success',
                    'analysis': analysis_results,
                    'summary': summary
                }
                logger.info(f"Analysis complete. Generated {len(analysis_results.get('plots', []))} plots")
                logger.info(f"\nSUMMARY:\n{summary}")
            else:
                results['stages']['analyze'] = {
                    'status': 'skipped'
                }
            
            # Pipeline completion
            pipeline_end = datetime.now()
            results['end_time'] = pipeline_end.isoformat()
            results['duration_seconds'] = (pipeline_end - pipeline_start).total_seconds()
            results['status'] = 'success'
            
            # Save metadata
            save_metadata(results, 'pipeline_metadata.json')
            
            logger.info("=" * 50)
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {results['duration_seconds']:.2f}s")
            logger.info("=" * 50)
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            results['status'] = 'failed'
            results['error'] = str(e)
            results['end_time'] = datetime.now().isoformat()
            return results
        
        finally:
            # Cleanup
            self.load_agent.close()

def main():
    """Main entry point for the pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Agentic ETL Pipeline')
    parser.add_argument('query', type=str, help='Natural language query')
    parser.add_argument('--data-dir', type=str, default='processed', 
                       help='Directory containing source data files')
    parser.add_argument('--skip-transform', action='store_true',
                       help='Skip transformation stage')
    parser.add_argument('--skip-load', action='store_true',
                       help='Skip loading stage')
    parser.add_argument('--no-analyze', action='store_true',
                       help='Skip analysis and visualization')
    parser.add_argument('--api-key', type=str, default=None,
                       help='Google API key (or set GOOGLE_API_KEY env var)')
    
    args = parser.parse_args()
    
    # Create and run pipeline
    pipeline = ETLPipeline(api_key=args.api_key)
    results = pipeline.run(
        user_query=args.query,
        data_directory=args.data_dir,
        skip_transform=args.skip_transform,
        skip_load=args.skip_load,
        analyze=not args.no_analyze
    )
    
    # Print results
    print("\n" + "=" * 50)
    print("PIPELINE RESULTS")
    print("=" * 50)
    print(f"Status: {results['status']}")
    print(f"Duration: {results.get('duration_seconds', 0):.2f}s")
    
    if results.get('stages', {}).get('analyze', {}).get('summary'):
        print("\n" + results['stages']['analyze']['summary'])
    
    if results.get('stages', {}).get('analyze', {}).get('analysis', {}).get('plots'):
        print(f"\nGenerated plots: {results['stages']['analyze']['analysis']['plots']}")

if __name__ == "__main__":
    main()