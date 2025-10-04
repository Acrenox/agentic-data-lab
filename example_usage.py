"""
Example usage of the Agentic ETL Pipeline
Interactive script with various example queries
"""
import sys
import os
from pathlib import Path

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent / "etl"))
sys.path.insert(0, str(Path(__file__).parent / "orchestrator"))

from extract_agent import ExtractAgent
from transform_agent import TransformAgent
from load_agent import LoadAgent
from visualize_agent import VisualizeAgent
from helpers import get_logger

logger = get_logger(__name__)


class AgenticPipeline:
    """Simplified pipeline class for examples"""
    def __init__(self, api_key=None):
        self.extract_agent = ExtractAgent(api_key)
        self.transform_agent = TransformAgent(api_key)
        self.load_agent = LoadAgent(api_key)
        self.visualize_agent = VisualizeAgent(api_key)
        logger.info("Pipeline initialized")
    
    def run(self, user_query, data_directory="processed", output_directory="output", create_viz=True):
        """Run the complete pipeline"""
        logger.info(f"Query: {user_query}")
        
        try:
            # Extract
            print("\n🔍 Extracting data...")
            extracted_data = self.extract_agent.ai_assisted_extract(user_query, data_directory)
            
            if not extracted_data:
                print("❌ No data found!")
                return None
            
            print(f"✅ Extracted {len(extracted_data)} dataset(s)")
            
            # Transform
            print("\n🔄 Transforming data...")
            transformed_df = self.transform_agent.iterative_transform(user_query, extracted_data)
            print(f"✅ Transformed: {transformed_df.shape[0]} rows, {transformed_df.shape[1]} columns")
            
            # Load
            print("\n💾 Saving results...")
            Path(output_directory).mkdir(exist_ok=True, parents=True)
            result_path = f"{output_directory}/result.csv"
            transformed_df.to_csv(result_path, index=False)
            print(f"✅ Saved to: {result_path}")
            
            # Visualize
            if create_viz:
                print("\n📊 Creating visualizations...")
                viz_path = self.visualize_agent.create_visualization(
                    user_query, transformed_df, f"{output_directory}/visualization.png"
                )
                print(f"✅ Visualization saved: {viz_path}")
                
                print("\n📝 Generating analysis...")
                analysis = self.visualize_agent.generate_analysis_report(user_query, transformed_df)
                
                analysis_path = f"{output_directory}/analysis.txt"
                with open(analysis_path, 'w', encoding='utf-8') as f:
                    f.write(f"Query: {user_query}\n\n")
                    f.write("=" * 80 + "\n")
                    f.write("ANALYSIS REPORT\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(analysis)
                
                print(f"✅ Analysis saved: {analysis_path}")
                print("\n" + "=" * 80)
                print("ANALYSIS PREVIEW")
                print("=" * 80)
                print(analysis[:500] + "..." if len(analysis) > 500 else analysis)
                print("=" * 80)
            
            print("\n✨ Pipeline completed successfully!\n")
            return transformed_df
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return None
        finally:
            self.load_agent.close()


def example_1_top_drivers():
    """Example 1: Top F1 drivers analysis"""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: TOP F1 DRIVERS BY CAREER POINTS")
    print("=" * 80)
    
    pipeline = AgenticPipeline()
    
    query = """
    Show me the top 10 Formula 1 drivers by total career points.
    Include their name, total points, and number of wins if available.
    Create a horizontal bar chart showing the top 10 drivers with their career points.
    """
    
    result = pipeline.run(
        user_query=query,
        data_directory="processed",
        output_directory="output/top_drivers"
    )
    
    if result is not None:
        print("\n📊 Top 5 drivers:")
        print(result.head().to_string())


def example_2_driver_comparison():
    """Example 2: Compare specific drivers"""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: DRIVER COMPARISON")
    print("=" * 80)
    
    pipeline = AgenticPipeline()
    
    query = """
    Compare the performance of drivers from the 1950s era.
    Show their career points, average finishing position, and total podiums.
    Create a multi-panel visualization comparing these metrics.
    """
    
    result = pipeline.run(
        user_query=query,
        data_directory="processed",
        output_directory="output/driver_comparison"
    )
    
    return result


def example_3_yearly_trends():
    """Example 3: Yearly trends analysis"""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: YEARLY TRENDS")
    print("=" * 80)
    
    pipeline = AgenticPipeline()
    
    query = """
    Analyze Formula 1 trends over the years in the dataset.
    Show the number of races per year and average points scored.
    Create a line chart showing these trends over time.
    """
    
    result = pipeline.run(
        user_query=query,
        data_directory="processed",
        output_directory="output/yearly_trends"
    )
    
    return result


def example_4_statistical_summary():
    """Example 4: Statistical summary"""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: STATISTICAL SUMMARY")
    print("=" * 80)
    
    pipeline = AgenticPipeline()
    
    query = """
    Provide a comprehensive statistical summary of F1 driver performance.
    Include mean, median, and standard deviation of career points.
    Create a distribution plot showing the spread of career points across all drivers.
    """
    
    result = pipeline.run(
        user_query=query,
        data_directory="processed",
        output_directory="output/statistical_summary"
    )
    
    return result


def example_5_custom_query():
    """Example 5: Custom user query"""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: CUSTOM QUERY")
    print("=" * 80)
    
    print("\nYou can ask questions like:")
    print("  - 'Which drivers had the most wins in their career?'")
    print("  - 'Show me drivers with more than 100 career points'")
    print("  - 'Compare the top 5 drivers by average finishing position'")
    print("  - 'Analyze race data and show trends over decades'")
    print()
    
    query = input("Enter your query: ").strip()
    
    if not query:
        print("No query entered. Skipping...")
        return None
    
    pipeline = AgenticPipeline()
    
    result = pipeline.run(
        user_query=query,
        data_directory="processed",
        output_directory="output/custom_query"
    )
    
    return result


def example_6_winners_analysis():
    """Example 6: Winners analysis"""
    print("\n" + "=" * 80)
    print("EXAMPLE 6: WINNERS ANALYSIS")
    print("=" * 80)
    
    pipeline = AgenticPipeline()
    
    query = """
    Find all drivers who have won at least one race.
    Show their total wins, career points, and win percentage.
    Create a scatter plot showing the relationship between wins and career points.
    """
    
    result = pipeline.run(
        user_query=query,
        data_directory="processed",
        output_directory="output/winners_analysis"
    )
    
    return result


def main():
    """Main menu for example selection"""
    
    # Check for API key
    if not os.getenv("GOOGLE_API_KEY"):
        print("\n" + "=" * 80)
        print("⚠️  WARNING: GOOGLE_API_KEY not set!")
        print("=" * 80)
        print("\nPlease set your Google API key:")
        print("  PowerShell: $env:GOOGLE_API_KEY='your-key-here'")
        print("  Linux/Mac: export GOOGLE_API_KEY='your-key-here'")
        print("\nGet your API key from: https://aistudio.google.com/app/apikey")
        print("=" * 80 + "\n")
        return
    
    print("\n" + "=" * 80)
    print("🏎️  AGENTIC ETL PIPELINE - F1 DATA ANALYSIS")
    print("=" * 80)
    print("\nSelect an example to run:\n")
    print("  1. Top 10 Drivers by Career Points")
    print("  2. Compare Driver Performance (1950s era)")
    print("  3. Yearly Trends Analysis")
    print("  4. Statistical Summary of Drivers")
    print("  5. Custom Query (Ask your own question)")
    print("  6. Winners Analysis")
    print("  0. Exit")
    print()
    
    while True:
        try:
            choice = input("Enter choice (0-6): ").strip()
            
            if choice == "0":
                print("\n👋 Goodbye!\n")
                break
            elif choice == "1":
                example_1_top_drivers()
            elif choice == "2":
                example_2_driver_comparison()
            elif choice == "3":
                example_3_yearly_trends()
            elif choice == "4":
                example_4_statistical_summary()
            elif choice == "5":
                example_5_custom_query()
            elif choice == "6":
                example_6_winners_analysis()
            else:
                print("❌ Invalid choice. Please enter 0-6.")
                continue
            
            # Ask if user wants to continue
            print()
            continue_choice = input("Run another example? (y/n): ").strip().lower()
            if continue_choice != 'y':
                print("\n👋 Goodbye!\n")
                break
                
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted. Goodbye!\n")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}\n")
            logger.error(f"Example failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()