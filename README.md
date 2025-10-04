# Agentic Data Lab

An AI-powered ETL (Extract, Transform, Load) pipeline that uses natural language queries to automatically analyze and visualize data. Built with Google's Gemini 2.0 Flash model.

## Overview

This system transforms how you interact with data by replacing manual data analysis with conversational AI agents. Simply ask questions in plain English, and the pipeline will:

1. **Extract** relevant data files
2. **Transform** data based on your query
3. **Load** results to appropriate destinations
4. **Visualize** insights with charts and reports

## Features

- **Natural Language Interface**: Ask questions like "Show me the top 10 drivers by career points"
- **Intelligent File Selection**: AI determines which data files are relevant
- **Automated Transformations**: Generates and executes pandas code automatically
- **Smart Visualization**: Creates appropriate charts (bar, line, scatter, multi-panel)
- **Comprehensive Analysis**: AI-generated insights and statistical summaries
- **Multiple Output Formats**: CSV, Excel, Parquet, or DuckDB tables

## Project Structure

```
agentic-data-lab/
├── etl/                          # Core ETL agents
│   ├── __init__.py
│   ├── helpers.py                # Shared utilities (logging, metadata)
│   ├── extract_agent.py          # Data extraction with AI file selection
│   ├── transform_agent.py        # AI-powered pandas transformations
│   ├── load_agent.py             # Intelligent data loading
│   └── visualize_agent.py        # Chart generation and analysis
├── orchestrator/                 # Pipeline orchestration
│   ├── __init__.py
│   └── run_pipeline.py           # Complete pipeline runner
├── processed/                    # Input data directory
│   ├── drivers_clean.csv
│   ├── races_clean.csv
│   └── results_clean.csv
├── output/                       # Generated results
│   ├── {query_name}/
│   │   ├── result.csv            # Transformed data
│   │   ├── visualization.png     # Generated chart
│   │   ├── analysis.txt          # AI analysis report
│   │   └── pipeline_metadata.json
├── example_usage.py              # Interactive demo with examples
├── simple_run.py                 # Command-line runner
├── requirements.txt              # Python dependencies
├── pyproject.toml                # Project configuration
└── README.md                     # This file
```

## Prerequisites

- Python 3.9+
- Google API Key (Gemini 2.0 Flash)
- Data files in CSV, Excel, JSON, or Parquet format

## Installation

### 1. Clone or Download the Project

```bash
cd agentic-data-lab
```

### 2. Install Dependencies

Using `uv` (recommended):
```bash
uv pip install -r requirements.txt
```

Using standard `pip`:
```bash
pip install -r requirements.txt
```

### 3. Set Google API Key

Get your API key from [Google AI Studio](https://aistudio.google.com/app/apikey)

**Windows PowerShell:**
```powershell
$env:GOOGLE_API_KEY="your-api-key-here"
```

**Linux/Mac:**
```bash
export GOOGLE_API_KEY="your-api-key-here"
```

**Persistent (add to `.env` file):**
```bash
echo "GOOGLE_API_KEY=your-api-key-here" > .env
```

## Usage

### Interactive Mode (Recommended)

Run the interactive menu with pre-built examples:

```bash
python example_usage.py
```

**Available Examples:**
1. Top 10 Drivers by Career Points
2. Compare Driver Performance (1950s era)
3. Yearly Trends Analysis
4. Statistical Summary of Drivers
5. Custom Query (ask your own question)
6. Winners Analysis

### Command Line Mode

```bash
python simple_run.py "Show me the top 5 drivers by total wins"
```

**Options:**
```bash
python simple_run.py "your query" \
  --data-dir processed \
  --output-dir output/custom \
  --no-viz  # Skip visualization
```

## How It Works

### Architecture

```
┌─────────────┐
│ User Query  │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  Extract Agent      │  ← AI selects relevant files
│  - Scans data dir   │
│  - Uses AI to pick  │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Transform Agent     │  ← Generates pandas code
│  - Creates code     │
│  - Executes safely  │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Load Agent        │  ← Chooses best format
│  - Determines dest  │
│  - Saves results    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Visualize Agent     │  ← Creates charts
│  - Generates plots  │
│  - Writes analysis  │
└─────────────────────┘
```

### Agent Details

#### 1. Extract Agent
- Scans `processed/` for data files
- Analyzes user query to determine relevance
- Loads only necessary files into memory
- Saves metadata about extracted data

#### 2. Transform Agent
- Generates pandas transformation code via Gemini
- Executes code in isolated environment
- Retry logic with error feedback to AI
- Returns transformed DataFrame

#### 3. Load Agent
- AI determines optimal storage format
- Supports: DuckDB, CSV, Excel, Parquet
- Handles database connections
- Sanitizes column names for SQL compatibility

#### 4. Visualize Agent
- Generates matplotlib/seaborn code
- Creates appropriate chart types
- Generates AI-written insights
- Saves high-resolution images (300 DPI)

## Example Queries

### Data Exploration
```
"Show me all drivers with more than 50 wins"
"What are the top 10 circuits by number of races?"
"Compare Lewis Hamilton and Michael Schumacher statistics"
```

### Statistical Analysis
```
"Calculate the average finishing position for each constructor"
"Show the distribution of career points across all drivers"
"Find drivers with the highest win percentage"
```

### Trend Analysis
```
"How has the number of races changed over decades?"
"Show the trend of average points per race by year"
"Analyze constructor dominance over time"
```

### Comparisons
```
"Compare the top 5 teams by total podiums"
"Which decade had the most competitive racing?"
"Compare driver performance across different eras"
```

## Output Files

For each query, the system generates:

### 1. `result.csv`
Transformed data ready for further analysis
```csv
full_name,total_points,wins
Lewis Hamilton,4820.5,105.0
Sebastian Vettel,3098.0,53.0
```

### 2. `visualization.png`
High-quality chart (300 DPI, publication-ready)
- Bar charts, line plots, scatter plots
- Multi-panel comparisons
- Properly labeled axes and legends

### 3. `analysis.txt`
AI-generated insights including:
- Key findings
- Statistical observations
- Data quality notes
- Recommended follow-up questions

### 4. `pipeline_metadata.json`
Execution metadata:
```json
{
  "query": "Show top drivers",
  "extracted_sources": ["drivers_clean", "results_clean"],
  "result_shape": [10, 3],
  "result_columns": ["full_name", "total_points", "wins"]
}
```

## Configuration

### Customizing Agents

Edit agent parameters in the respective files:

**Extract Agent** (`etl/extract_agent.py`):
- Change supported file types
- Modify file selection logic
- Add custom extractors

**Transform Agent** (`etl/transform_agent.py`):
- Adjust retry attempts (default: 3)
- Modify prompt engineering
- Add custom transformations

**Visualize Agent** (`etl/visualize_agent.py`):
- Change plot styles
- Adjust figure sizes
- Customize color palettes

### Adding New Data Sources

1. Place files in `processed/` directory
2. Supported formats: CSV, Excel, JSON, Parquet
3. System automatically detects and analyzes them

## Troubleshooting

### "result_df not found" Error

The AI occasionally forgets to assign the final DataFrame. The system has auto-fix logic, but if it persists:
- Simplify your query
- Use more specific column names
- Check the generated code in logs

### Visualization Not Generated

- Ensure matplotlib and seaborn are installed
- Check that output directory is writable
- Look in `output/visualization.png` (might be in root)

### API Rate Limits

If you hit Gemini API rate limits:
- Add delays between queries
- Reduce retry attempts in `transform_agent.py`
- Use more specific queries to reduce iterations

### No Data Extracted

- Check file names match patterns (*.csv, *.xlsx, etc.)
- Ensure files are in `processed/` directory
- Verify files are not corrupted

## Performance Tips

1. **Pre-clean your data**: Remove unnecessary columns before analysis
2. **Use specific queries**: "Top 10 drivers by wins" vs "Show me driver data"
3. **Limit data size**: Large datasets (>1M rows) may be slow
4. **Cache results**: Reuse transformed data for multiple visualizations

## Security Notes

- Never commit API keys to version control
- Use environment variables or `.env` files
- Keep `service_account.json` files private
- Sanitize user inputs if building a web interface

## Dependencies

Core libraries:
- `pandas` - Data manipulation
- `google-generativeai` - Gemini AI
- `matplotlib` & `seaborn` - Visualization
- `duckdb` - Database support
- `openpyxl` - Excel support

## Limitations

- AI-generated code may occasionally fail (retry logic helps)
- Complex statistical analysis may need manual review
- Limited to pandas operations (no Spark/SQL)
- Gemini API rate limits apply
- English queries work best

## Future Enhancements

Potential additions:
- [ ] Multi-language support
- [ ] Interactive dashboards (Streamlit/Dash)
- [ ] Database connectors (PostgreSQL, MySQL)
- [ ] Real-time data streaming
- [ ] Automated data quality checks
- [ ] Export to PowerPoint/PDF reports
- [ ] Scheduled pipeline execution
- [ ] Version control for transformations

## Contributing

To extend the system:

1. Add new agents in `etl/` directory
2. Implement the agent interface pattern
3. Register in orchestrator
4. Add examples to `example_usage.py`

## License

This project is provided as-is for educational and commercial use.

## Support

For issues or questions:
1. Check troubleshooting section
2. Review generated logs in console
3. Inspect `pipeline_metadata.json` files
4. Test with simpler queries first

## Credits

Built with:
- Google Gemini 2.0 Flash
- pandas Data Analysis Library
- matplotlib & seaborn Visualization
- DuckDB Analytical Database

---

**Note**: This is an AI-powered system. Always validate critical results manually before making business decisions.