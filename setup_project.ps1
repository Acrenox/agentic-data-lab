# Setup script for Agentic Data Lab project
# Run this in PowerShell: .\setup_project.ps1

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Setting up Agentic Data Lab Project" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Create directory structure
Write-Host "Creating directories..." -ForegroundColor Yellow
$directories = @("etl", "orchestrator", "processed", "output")
foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
        Write-Host "  ✓ Created: $dir" -ForegroundColor Green
    } else {
        Write-Host "  ✓ Exists: $dir" -ForegroundColor Gray
    }
}

# Create __init__.py files
Write-Host "`nCreating Python package files..." -ForegroundColor Yellow
@("etl\__init__.py", "orchestrator\__init__.py") | ForEach-Object {
    if (!(Test-Path $_)) {
        "" | Out-File -FilePath $_ -Encoding utf8
        Write-Host "  ✓ Created: $_" -ForegroundColor Green
    } else {
        Write-Host "  ✓ Exists: $_" -ForegroundColor Gray
    }
}

# Check for required Python files
Write-Host "`nChecking Python files..." -ForegroundColor Yellow
$required_files = @(
    "etl\helpers.py",
    "etl\extract_agent.py",
    "etl\transform_agent.py",
    "etl\load_agent.py",
    "etl\visualize_agent.py"
)

$missing_files = @()
foreach ($file in $required_files) {
    if (Test-Path $file) {
        Write-Host "  ✓ Found: $file" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Missing: $file" -ForegroundColor Red
        $missing_files += $file
    }
}

# Check for data files
Write-Host "`nChecking data files in 'processed' folder..." -ForegroundColor Yellow
$data_files = Get-ChildItem -Path "processed" -File -ErrorAction SilentlyContinue
if ($data_files) {
    Write-Host "  ✓ Found $($data_files.Count) file(s)" -ForegroundColor Green
    $data_files | ForEach-Object { Write-Host "    - $($_.Name)" -ForegroundColor Gray }
} else {
    Write-Host "  ✗ No data files found in 'processed' folder" -ForegroundColor Red
    Write-Host "    Please add your CSV/Excel files to the 'processed' folder" -ForegroundColor Yellow
}

# Summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Setup Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if ($missing_files.Count -gt 0) {
    Write-Host "`n⚠️  Missing Files:" -ForegroundColor Red
    $missing_files | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "`nPlease create these files with the provided code." -ForegroundColor Yellow
} else {
    Write-Host "`n✓ All required files present!" -ForegroundColor Green
}

Write-Host "`nNext Steps:" -ForegroundColor Cyan
Write-Host "1. Set your Google API key:" -ForegroundColor White
Write-Host "   `$env:GOOGLE_API_KEY='your-api-key-here'" -ForegroundColor Gray
Write-Host "`n2. Install dependencies:" -ForegroundColor White
Write-Host "   uv pip install pandas numpy matplotlib seaborn google-generativeai duckdb openpyxl xlrd pyarrow" -ForegroundColor Gray
Write-Host "`n3. Run the example:" -ForegroundColor White
Write-Host "   python example_usage.py" -ForegroundColor Gray
Write-Host ""