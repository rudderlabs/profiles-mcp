# Optimal Pilot Dates Feature

This document describes the new feature that intelligently suggests begin_time and end_time for pilot/dry runs to achieve fast execution.

## Overview

Previously, begin_time selection was arbitrary and test runs could take an hour or more. This new feature analyzes input tables to find the maximum timestamp and suggests using a recent time window (default: last 7 days) for faster testing.

## Implementation

### 1. New Snowflake Method

Added `suggest_optimal_pilot_dates()` method to `src/tools/snowflake.py`:

- **Purpose**: Analyzes input tables to find optimal date ranges for pilot runs
- **Parameters**:
  - `input_tables`: List of fully qualified table names (e.g., ["DB.SCHEMA.TABLE1"])
  - `target_duration_days`: Desired test duration (default: 7 days)
- **Returns**: Comprehensive analysis with recommended dates and alternatives

### 2. New MCP Tool

Added `suggest_optimal_pilot_dates()` tool to `src/main.py`:

- Exposes the Snowflake method as an MCP tool
- Requires active Snowflake connection (call `initialize_snowflake_connection()` first)
- Provides detailed documentation and usage examples

### 3. Dependencies

Added `python-dateutil>=2.8.0` to `pyproject.toml` for robust timestamp parsing.

## How It Works

1. **Table Analysis**: Scans input tables for common timestamp columns (timestamp, sent_at, received_at, etc.)
2. **Max Timestamp Discovery**: Finds the latest timestamp across all input tables
3. **Date Range Calculation**: Suggests a time window ending at the max timestamp
4. **Multiple Options**: Provides recommended, conservative (1 day), and extended (14 days) alternatives
5. **Data Freshness Assessment**: Warns if data is stale and provides confidence levels

## Usage

### Basic Usage
```python
# After initializing Snowflake connection
tables = ["ANALYTICS.PROD.EVENTS", "ANALYTICS.PROD.USERS"]
result = suggest_optimal_pilot_dates(tables, target_duration_days=7)

if result["success"]:
    recommended = result["recommended"]
    print(f"Use: pb run --begin_time '{recommended['begin_time']}' --end_time '{recommended['end_time']}'")
```

### Example Output
```json
{
  "success": true,
  "recommended": {
    "begin_time": "2025-07-19T11:31:28Z",
    "end_time": "2025-07-26T11:31:28Z", 
    "duration_days": 7,
    "rationale": "Using last 7 days of data ending at 2025-07-26T11:31:28Z for optimal balance of speed and data coverage",
    "confidence": "high"
  },
  "alternatives": [
    {
      "name": "conservative",
      "duration_days": 1,
      "begin_time": "2025-07-25T11:31:28Z",
      "end_time": "2025-07-26T11:31:28Z",
      "rationale": "Ultra-fast execution with minimal data for quick validation"
    }
  ],
  "analysis": {
    "data_freshness": "current",
    "days_since_last_data": 1,
    "tables_analyzed": 2,
    "tables_with_data": 2
  }
}
```

## Key Features

### Smart Timestamp Detection
- Automatically finds timestamp columns in tables
- Handles multiple timestamp formats and column names
- Works with TIMESTAMP_NTZ, TIMESTAMP_LTZ, and other Snowflake types

### Multiple Duration Options
- **1-3 days**: Ultra-fast testing
- **7 days**: Recommended for most cases (default)
- **14+ days**: Comprehensive testing

### Data Quality Insights
- Reports data freshness (current/recent/stale)
- Provides confidence levels based on data recency
- Warns about potential data pipeline issues

### Error Handling
- Graceful handling of missing timestamp columns
- Validation of table format (DATABASE.SCHEMA.TABLE)
- Comprehensive error reporting and warnings

## Testing

The implementation was tested with:

1. **Unit Tests**: Mock Snowflake connection with sample data
2. **Edge Cases**: Empty tables, invalid formats, various durations
3. **Syntax Validation**: Python compilation checks
4. **Dependency Installation**: Verified python-dateutil integration

All tests passed successfully.

## Integration with Existing Workflow

This tool integrates seamlessly with the existing profiles workflow:

1. **Discovery Phase**: Use `input_table_suggestions()` to find relevant tables
2. **Date Analysis**: Use `suggest_optimal_pilot_dates()` to get optimal date ranges
3. **Pilot Testing**: Use suggested dates with `pb run --begin_time --end_time`
4. **Production**: Remove date filters for full data processing

## Benefits

- **Faster Testing**: Reduces pilot run time from hours to minutes
- **Data-Driven**: Uses actual data patterns instead of arbitrary dates  
- **Intelligent**: Automatically finds the most recent data
- **Flexible**: Provides multiple options based on testing needs
- **Reliable**: Comprehensive error handling and validation

## Files Modified

1. `src/tools/snowflake.py` - Added `suggest_optimal_pilot_dates()` method
2. `src/main.py` - Added MCP tool wrapper
3. `pyproject.toml` - Added python-dateutil dependency

## Next Steps

1. **Restart MCP Server**: Restart the server to pick up the new function
2. **Test with Real Data**: Test with actual Snowflake tables
3. **Documentation Update**: Update user documentation with new workflow
4. **Usage Training**: Train users on the new optimal date selection process