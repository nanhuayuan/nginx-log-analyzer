# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running the Analyzer
```bash
# Main nginx log analyzer (optimized for large datasets)
python self/self_09_main_nginx_log_analyzer.py

# Aliyun CDN log analyzer
python aliyun-log-analyzer/cdn_log_analyzer.py

# N9E daily monitoring report
python n9e-daily/nginx_monitoring_optimized.py

# Log downloader
python self-log-download/nginx_downloader.py
```

### Common Python Commands
```bash
# Run with specific Python version
python3 self/self_09_main_nginx_log_analyzer.py

# Memory profiling (psutil required)
python -m memory_profiler self/self_09_main_nginx_log_analyzer.py
```

## Architecture Overview

### Core Components
1. **Main Analysis Pipeline** (`self/` directory)
   - Entry point: `self_09_main_nginx_log_analyzer.py`
   - Modular design with 8 specialized analyzers
   - Processes both 自研 (self-developed) and 底座 (base platform) nginx logs

2. **Log Processing Chain**
   - `self_00_03_log_parser.py`: Parses JSON and text format logs
   - `self_00_04_excel_processor.py`: Handles Excel output generation
   - `self_00_01_constants.py`: Configuration constants
   - `self_00_02_utils.py`: Utility functions

3. **Analysis Modules**
   - `self_01_api_analyzer.py`: API performance analysis
   - `self_02_service_analyzer.py`: Service-level analysis
   - `self_03_slow_requests_analyzer.py`: Slow request identification
   - `self_04_status_analyzer.py`: HTTP status code analysis
   - `self_05_time_dimension_analyzer.py`: Time-series analysis
   - `self_06_performance_stability_analyzer.py`: Stability metrics
   - `self_07_generate_summary_report_analyzer.py`: Comprehensive reporting

4. **Additional Systems**
   - `aliyun/cdn_log_analyzer.py`: Aliyun CDN log analysis
   - `n9e_daily/nginx_monitoring_optimized.py`: N9E monitoring GUI with modern tkinter interface
   - `self_log_download/nginx_downloader.py`: Log file downloader

### N9E Monitoring System Features
- **GUI Interface**: Modern tkinter-based interface with tabbed design
- **Flexible Time Ranges**: Supports precise time selection and multi-day analysis
- **Dynamic Data Source**: Configurable Prometheus data source ID
- **Smart Time Handling**: 
  - Single day periods output as daily data
  - Multi-day periods automatically split by natural days
- **Multiple Output Formats**: Text reports and Excel spreadsheets
- **Real-time Monitoring**: Progress tracking and log display

### Log Format Support
- **自研 (Self-developed)**: JSON format with comprehensive fields
- **底座 (Base platform)**: Text format with key-value pairs
- **Aliyun CDN**: Custom format with timestamp, IP, response time, etc.

### Key Configuration
- Default log directory: `DEFAULT_LOG_DIR` in constants
- Slow request threshold: 3 seconds
- Success status codes: ['200']
- Memory optimization: Chunked processing (100K records)

### Data Flow
1. Log files collected from `DEFAULT_LOG_DIR`
2. Parsed into unified CSV format with normalized columns
3. Multiple analysis passes generate Excel reports
4. Summary report aggregates all findings
5. Temporary files cleaned up automatically

### Performance Considerations
- Memory monitoring with psutil
- Chunked file processing to handle large logs
- Streaming CSV processing for memory efficiency
- Excel sheet pagination for large datasets

### Output Structure
Analysis generates timestamped directories with:
- `01.接口性能分析.xlsx` - API performance metrics
- `02.服务层级分析.xlsx` - Service-level statistics  
- `03_慢请求分析.xlsx` - Slow request analysis
- `04.状态码统计.xlsx` - HTTP status code distribution
- `05.时间维度分析-全部接口.xlsx` - Time-series analysis
- `06_服务稳定性.xlsx` - Stability metrics
- `08_IP来源分析.xlsx` - **NEW**: IP source analysis with security insights
- `10_请求头分析.xlsx` - **NEW**: Request header analysis (User-Agent, Referer)
- `11_请求头性能关联分析.xlsx` - **NEW**: Header-performance correlation analysis
- `12_综合报告.xlsx` - Executive summary

### New Features (v2.3)
- **Header-Performance Correlation Analysis**: Advanced analysis combining request headers with performance metrics:
  - Browser/OS/Device performance correlation with slow requests
  - Referer source impact on response times
  - Bot/crawler performance patterns and optimization suggestions
  - Statistical analysis with percentile distributions (P50, P95, P99)
  - Automated performance recommendations based on header patterns
- **Request Header Analysis**: Comprehensive analysis of request headers including:
  - User-Agent analysis with browser, OS, and device detection
  - Referer analysis with search engine and social media detection
  - Bot/crawler identification and classification
  - Security insights through header pattern analysis
- **IP Source Analysis**: Comprehensive IP behavior analysis including:
  - IP classification (internal/external/reserved)
  - Risk scoring based on request patterns
  - Geographic and temporal distribution
  - Anomaly detection for security insights
- **Memory Optimization**: Significant improvements for large dataset processing:
  - Reduced memory usage by 60-75%
  - Sampling algorithms for statistical analysis
  - Temporary file caching for large intermediate results
- **Performance Enhancements**: 
  - Optimized chunk processing
  - Incremental statistics calculation
  - Enhanced garbage collection

The system is designed for production nginx log analysis with emphasis on performance monitoring, security analysis, troubleshooting, and capacity planning.