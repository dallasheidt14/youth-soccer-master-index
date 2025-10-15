# Youth Soccer Master Index Pipeline

## Overview

The Youth Soccer Master Index is a comprehensive data pipeline that scrapes, normalizes, and maintains a master database of youth soccer teams across the United States. This document describes the complete pipeline flow and architecture.

## Pipeline Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Normalization │    │   Master Index  │
│                 │    │                 │    │                 │
│ • GotSport API  │───▶│ • Text Cleanup  │───▶│ • Unified Schema│
│ • Modular11     │    │ • State Codes   │    │ • Team IDs      │
│ • AthleteOne    │    │ • Gender Norm   │    │ • Validation    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Incremental   │    │   Multi-Provider│    │   Quality &     │
│   Detection     │    │   Merging       │    │   Monitoring    │
│                 │    │                 │    │                 │
│ • Delta Track   │    │ • Conflict Res  │    │ • Schema Valid  │
│ • New Teams     │    │ • Data Complete │    │ • Metrics Snap  │
│ • Registry      │    │ • Provider Track│    │ • State Summary │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Detailed Pipeline Flow

### 1. Provider Scraping (GotSport API)

**Location**: `src/scraper/providers/gotsport_scraper.py`

- **Input**: GotSport Rankings API endpoints
- **Process**: 
  - Paginated API calls for all age groups (U10-U18)
  - Both male and female teams
  - Rate limiting with random delays (1.5-2.5s)
- **Output**: Raw team data with provider-specific fields

**Key Features**:
- ZenRows integration for dynamic content
- Retry logic with exponential backoff
- Comprehensive error handling
- Real-time progress logging

### 2. Data Normalization

**Location**: `src/normalizers/text_normalizer.py`, `src/utils/team_id_generator.py`

- **Text Normalization**:
  - Remove special characters
  - Standardize case
  - Remove common stopwords ("FC", "SC", "Academy", etc.)
  - Collapse multiple spaces

- **Team ID Generation**:
  - Deterministic SHA1 hash: `sha1("name|state|age|gender")[:12]`
  - Provider-agnostic global identity
  - Enables cross-provider merging

- **State Code Normalization**:
  - Map regional codes to standard 2-letter US codes
  - Filter out non-US territories
  - Validate against US state list

- **Gender Normalization**:
  - Standardize to "M"/"F" format
  - Handle various input formats ("Male"/"Female", "1"/"0", etc.)

### 3. USA-Only Filtering

**Location**: `src/scraper/utils/state_normalizer.py`

- **Process**:
  - Apply state code mapping
  - Remove non-US territories
  - Validate 2-letter state codes
- **Output**: Clean US-only dataset

### 4. Incremental Detection

**Location**: `src/scraper/utils/incremental_detector.py`

- **Baseline Loading**:
  - Registry-based lookup (no file globbing)
  - Fallback to file search if registry unavailable
  - Sample mode support for testing

- **Delta Detection**:
  - Compare new data against baseline
  - Identify truly new teams
  - Track changes per provider
  - Generate delta summaries

### 5. Multi-Provider Merging

**Location**: `src/utils/multi_provider_merge.py`

- **Conflict Resolution**:
  - Join on deterministic `team_id`
  - Data completeness scoring
  - Provider preference ranking
  - Merge conflict logging

- **Provider Tracking**:
  - Maintain `providers` list for each team
  - Track data source provenance
  - Enable provider-specific analytics

### 6. Schema Validation

**Location**: `src/schema/master_team_schema.py`, `src/validators/verify_master_index.py`

- **Pandera Schema**:
  - Type validation
  - Range checks (age 10-18)
  - Format validation (state codes, gender)
  - Required field validation

- **Data Quality Checks**:
  - Duplicate detection
  - Completeness scoring
  - Consistency validation
  - Error reporting

### 7. Atomic Writes with Checksums

**Location**: `src/io/safe_write.py`

- **Process**:
  - Write to temporary files (.tmp)
  - Compute MD5 checksums
  - Atomic rename operation
  - Integrity verification

- **Benefits**:
  - Prevents partial writes
  - Enables corruption detection
  - Supports rollback scenarios

### 8. Registry Updates

**Location**: `src/registry/metadata_registry.py`, `src/registry/history_registry.py`

- **Metadata Registry**:
  - Build timestamps
  - File paths and checksums
  - Data quality metrics
  - Provider information

- **History Registry**:
  - Delta tracking (added/removed/renamed)
  - Build statistics
  - Trend analysis
  - Data retention policy

### 9. Metrics and Monitoring

**Location**: `src/utils/metrics_snapshot.py`, `src/utils/state_summary_builder.py`

- **Build Metrics**:
  - Team counts by state
  - Age group distributions
  - Provider coverage
  - Data quality scores

- **State Summaries**:
  - Per-state team counts
  - Provider coverage by state
  - Age/gender breakdowns
  - Coverage analysis

### 10. Data Retention and Archival

**Location**: `src/registry/history_registry.py`

- **Retention Policy**:
  - Keep last 20 builds in active registry
  - Archive older builds to ZIP files
  - Compress with metadata
  - Automatic cleanup

## File Structure

```
src/
├── scraper/
│   ├── providers/
│   │   └── gotsport_scraper.py
│   ├── utils/
│   │   ├── file_utils.py
│   │   ├── incremental_detector.py
│   │   └── state_normalizer.py
│   └── build_master_team_index.py
├── normalizers/
│   └── text_normalizer.py
├── validators/
│   └── verify_master_index.py
├── io/
│   └── safe_write.py
├── registry/
│   ├── metadata_registry.py
│   └── history_registry.py
├── schema/
│   └── master_team_schema.py
└── utils/
    ├── team_id_generator.py
    ├── metrics_snapshot.py
    ├── state_summary_builder.py
    └── multi_provider_merge.py

data/
├── master/
│   ├── master_team_index_*.csv
│   ├── state_summaries.json
│   └── history/
│       └── history_registry.json
├── metrics/
│   └── build_*.json
├── aliases/
│   └── team_aliases.csv
└── archive/
    └── archive_*.zip
```

## Data Schema

### Master Team Index Schema

| Field | Type | Description | Validation |
|-------|------|-------------|------------|
| `team_id` | str | Deterministic hash (12 chars) | Required, unique |
| `provider_team_id` | str | Original provider ID | Optional |
| `team_name` | str | Team name | Required, 1-200 chars |
| `age_group` | str | Display format (U10-U18) | Required, regex pattern |
| `age_u` | int | Numeric age (10-18) | Required, range 10-18 |
| `gender` | str | Normalized (M/F) | Required, enum |
| `state` | str | 2-letter US code | Required, US states |
| `provider` | str | Data source | Required |
| `club_name` | str | Club/organization | Optional |
| `source_url` | str | API endpoint | Required |
| `created_at` | str | ISO timestamp | Optional |
| `providers` | list | All providers for team | Auto-generated |

## Quality Metrics

### Data Completeness Score

Calculated as weighted average of field completeness:

- `team_name`: 25%
- `team_id`: 20%
- `provider_team_id`: 15%
- `age_group`: 10%
- `age_u`: 10%
- `gender`: 10%
- `state`: 10%
- `club_name`: 5%
- `source_url`: 5%

### Validation Checks

1. **Schema Validation**: Pandera schema compliance
2. **Duplicate Detection**: Team name + state + age + gender
3. **State Validation**: US state code verification
4. **Gender Validation**: M/F normalization
5. **Age Validation**: Range 10-18, consistency checks
6. **Team ID Uniqueness**: No duplicate team_id values

## Monitoring and Alerting

### Build Metrics

- Total team count
- New teams added
- Teams removed/renamed
- States covered
- Data quality score
- Build duration
- Provider coverage

### State Coverage

- Teams per state
- Provider distribution
- Age group breakdown
- Gender distribution
- Coverage trends

### Data Quality Trends

- Completeness scores over time
- Validation failure rates
- Duplicate detection rates
- Schema compliance rates

## Error Handling

### Retry Logic

- API calls: 3 retries with exponential backoff
- File operations: Atomic writes with rollback
- Network timeouts: Configurable timeouts

### Graceful Degradation

- Partial data processing
- Fallback mechanisms
- Error logging and reporting
- Continue on non-critical failures

### Data Recovery

- Checksum verification
- Backup file retention
- Registry-based rollback
- Incremental rebuild capability

## Performance Optimization

### Caching

- Registry data caching
- State code mapping cache
- Provider preference cache

### Parallel Processing

- Multi-threaded API calls
- Batch processing
- Async file operations

### Memory Management

- Streaming data processing
- Chunked operations
- Garbage collection optimization

## Security Considerations

### API Security

- Rate limiting compliance
- Request header rotation
- Error message sanitization

### Data Privacy

- No PII collection
- Team name anonymization options
- Secure data storage

### Access Control

- File permission management
- Registry access controls
- Audit logging

## Troubleshooting

### Common Issues

1. **API Rate Limiting**: Increase delays, check headers
2. **Schema Validation Failures**: Check data normalization
3. **Duplicate Team IDs**: Verify team_id generation
4. **State Code Errors**: Update state mapping
5. **Memory Issues**: Reduce batch sizes

### Debug Tools

- Verbose logging mode
- Data validation reports
- Schema compliance checks
- Performance profiling

### Recovery Procedures

- Registry rollback
- Incremental rebuild
- Data integrity checks
- Backup restoration

## Future Enhancements

### Planned Features

- Additional data providers (Modular11, AthleteOne)
- Real-time data updates
- Advanced analytics dashboard
- Machine learning integration
- API endpoint for data access

### Scalability Improvements

- Distributed processing
- Database backend
- Cloud storage integration
- Microservices architecture

## Contributing

### Development Setup

1. Clone repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run tests: `pytest`
4. Run sample build: `python src/scraper/build_master_team_index.py --sample`

### Code Standards

- Type hints required
- Comprehensive docstrings
- Error handling mandatory
- Logging with emojis
- Unit tests for new features

### Testing

- Unit tests for all utilities
- Integration tests for pipeline
- End-to-end tests with sample data
- Performance benchmarks
- Security scans

---

*Last updated: October 14, 2025*
*Version: 1.0.0*
