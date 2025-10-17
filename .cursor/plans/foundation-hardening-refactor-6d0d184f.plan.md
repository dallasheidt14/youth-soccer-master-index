<!-- 6d0d184f-f11c-4b04-87a3-ea3ac0359ae5 7f248ff5-aba1-4e39-b45d-39ed310fc7ea -->
# Game History Scraper Implementation

## Overview

Build a modular game history scraping system starting with GotSport provider for AZ state, both genders, U10 age group. System supports incremental updates, checkpoints, activity filtering, and dual outputs (games + club lookup).

## Phase 1: Core Infrastructure

### 1. Provider Base Interface

**File**: `src/scraper/providers/game_provider_base.py` (NEW)

Create abstract base class:

```python
from abc import ABC, abstractmethod
from datetime import date
from typing import Iterable, Dict, Any, Optional

class GameHistoryProvider(ABC):
    @abstractmethod
    def iter_teams(self, state:str, gender:str, age_group:str) -> Iterable[Dict[str, Any]]:
        """Yield team dicts with team_id_source, team_id_master, team_name, etc."""
        
    @abstractmethod
    def fetch_team_games_since(self, team:Dict[str,Any], since:Optional[date]) -> Iterable[Dict[str,Any]]:
        """Yield game dicts newest->oldest, filtered by since date if provided."""
```

### 2. GotSport Provider Implementation

**File**: `src/scraper/providers/gotsport_games.py` (NEW)

Implement provider using confirmed working API:

- Base URL: `https://system.gotsport.com/api/v1/teams/{team_id}/matches?past=true`
- Headers: User-Agent, Accept, Origin, Referer (rankings.gotsport.com)
- Delay: 1.5-2.5s random between requests
- Load teams from slice CSVs: `data/master/slices/{state}_{gender}_{age_group}_master.csv`
- Parse API response for: matchTime, homeTeam, awayTeam, scores, venue
- Apply 12-month filter during scraping: skip games older than 365 days (single source of truth for retention policy)
- Note: Update corresponding tests and provider/orchestrator implementation to reflect the single source of truth for the 12-month retention policy
- Return normalized dicts with: provider, team_id_source, team_id_master, team_name, opponent_name, game_date, goals_for, goals_against, venue, source_url

### 3. Provider Registry

**File**: `src/scraper/providers/__init__.py` (UPDATE)

Add game providers:

```python
from .gotsport_games import GotSportGameProvider
GAME_PROVIDERS = {"gotsport": GotSportGameProvider}
```

## Phase 2: State Management

### 4. Checkpoint System

**File**: `src/scraper/utils/game_state.py` (NEW)

Implement checkpoint management:

- Storage location: `data/game_history/state/gotsport/{state}_{gender}_{age_group}.json`
- Structure: `{last_build_id, completed_teams[], per_team: {team_id: {last_scraped_game_date, last_seen_active_date}}}`
- Functions:
  - `load_checkpoint(provider, state, gender, age_group)` -> dict
  - `save_checkpoint(provider, state, gender, age_group, data)` -> None
  - `mark_team_complete(checkpoint, team_id, last_game_date)` -> dict
  - `get_teams_to_scrape(slice_df, checkpoint)` -> filtered DataFrame

### 5. Master Slice Generator

**File**: `scripts/generate_master_slices.py` (NEW)

Create pre-filtered slice CSVs from master index:

- Load `data/master/master_team_index_migrated_20251014_1717.csv`
- Filter by state, gender, age_group combinations
- Include columns: team_id (as team_id_master), provider_team_id (as team_id_source), team_name, club_name, state, gender, age_group
- Save to: `data/master/slices/{state}_{gender}_{age_group}_master.csv`
- Log slice creation with team counts

## Phase 3: Orchestrator

### 6. Main Build Script

**File**: `src/scraper/build_game_history.py` (NEW)

Implement orchestrator with CLI args:

- `--providers` (default: gotsport)
- `--states` (comma-separated, e.g., AZ)
- `--genders` (comma-separated, e.g., M,F)
- `--ages` (comma-separated, e.g., U10)
- `--resume` (flag, load from checkpoints)
- `--max-teams` (optional limit for testing)

Main workflow per slice:

1. Load checkpoint if --resume
2. Load master slice CSV
3. Filter teams to scrape (exclude completed from checkpoint)
4. Apply 120-day inactivity filter (skip teams inactive > 120 days)
5. Initialize provider instance
6. For each team:

   - Get last_scraped_game_date from checkpoint
   - Call `fetch_team_games_since(team, since=last_date)`
   - Collect games
   - Update checkpoint after each team

7. Write outputs (games CSV + club lookup CSV)
8. Update metrics registry
9. Final checkpoint save

### 7. Activity Filter Logic

**File**: `src/scraper/utils/activity_filter.py` (NEW)

Implement filtering functions:

- `is_team_active(last_active_date, threshold_days=120)` -> bool
- `filter_recent_games(games_list, months_back=12)` -> filtered list
- Uses datetime calculations for date comparisons

## Phase 4: Data Outputs

### 8. Game Schema Definition

**File**: `src/schema/game_history_schema.py` (NEW)

Define output schemas:

Games columns:

```python
GAMES_COLUMNS = [
    "provider", "team_id_source", "team_id_master", "team_name", "club_name",
    "opponent_name", "opponent_id", "age_group", "gender", "state",
    "game_date", "home_away", "goals_for", "goals_against", "result",
    "competition", "venue", "city", "source_url", "scraped_at"
]
```

Club lookup columns:

```python
CLUB_COLUMNS = [
    "provider", "club_id", "club_name", "state", "city", "website",
    "first_seen_at", "last_seen_at", "source_url"
]
```

Validation function using Pandera (similar to master_team_schema.py)

### 9. Output Writers

**File**: `src/scraper/utils/game_writers.py` (NEW)

Implement output functions:

- `write_games_csv(games_df, provider, state, gender, age_group, build_id)` -> Path
  - Output: `data/games/{build_id}/games_{provider}_{state}_{gender}_{age_group}.csv`
  - Use atomic write from `src/io/safe_write.py`

- `write_club_lookup_csv(games_df, provider, state, gender, age_group, build_id)` -> Path
  - Extract unique clubs from games with deduplication logic:
    - Primary deduplication key: (provider, club_id) when club_id is not null
    - Fallback deduplication key: (provider, normalized_club_name) when club_id is null
    - Normalize club_name: trim whitespace and convert to lowercase before comparison
  - Track first_seen_at = MIN(game_date) and last_seen_at = MAX(game_date) across all games matching the deduplication key
  - Validation rules:
    - Drop or flag rows where both club_id and club_name are null
    - Log warning/metrics for inconsistent club_id/club_name pairs before writing
  - Output: `data/games/{build_id}/club_lookup_{provider}_{state}_{gender}_{age_group}.csv`

## Phase 5: Registry Integration

### 10. Metrics Extension

**File**: `src/utils/metrics_snapshot.py` (UPDATE)

Add game history metrics support:

- New metric type: "games_build"
- Fields: provider, build_id, slices[], started_at, finished_at
- Per-slice: {key, teams_processed, games_scraped, new_games, skipped_inactive}
- Write to `data/metrics/games_build_{build_id}.json`

### 11. History Registry Extension

**File**: `src/registry/history_registry.py` (UPDATE)

Add game build tracking:

- New entry type for game scrapes
- Track: provider, slices, delta from previous build (new_games count)
- Compare against previous game build to compute deltas

## Phase 6: Directory Structure

Create directories:

```
data/
  games/
    {build_id}/
      games_{provider}_{state}_{gender}_{age}.csv
      club_lookup_{provider}_{state}_{gender}_{age}.csv
  master/
    slices/
      {state}_{gender}_{age}_master.csv
  game_history/
    state/
      {provider}/
        {state}_{gender}_{age}.json
  metrics/
    games_build_{build_id}.json
```

## Phase 7: Testing Strategy

### MockGameProvider Implementation

**File**: `tests/mock_providers.py` (NEW)

Create MockGameProvider that implements GameHistoryProvider interface:

- Returns fixed deterministic data for specific team IDs
- Simulates API delays and rate limiting behavior
- Supports test scenarios: empty results, errors, partial data
- Configurable response patterns for different test cases

### Unit Tests

**Files**: `tests/test_checkpoint.py`, `tests/test_filters.py`, `tests/test_writers.py` (NEW)

- Checkpoint load/save with version migration
- Activity/team filters (12-month, 120-day rules)
- Output writers (games CSV, club lookup CSV)

### Integration/E2E Tests

**File**: `tests/test_pipeline_e2e.py` (NEW)

- Run orchestrator with MockGameProvider
- Verify outputs match expected format and content
- Test metrics collection and registry updates
- Resume scenario tests (mid-team interruption, corrupted checkpoint, missing source files)

### Test Invocation

```bash
# Run all tests
pytest tests/ -v --tb=short

# Run specific test categories
pytest tests/test_checkpoint.py -v
pytest tests/test_pipeline_e2e.py -v
```

## Implementation Steps

1. Create directory structure
2. Generate master slices for AZ M/F U10
3. Implement base provider interface
4. Implement GotSport provider
5. Create checkpoint system
6. Build orchestrator script
7. Add activity filters
8. Define game schemas
9. Implement output writers
10. Extend metrics and registry
11. Create MockGameProvider for testing
12. Implement unit tests for checkpoint load/save, activity/team filters, and output writers
13. Create integration/e2e test file (tests/test_pipeline_e2e.py)
14. Add resume scenario tests (mid-team interruption, corrupted checkpoint, missing source files)
15. Test with AZ M U10 (small subset with --max-teams 5)
16. Full run: AZ M/F U10

## Success Criteria

- Master slices generated for AZ M/F U10
- GotSport provider successfully scrapes games using confirmed API
- 12-month filter enforced (games older than 365 days excluded)
- 120-day inactivity rule enforced (inactive teams skipped)
- Checkpoint system works (can resume after interruption)
- Dual outputs written: games CSV + club lookup CSV
- Metrics registry updated with games_build entry
- Both team_id_source and team_id_master present in all records
- Safe to run incrementally with --resume flag
- MockGameProvider implemented with deterministic test data
- Unit tests pass for checkpoint load/save, activity/team filters, and output writers
- Integration/e2e tests pass with MockGameProvider
- Resume scenario tests pass (mid-team interruption, corrupted checkpoint, missing source files)
- All tests can be invoked via pytest with clear test categories

### To-dos

- [ ] Create directory structure (data/games, data/master/slices, data/game_history/state)
- [ ] Create scripts/generate_master_slices.py to generate slice CSVs from master index
- [ ] Run slice generator for AZ M/F U10
- [ ] Create src/scraper/providers/game_provider_base.py with abstract interface
- [ ] Implement src/scraper/providers/gotsport_games.py with API integration
- [ ] Update src/scraper/providers/__init__.py to register game providers
- [ ] Create src/scraper/utils/game_state.py with checkpoint management
- [ ] Create src/scraper/utils/activity_filter.py with 12-month and 120-day filters
- [ ] Create src/schema/game_history_schema.py with Pandera validation
- [ ] Create src/scraper/utils/game_writers.py for CSV outputs
- [ ] Create src/scraper/build_game_history.py main orchestrator with CLI
- [ ] Update src/utils/metrics_snapshot.py to support game builds
- [ ] Update src/registry/history_registry.py to track game builds
- [ ] Test with --max-teams 5 for AZ M U10
- [ ] Full run for AZ M/F U10