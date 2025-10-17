# Registry API Documentation

## Per-Slice Build Tracking (v2.1.0)

Starting in v2.1.0, the build registry tracks builds independently for each slice (state_gender_age combination).

### Registry Structure
```json
{
  "__registry_version__": "v2.1.0",
  "latest_build": "build_20251016_1325",  // Global fallback (deprecated)
  "AZ_M_U10": {
    "latest_build": "build_20251015_1412",
    "last_updated": "2025-10-16T21:20:01+00:00"
  }
}
```

### Usage
```python
from src.registry.registry import get_registry

registry = get_registry()

# Get latest build for a specific slice
build = registry.get_latest_build("AZ_M_U10")

# Update build for a slice (done automatically by scraper)
registry.update_build_registry("AZ_M_U10", "build_20251017_0930")

# Get global fallback (deprecated)
global_build = registry.get_global_build()
```

### Benefits
- Partial scrapes can be normalized without errors
- Each slice tracks its own build independently
- Normalizer can mix slices from different builds
- Dashboard shows per-slice build status

## Registry Methods

### Build Registry

#### `get_latest_build(slice_key: str) -> Optional[str]`
Get the latest build directory for a slice. Falls back to global build if slice not found.

#### `update_build_registry(slice_key: str, build_dir: str) -> None`
Update the build registry with a new build directory.

#### `get_global_build() -> Optional[str]`
Get the global latest build (deprecated, for backward compatibility only).

#### `list_all_builds() -> Dict[str, Any]`
Get all build registry entries.

### Metadata Registry

#### `add_metadata_entry(entry: Dict[str, Any]) -> None`
Add a new metadata entry to the registry with deduplication.

#### `get_latest_metadata() -> Optional[Dict[str, Any]]`
Get the latest metadata entry.

#### `get_latest_master_index_path() -> Optional[Path]`
Get the path to the latest master team index file.

### History Registry

#### `add_history_entry(build_info: Dict[str, Any], deltas: Dict[str, int]) -> None`
Add a new history entry to the registry.

#### `add_games_build_entry(build_id: str, providers: List[str], slices: List[Dict[str, str]], results: List[Dict[str, Any]]) -> None`
Add a games build entry to the history registry.

#### `get_build_history(limit: int = 10) -> List[Dict[str, Any]]`
Get recent build history entries.

### Utility Methods

#### `get_registry_stats() -> Dict[str, Any]`
Get comprehensive registry statistics for monitoring and alerts.

#### `get_registry_version() -> str`
Get the current registry version.

#### `check_version_compatibility() -> Dict[str, Any]`
Check if registry version is compatible with current code.

#### `migrate_legacy_registries() -> None`
Migrate data from old registry locations to unified location.

## Migration

To migrate existing builds to per-slice tracking:

```bash
python scripts/migrate_per_slice_registry.py
```

This will scan all existing build directories and register each slice with its corresponding build.