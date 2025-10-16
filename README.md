
# Youth Soccer Master Index

This repository is the foundation for a scalable scraping system that builds a unified **master team index** across GotSport, Modular11, and AthleteOne.

## 🚀 Running Scripts

This project uses proper Python package structure. Scripts should be run as modules to ensure correct import resolution:

### Running Registry Scripts
```bash
# Rebuild the build registry
python -m src.registry.build_registry --refresh

# Show current registry
python -m src.registry.build_registry --show

# Test specific slice
python -m src.registry.build_registry --test-slice AZ_M_U10
```

### Running Utility Scripts
```bash
# Rebuild registry from existing folders
python -m scripts.rebuild_registry

# Verify master index
python -m src.validators.verify_master_index
```

### Alternative: Install in Development Mode
```bash
pip install -e .
```

This allows running scripts directly without the `-m` flag.

## 📁 Project Structure

```
youth-soccer-master-index/
├── README.md
├── requirements.txt
├── .github/workflows/weekly.yml
├── data/
│   ├── master/      # per-state team outputs
│   ├── logs/        # scrape logs
│   └── temp/        # temporary cache
└── src/
    └── scraper/
        ├── build_master_team_index.py  # orchestrator entrypoint
        ├── providers/                  # scrapers for each platform
        ├── utils/                      # shared helpers
        └── config/                     # static configs
```

## 🧠 Next Steps
- Implement `GotSport`, `Modular11`, and `AthleteOne` scrapers under `src/scraper/providers/`.
- Use `src/scraper/utils/zenrows_client.py` for ZenRows integration.
- Schedule weekly updates via GitHub Actions.
