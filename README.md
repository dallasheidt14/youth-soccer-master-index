
# Youth Soccer Master Index

This repository is the foundation for a scalable scraping system that builds a unified **master team index** across GotSport, Modular11, and AthleteOne.

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
