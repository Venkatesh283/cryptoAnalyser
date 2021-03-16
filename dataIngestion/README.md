# Data Ingestion Pipeline

## Update Config

- Copy settings_template.py to settings.py
- Update settings.py with required credentials

## Background scripts to load and update data
- Load Json/CSV data to database

```
python ingest_data.py -ff json -inp fetchTickers.json
```

- Get Historical Data

```
python get_historical_data.py
```

