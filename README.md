# SemiChainETL

![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg) ![Python](https://img.shields.io/badge/Python-3.13-blue.svg) ![Status](https://img.shields.io/badge/Status-In%20Progress-yellow.svg)

## Overview

`SemiChainETL` is an Extract, Transform, Load (ETL) pipeline built with PySpark to process and clean semiconductor supply chain data sourced from Georgetown CSET’s [ETO Chip Explorer](https://github.com/georgetown-cset/eto-chip-explorer). It transforms five raw CSV datasets—inputs, providers, provision relationships, production sequences, and stages—into a consistent format, preparing them for dependency analysis and visualizations (e.g., choropleth maps, Sankey diagrams). The initial phase focuses on robust data cleaning to ensure data integrity, addressing issues like outlier market shares and redundant columns.

### Objectives
- **Fetch Raw Data:** Automate dataset downloads from a public GitHub repo.
- **Clean Data:** Resolve inconsistencies and outliers in raw CSVs.
- **Prepare for Analysis:** Set up for feature engineering (e.g., dependency scores).

## Project Structure


## Data Source
- **Origin:** [Georgetown CSET ETO Chip Explorer](https://github.com/georgetown-cset/eto-chip-explorer/tree/main/data)
- **Files:**
  - `inputs.csv`: Semiconductor inputs (e.g., CPUs, GPUs).
  - `providers.csv`: Countries and firms supplying inputs.
  - `provision.csv`: Market share allocations across providers.
  - `sequence.csv`: Input relationships in production sequences.
  - `stages.csv`: Manufacturing stages (e.g., Design, Fabrication).

## Data Cleaning Process

The pipeline processes these CSVs using PySpark 3.x and Python 3.13. Below is the cleaning journey as of April 06, 2025.

### 1. Data Fetching
- **Script:** `fetchdata.py`
- **Action:** Downloads raw CSVs from CSET’s GitHub repo using `requests`.
- **Output:** Populates `raw_data/` directory with the five files.

### 2. Initial Data Loading
- **Script:** `main.py`
- **Action:** Loaded CSVs with multi-line support and quote escaping.
- **Example:** Parsed `inputs_df` with fields like `input_name`, `description`, `market_share_chart_global_market_size_info`.

### 3. Pre-Filter Examination
- **Goal:** Assess `provision_df` quality for market share allocations.
- **Findings:**
  - Total unique `provided_id`s: 67.
  - Outliers (`total_share` > 110): 4 (5.97%)—`N30`, `N21`, `N22`, `N20`, all at 200.0.
  - NULL sums: 24 (35.82%)—firm-level entries (e.g., Intel).
  - Minor excesses: e.g., `N10` at 101.0, `N71` at 104.0.
- **Raw Data Insight:**
  - `N20`: NLD (100.0) + ASML (100.0) = 200.0.
  - `N21`: NLD (57.0) + JPN (43.0) + ASML (57.0) + Nikon (43.0) = 200.0.
  - **Diagnosis:** Double-counting due to overlapping country (e.g., NLD) and firm (e.g., ASML) shares.

**Log Excerpt:**


### 4. Data Cleaning Steps
- **Remove Redundant Columns:**
  - Dropped all-NULL columns (e.g., `market_share_chart_caption` in `inputs_df`) and constant columns (e.g., `negligible_market_share` in `provision_df`).
- **Text Formatting:**
  - Converted `share_provided` from "61%" to 61.0 (`provision_df`).
  - Parsed `market_size` and `year` (e.g., "$56.2 billion (2019)" → 56200.0, 2019) in `inputs_df` and `stages_df`.
  - Cleaned `description` fields—removed newlines, trailing punctuation (e.g., `"nodes,"` → `"nodes"`).
- **Enhance Providers:**
  - Filled `country` NULLs with `provider_name` for country-type providers (e.g., "USA" → "USA").

### 5. Outlier Resolution
- **Issue:** Market shares summed to 200.0 due to country-firm overlaps.
- **Fix:** Filtered `provision_df` to `provider_type = "country"`, dropping firm-level rows.
- **Result:**
  - Pre-filter: 67 IDs, 4 outliers at 200.0.
  - Post-filter: 59 IDs, 0 outliers > 110, sums ≤ 100 (e.g., `N20`: 100.0).
  - Minor excesses (e.g., `N10`: 101.0) remain—pending normalization.

**Log Excerpt:**


## Current Status
- **DataFrames:**
  - `inputs_df`: Cleaned inputs with parsed market sizes and years.
  - `providers_df`: Providers with enhanced country data.
  - `provision_df`: Filtered to country-level shares, sums ≤ 100 (minor excesses pending).
  - `sequence_df`: Production sequences, structurally intact.
  - `stages_df`: Stages with cleaned descriptions.
- **Achievements:**
  - Automated data fetching with `fetchdata.py`.
  - Resolved major outliers (200.0 → 100.0) via filtering.
- **Next Steps:**
  - Normalize minor share excesses (e.g., 101.0 → 100.0).
  - Feature engineering: Dependency scores, market size aggregates.
  - Visualizations: Choropleth maps, Sankey diagrams.

## How to Run
1. **Prerequisites:**
   - Python 3.13
   - PySpark 3.x
   - OpenJDK 11
2. **Setup:**
   ```bash
   git clone https://github.com/JinHuang0101/SemiChainETL.git 
   cd SemiChainETL
   python3 -m venv chipsupply
   source chipsupply/bin/activate
   pip install pyspark requests
3. **Fetch Data:**
   python3 fetchdata.py
4. **Execute ETL:**
   python3 main.py > output.log 2>&1
5. **Output:** Review output.log for pre- and post-filter validation and cleaned DataFrames.

## Future Enhancements
- Normalize minor share excesses in provision_df.
- Calculate dependency scores (e.g., max share / total share per provided_id).
- Export to Parquet for visualization tools (e.g., Tableau, Plotly).
- Add unit tests for data cleaning steps.

## Acknowledgments
Data Source: Georgetown CSET’s ETO Chip Explorer.
