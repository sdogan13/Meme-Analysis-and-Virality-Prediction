# Early Meme Virality Prediction

Notebook-based analysis for a time-series Reddit meme dataset. It covers dataset inspection, virality target construction, multimodal feature engineering, and modelling experiments (classical baselines, deep baselines, and a dual-attention model).

## Contents

- `Reddit_Virality_Data.ipynb`
- `Media_Download.ipynb`
- `data_collection.py`
- `LLM Feature Ext.py`
- [Quickstart](#quickstart)
- [Expected data layout](#expected-data-layout)
- [Database overview](#database-overview)
- [Notebook structure](#notebook-structure)
- [Outputs](#outputs)
- [Reproducibility](#reproducibility)
- [Data usage and ethics](#data-usage-and-ethics)
- [Citation](#citation)
## `Reddit_Virality_Data.ipynb`

The main research notebook, organised to align with thesis chapters and experiments.

## Quickstart

```bash
# 1) Create an environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip

# 2) Install common dependencies
pip install pandas numpy matplotlib scipy scikit-learn tqdm tabulate joblib xgboost shap
pip install torch torchvision transformers pillow

# 3) Launch Jupyter
pip install jupyter
jupyter lab
```

Open `Reddit_Virality_Data.ipynb` and run the sections you need.

## Expected data layout

Set `DATA_DIR = ~/Reddit_Virality_Data/` (or your preferred path). The notebook expects the following structure:

```text
~/Reddit_Virality_Data/
  virality.db                 # raw collected dataset (SQLite)
  media_files/                # downloaded media from URLs stored in virality.db
    <post_id>.jpg|png|jpeg|gif|webp|mp4|webm
  contextual_features.json    # LLM-derived contextual features built from titles (+ media when available)
```

- `virality.db` and `contextual_features.json` are provided via OSF (see **Data files (hosted on OSF)**).
- `media_files/` can be created by running `Media_Download.ipynb` (see **Downloading media**). This is only required for deep baseline comparisons.

To use a different folder, edit `DATA_DIR` (search for `DATA_DIR =` in the notebook).


## Downloading media (required for deep baselines)

Deep visual baselines (e.g., **CLIP** embeddings and **InceptionV3** features) require the original media files (images/videos/GIFs) to be present under `media_files/`.

To populate `media_files/`, run the companion notebook:

- `Media_Download.ipynb`  
  Downloads media using the URLs stored in `virality.db` and saves files into the expected `media_files/` directory.

Once `media_files/` is populated, you can run the deep-baseline cells in `Reddit_Virality_Data.ipynb` without path errors or missing-file skips.



## Data collection and LLM feature extraction scripts

This repository also includes two helper scripts used during dataset construction:

- `data_collection.py`  
  Collects Reddit submissions and engagement snapshots and writes them into `virality.db`.  
  This is **not required** to run the modelling experiments unless you want to rebuild the dataset from scratch.

- `LLM Feature Ext.py`  
  Extracts LLM-derived contextual features and writes them to `contextual_features.json`.  
  The script typically uses submission titles and (when available) local media from `media_files/`.

These scripts may require API credentials (e.g., Reddit API / LLM provider). Check the configuration variables at the top of each script before running.

## Database overview

`virality.db` is a SQLite database. Typical tables include:

- `posts`  
  One row per Reddit submission.
- `time_series`  
  Engagement snapshots per submission over time (e.g., score, comments, crossposts, upvote ratio).
- `subreddits`  
  Subreddit metadata (e.g., subscriber counts).

The notebook provides an `inspect_database()` helper to print schemas and sample rows.

## Notebook structure

Top-level sections in the notebook:

- Collected Data  
  Sanity checks and dataset counts
- Defining Virality (Chapter 3)  
  Target construction (percentile rules + clustering) and archetype labelling
- A Multimodal Feature Framework (Chapter 4)  
  Feature engineering (visual/textual/contextual/temporal), baselines, and SHAP analysis
- A Time-Window Analysis (Chapter 5)  
  Feature extraction and modelling per observation window (30–420 minutes)
- A Dual Attention Architecture (Chapter 6)  
  Preprocessing, model definition, training, and error analysis
## Chapter dependency chain

The notebook follows a cumulative pipeline. Each chapter reuses the outputs of the previous ones:

- **Chapter 3 (Defining Virality)** produces the **targets/labels** used throughout the notebook.
- **Chapter 4 (Multimodal Feature Framework)** uses the Chapter 3 targets to engineer **static multimodal features** and train baseline models.
- **Chapter 5 (Time-Window Analysis)** uses the Chapter 3 targets plus Chapter 4 static features to build **time-window datasets** and **dynamic early signals** across observation windows (30–420 minutes).
- **Chapter 6 (Dual Attention Architecture)** uses the Chapter 3 targets, Chapter 4 static features, and Chapter 5 time-window/dynamic features to train the **dual-attention model** and run diagnostic/error analysis.

If you are running the notebook end-to-end for the first time, run Chapters 3 → 6 in order so that dependencies are created before they are consumed.

## Outputs
Several sections write artefacts under `DATA_DIR`, typically into chapter folders such as:

```text
~/Reddit_Virality_Data/
  Chapter_3/
  Chapter_4/
  Chapter_5/
  Chapter_6/
```

If you use a different folder layout, update directory variables before running the relevant cells.

## Reproducibility

- This is a research notebook (not a packaged library). Paths and run order may need small adjustments depending on your machine.
- If you re-run experiments, keep track of random seeds, hardware (CPU/GPU), and package versions to make results comparable.

## Data usage and ethics

This work uses Reddit content and engagement logs. Follow Reddit’s terms and your institution’s ethics requirements. If you redistribute any part of the database, remove/anonymise user identifiers and review redistribution rules for any media files.

## Citation

If you use this notebook or derived artefacts in academic work, cite the thesis:

```bibtex
@phdthesis{dogan_reddit_virality,
  title  = {Adaptive Sequential Reasoning: A Dual-Attention Architecture for Multimodal Virality Prediction},
  author = {Dogan, Sedat},
  school = {University of Hull},
  year   = {2025}
}
```
