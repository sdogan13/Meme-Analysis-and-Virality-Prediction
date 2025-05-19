# Meme Analysis and Virality Prediction

Predict which Reddit memes will go viral within minutes using multimodal features.

---

## Repository Contents

* \`\`
  Tracks Reddit meme posts across 36 subreddits (8 languages).

  * Rotates PRAW API clients, monitors rate limits
  * Polls categories (`new`, `hot`, `rising`, `top`) at dynamic intervals (5–720 min) based on post age
  * Stores into WAL-mode SQLite DB (`virality.db`) with tables: `posts`, `time_series`, `subreddits`
  * Logs progress and errors concisely, rotates daily logs
  * Archives posts > 30 days old; keeps 7 days of backups
  * Exports active‑post snapshots to CSV every 12 h under `exports/`

* \`\`
  Trains and evaluates baselines (RF & LR) and a 60 min RF ablation study.

  1. **Configuration**

     * Paths:

       * `BASE_PATH` → your project root
       * Input: `processed_data/`
       * Outputs:

         * `evaluation_results/`
         * `evaluation_results/feature_importances/`
         * `evaluation_results/shap_results/`
     * Time windows: 30, 60, 120, 180, 240, 300, 360, 420 min
     * Modalities grouped by column prefixes: `contextual_`, `temporal_`, `visual_`, `network_`, `textual_`
  2. **Data loading**

     * Reads Parquet splits: `train_*.parquet`, `val_*.parquet`, `test_*.parquet`
     * Skips missing windows gracefully
  3. **Feature prep**

     * Drops ID, snapshot, target columns
     * Fills NaNs, scales numeric with `StandardScaler`
     * Dynamically assigns features to modality groups
  4. **Scenarios**

     * **Baseline**: all features
     * **Ablation (60 min only)**: exclude one modality at a time
  5. **Modeling**

     * **Random Forest** (250 trees, class-balanced, depth=20, leaf=10)

       * Saves `feature_importances_` CSVs
       * Computes SHAP values (bar & beeswarm plots saved as PNGs)
     * **Logistic Regression** (liblinear, balanced)

       * Saves coefficient CSVs
  6. **Evaluation**

     * Metrics on valid & test: PR AUC, ROC AUC, F1, precision, recall, accuracy
     * Aggregates into `evaluation_results/RF_LR_baseline_all_windows_RF_ablation_60min.csv`
     * Prints final table in notebook

* \`\`
  Parquet datasets for each window (30–420 min):

  ```
  train_<w>min.parquet
  val_<w>min.parquet
  test_<w>min.parquet
  ```

  Includes normalized engagement, author/network stats, and LLM-derived features.
  All records include an `id` key that matches entries in `raw_data.db` and `LLM_features.json`.

* \`\`
  Static content features extracted via an LLM for each post ID (captions, sentiment, cultural refs).
  File names in this JSON end with the media extension (e.g., `1384y29.png`), which serves as the `id` matching raw and processed data.

* \`\`
  The initial SQLite database (`virality.db`) containing raw `posts` & `time_series` tables from `data_collection.py`.

* \`\`
  Folder of raw media assets (images, videos, GIFs, audio). Size \~28 GB. Best options to include:

  1. **External storage with DVC**: version dataset with DVC and push media to an S3/GCS remote. Maintains pointers in Git and offloads large binaries:

     ```bash
     dvc init
     dvc remote add -d storage s3://mybucket/media_files
     dvc add media_files/
     git add media_files.dvc .gitignore
     git commit -m "Track media_files with DVC"
     dvc push
     ```
  2. **Cloud bucket + links**: upload `media_files/` to AWS S3, GCS, or Azure Blob and load assets via URLs in code.
  3. **Git LFS splitting** (if you prefer Git LFS):

     * Split into subfolders ≤ 2 GB each (`images_a/`, `images_b/`, etc.)
     * Track extensions in `.gitattributes`:

       ```bash
       git lfs track "media_files/**/*.png" 
       git lfs track "media_files/**/*.mp4" 
       ```
     * Commit and push each batch separately.
