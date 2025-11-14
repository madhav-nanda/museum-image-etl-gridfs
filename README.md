# 🖼️ Museum Image ETL Pipeline (MongoDB + GridFS)

A production-style **end-to-end ETL pipeline** that ingests artwork data from **The MET Museum API**, stores raw images in **MongoDB GridFS**, cleans & transforms metadata, generates resized ML-ready images, removes duplicates, and prepares a complete **train/validation/test** dataset.

---

## 🚀 What This Project Does

- **Extracts** metadata + high-resolution images from The MET API  
- **Loads** images into MongoDB **GridFS** (`fs.files`, `fs.chunks`)  
- **Cleans & standardizes** metadata (missing values → “NA”)  
- **Removes duplicate records** and duplicate GridFS images  
- **Transforms images** → resized (256×256), RGB, normalized  
- **Saves processed images** into `fs_transformed.files` + `.chunks`  
- **Splits dataset** into **80% train / 10% val / 10% test**  
- Produces **clean sample outputs** for recruiters to verify  
- All outputs are stored under `data_outputs/` with screenshots  

---

## 📁 Project Layout

src/
├── ingestion.py # Extract + load raw images & metadata
├── transform_load.py # Clean metadata, resize images, dedupe, split
└── etl_museum_gridfs.py # Full ETL orchestrator

data_outputs/
├── fs_chunks_view.png
├── fs_files_view.png
├── fs_transformed_chunks_view.png
├── fs_transformed_files_view.png
└── metadata_sample.json

**🛠 Tech Stack**

Languages & Tools:
Python
Git & GitHub

Libraries:
requests – API extraction
pymongo – MongoDB operations
gridfs – image storage in chunks
Pillow (PIL) – image resizing & RGB conversion
sklearn – train/val/test split
io – in-memory image processing

Databases:
MongoDB
GridFS (fs.files, fs.chunks, fs_transformed.*)

APIs:
The MET Museum Public API

Concepts Used:
ETL pipeline (Extract → Transform → Load)
Image ingestion & binary handling
Metadata cleaning
Duplicate detection & deletion
Dataset preparation for ML (80/10/10 split)
Professional project structuring
