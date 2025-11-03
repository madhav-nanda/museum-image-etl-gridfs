# museum-image-etl-gridfs
End-to-end ETL pipeline that ingests artwork images + metadata from The MET Museum API, stores raw assets in MongoDB GridFS, standardizes images (224×224 RGB), enriches metadata, and writes curated datasets back with train/val/test splits. Built with Python, Requests, Pillow, scikit-learn, and PyMongo.
