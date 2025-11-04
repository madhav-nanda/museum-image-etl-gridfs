"""
Museum Image ETL (Extract & Load): The MET Museum → MongoDB (Staging)

Overview
--------
1. Fetch object IDs from The MET Museum Collection API.
2. Retrieve metadata + primary image for each artwork.
3. Save images locally and insert metadata into MongoDB.

Purpose
-------
Acts as the raw ingestion layer (staging zone) before transformation.
Images are stored on disk; metadata goes to MongoDB.

Requirements
------------
- Local MongoDB running at mongodb://localhost:27017/
- Python packages: requests, pymongo, gridfs
"""

# ------------------ Imports ------------------
import requests
import pymongo
import os
from datetime import datetime, timezone
from bson.objectid import ObjectId
import gridfs

# ------------------ MongoDB Setup ------------------
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["museum_db"]
collection = db["artworks"]

# ------------------ Filesystem Prep ------------------
os.makedirs("images1", exist_ok=True)

# ------------------ Extract: Object IDs ------------------
object_id_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
ids_response = requests.get(object_id_url)
object_ids = ids_response.json()["objectIDs"]
print(f"Total objects found: {len(object_ids)}")

# ------------------ Extract & Load ------------------
count = 0
max_downloads = 20

for obj_id in object_ids:
    object_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"
    response = requests.get(object_url)
    if response.status_code != 200:
        continue

    data = response.json()
    image_url = data.get("primaryImage")
    if not image_url:
        continue

    try:
        # Download image and store locally
        img_data = requests.get(image_url).content
        image_path = f"images1/{obj_id}.jpg"
        with open(image_path, "wb") as handler:
            handler.write(img_data)

        # Prepare metadata document for MongoDB
        document = {
            "doc_id": str(ObjectId()),
            "object_id": obj_id,
            "title": data.get("title"),
            "artist": data.get("artistDisplayName"),
            "department": data.get("department"),
            "culture": data.get("culture"),
            "period": data.get("period"),
            "object_date": data.get("objectDate"),
            "medium": data.get("medium"),
            "image_path": image_path,
            "image_url": image_url,
            "source": "The MET Museum API",
            "created_at": datetime.now(timezone.utc)
        }

        collection.insert_one(document)
        print(f"Inserted into MongoDB: Object ID {obj_id}")

        count += 1
        if count >= max_downloads:
            break

    except Exception as e:
        print(f"Error processing Object ID {obj_id}: {e}")
        continue

print("Completed inserting artworks into MongoDB.")
