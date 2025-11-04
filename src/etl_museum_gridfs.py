# ============================== Imports ==============================

import requests  # for making HTTP requests to The Met API
import pymongo   # for connecting to MongoDB
import os        # for filesystem operations (folders/paths)
from datetime import datetime, timezone  # for UTC timestamps
from bson.objectid import ObjectId  # for generating unique IDs in MongoDB
import json
import gridfs
from PIL import Image
import io
from sklearn.model_selection import train_test_split


# ======================== MongoDB / GridFS Setup =====================
# mongosetup
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["museum_db"]
collection_metadata = db["artwork_metadata"]
fs = gridfs.GridFS(db)


# ============================ Extraction =============================
# step 1 : get all the object ids
object_id_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"  # fixed leading space
ids_response = requests.get(object_id_url)  # call IDs endpoint
object_ids = ids_response.json()["objectIDs"]  # extract the list of object IDs from JSON

print(f"total objects found : {len(object_ids)}")


# ======================= Load (Raw/Staging) ==========================
# step : 2 download artworks and save to mongodb
count = 0            # counter to track how many images have been downloaded
max_downloads = 20   # limit to only download 20 artworks

# Loop through each object ID  # Loop through each object ID
for obj_id in object_ids:
    object_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{obj_id}"  # URL for a single artwork
    response = requests.get(object_url)  # Fetch artwork metadata

    if response.status_code != 200:  # If request failed, skip to next object
        continue

    data = response.json()  # Parse the artwork data

    # Check if valid image URL exists
    image_url = data.get("primaryImage")  # Extract the image URL field

    if image_url:  # If image URL is present
        try:
            # Download image bytes
            img_response = requests.get(image_url)  # Send GET request to fetch the image
            if img_response.status_code != 200:  # If image download fails, skip
                continue

            img_bytes = img_response.content  # Store image content (bytes)

            # Generate a unique document ID manually
            doc_id = str(ObjectId())  # Create a custom document ID for metadata record

            # Upload image to GridFS (handles storage of large files)
            file_id = fs.put(
                img_bytes,
                filename=f"{obj_id}.jpg",
                metadata={"source": "The MET Museum API"}  # Save source information inside GridFS
            )

            # Prepare a document to store metadata (related to the image)
            metadata_document = {
                "doc_id": doc_id,                          # Our custom ID
                "object_id": obj_id,                       # Museum's original object ID
                "title": data.get("title"),                # Artwork Title
                "artist": data.get("artistDisplayName"),   # Artist name
                "department": data.get("department"),      # Museum department
                "culture": data.get("culture"),            # Culture associated
                "period": data.get("period"),              # Period (e.g., 18th century)
                "object_date": data.get("objectDate"),     # Approximate creation date
                "medium": data.get("medium"),              # Material used
                "source": "The MET Museum API",            # Always note the source
                "gridfs_file_id": file_id,                 # Link to the image stored in GridFS
                "created_at": datetime.now(timezone.utc)   # Timestamp when inserted (timezone-aware)
            }

            # Insert metadata document into MongoDB collection
            collection_metadata.insert_one(metadata_document)

            print(f"Inserted Object ID {obj_id} into MongoDB with image in GridFS")  # Confirm insertion

            count += 1  # Increment counter
            if count >= max_downloads:  # If we reached the limit, stop
                break

        except Exception as e:
            print(f"Error processing Object ID {obj_id}: {e}")  # Print any errors for debugging
            continue

print("Completed Inserting Artworks into MongoDB + GridFS.")  # Final confirmation


# =========================== Transformations =========================
# Loading and Transforming and Loading back into MongoDB.



# ====================== Transform & Curate (GridFS) ==================
# transformations
import pymongo
import gridfs
from PIL import Image
import io
from sklearn.model_selection import train_test_split

# ------------------------------- Step 1: Connect to MongoDB and GridFS
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["museum_db"]
metadata_collection = db["artwork_metadata"]

fs_original = gridfs.GridFS(db)
fs_transformed = gridfs.GridFS(db, collection="fs_transformed")

# ------------------------------- Step 2: Retrieve and Transform Data
docs = list(metadata_collection.find({}))  # Retrieve all metadata documents
processed_records = []  # List to hold successfully processed records

for doc in docs:
    gridfs_file_id = doc.get("gridfs_file_id")
    if gridfs_file_id is None:
        continue  # Skip if no image associated

    try:
        # Retrieve original image
        file_obj = fs_original.get(gridfs_file_id)
        img_bytes = file_obj.read()

        # Load and transform image
        img = Image.open(io.BytesIO(img_bytes))
        img_transformed = img.resize((224, 224)).convert("RGB")

        # Save transformed image to memory
        output = io.BytesIO()
        img_transformed.save(output, format="JPEG")
        transformed_bytes = output.getvalue()
        output.close()

        # Upload transformed image to new GridFS collection
        transformed_file_id = fs_transformed.put(
            transformed_bytes,
            filename=f"{doc.get('object_id', 'unknown')}_transformed.jpg",
            metadata={"transformed": True}
        )

        # Update document with transformed image ID
        doc["transformed_gridfs_file_id"] = transformed_file_id
        processed_records.append(doc)

    except Exception as e:
        print(f"Error processing document with object_id {doc.get('object_id')}: {e}")
        continue

print(f"Transformed {len(processed_records)} images.")


# ---------------- Step 3: Split Data into Train/Validation/Test Sets
train_val, test = train_test_split(processed_records, test_size=0.20, random_state=42)
train, val = train_test_split(train_val, test_size=0.20, random_state=42)


# ---------------- Step 4: Update Metadata with Split Information
for record in train:
    metadata_collection.update_one({"_id": record["_id"]}, {"$set": {"split": "train"}})

for record in val:
    metadata_collection.update_one({"_id": record["_id"]}, {"$set": {"split": "validation"}})

for record in test:
    metadata_collection.update_one({"_id": record["_id"]}, {"$set": {"split": "test"}})

print("Data splitting completed! Metadata updated with train, validation, and test splits.")
