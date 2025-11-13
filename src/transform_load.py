# =========================== Transformations =========================
# Loading, Cleaning, Transforming, and Loading back into MongoDB.


# ====================== Imports ======================
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


# ============================= METADATA CLEANING =============================
fields_to_clean = ['artist', 'culture', 'period', 'object_date', 'medium']

docs = list(metadata_collection.find({}))  # load all documents first

for doc in docs:
    # Clean each field
    for field in fields_to_clean:
        if not doc.get(field):
            doc[field] = "NA"

    # Update cleaned fields back into DB
    metadata_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "artist": doc["artist"],
            "culture": doc["culture"],
            "period": doc["period"],
            "object_date": doc["object_date"],
            "medium": doc["medium"],
        }}
    )


# ============================= REMOVE DUPLICATES =============================
pipeline = [
    {"$group": {"_id": "$object_id", "count": {"$sum": 1}, 
                "docs": {"$push": {"id": "$_id", "file": "$gridfs_file_id"}}}},
    {"$match": {"count": {"$gt": 1}}}
]

duplicates = list(metadata_collection.aggregate(pipeline))

print("Duplicates found:", len(duplicates))

for dup in duplicates:
    docs_list = dup["docs"]
    keep = docs_list[0]          # keep first one
    remove = docs_list[1:]       # delete duplicates

    for r in remove:
        # Delete metadata
        metadata_collection.delete_one({"_id": r["id"]})

        # Delete duplicate image file from GridFS
        try:
            fs_original.delete(r["file"])
        except:
            pass

    print(f"Removed {len(remove)} duplicates for object_id {dup['_id']}")


# ====================== IMAGE TRANSFORMATION ======================
docs = list(metadata_collection.find({}))  # Load updated documents
processed_records = []


for doc in docs:

    # SKIP if already transformed (avoid duplicates)
    if doc.get("transformed_gridfs_file_id") is not None:
        continue

    gridfs_file_id = doc.get("gridfs_file_id")
    if gridfs_file_id is None:
        continue  # no raw image file => skip

    try:
        # Retrieve original image
        file_obj = fs_original.get(gridfs_file_id)
        img_bytes = file_obj.read()

        # Load and resize image
        img = Image.open(io.BytesIO(img_bytes))
        img_transformed = img.resize((224, 224)).convert("RGB")

        # Save transformed bytes
        output = io.BytesIO()
        img_transformed.save(output, format="JPEG")
        transformed_bytes = output.getvalue()
        output.close()

        # Upload to transformed GridFS bucket
        transformed_file_id = fs_transformed.put(
            transformed_bytes,
            filename=f"{doc.get('object_id', 'unknown')}_transformed.jpg",
            metadata={"transformed": True}
        )

        # Add transformed ID to metadata
        doc["transformed_gridfs_file_id"] = transformed_file_id
        processed_records.append(doc)

        metadata_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"transformed_gridfs_file_id": transformed_file_id}}
        )

    except Exception as e:
        print(f"Error processing document with object_id {doc.get('object_id')}: {e}")
        continue

print(f"Transformed {len(processed_records)} images.")


# ====================== TRAIN / VAL / TEST SPLIT ======================
if len(processed_records) == 0:
    print("No processed records found. Skipping dataset split.")
else:
    train_val, test = train_test_split(processed_records, test_size=0.20, random_state=42)
    train, val = train_test_split(train_val, test_size=0.20, random_state=42)

    for record in train:
        metadata_collection.update_one({"_id": record["_id"]}, {"$set": {"split": "train"}})

    for record in val:
        metadata_collection.update_one({"_id": record["_id"]}, {"$set": {"split": "validation"}})

    for record in test:
        metadata_collection.update_one({"_id": record["_id"]}, {"$set": {"split": "test"}})

    print(
        "Data splitting completed! Metadata updated with "
        f"train ({len(train)}), validation ({len(val)}), and test ({len(test)}) splits."
    )
