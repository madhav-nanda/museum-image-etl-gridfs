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
if len(processed_records) == 0:
    print("No processed records found. Skipping train/validation/test split.")
else:
    train_val, test = train_test_split(processed_records, test_size=0.20, random_state=42)
    train, val = train_test_split(train_val, test_size=0.20, random_state=42)

    # ---------------- Step 4: Update Metadata with Split Information
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
