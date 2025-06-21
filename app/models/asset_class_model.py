from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class AssetClassModel:
    """MongoDB model class for handling asset class operations and data management"""
    
    def __init__(self):
        """Initialize the AssetClassModel with the 'asset_classes' collection"""
        self.collection = db["asset_classes"]

    def create_asset_class(self, name):
        """Create a new asset class
        
        Args:
            name (str): Name of the asset class
            
        Returns:
            str|None: Inserted asset class ID as string, or None on error
        """
        try:
            # Check if asset class with same name already exists
            existing = self.collection.find_one({"name": name})
            if existing:
                logger.error(f"Asset class with name '{name}' already exists")
                return None
            
            asset_class_data = {
                "name": name
            }
            
            asset_class_data = add_timestamps(asset_class_data)
            result = self.collection.insert_one(asset_class_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating asset class: {e}")
            return None

    def get_asset_class(self, asset_class_id):
        """Get an asset class by its ID
        
        Args:
            asset_class_id (str): ID of the asset class to retrieve
            
        Returns:
            dict|None: Asset class data as dictionary, or None if not found or error
        """
        try:
            asset_class = self.collection.find_one({"_id": ObjectId(asset_class_id)})
            if asset_class:
                asset_class["_id"] = str(asset_class["_id"])
            return asset_class
        except PyMongoError as e:
            logger.error(f"Database error while getting asset class: {e}")
            return None

    def get_all_asset_classes(self):
        """Get all asset classes
        
        Returns:
            list: List of asset classes as dictionaries, or empty list on error
        """
        try:
            asset_classes = self.collection.find().sort("name", 1)  # Sort by name alphabetically
            asset_class_list = []
            for asset_class in asset_classes:
                asset_class["_id"] = str(asset_class["_id"])
                asset_class_list.append(asset_class)
            return asset_class_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching all asset classes: {e}")
            return []

    def get_asset_class_by_name(self, name):
        """Get an asset class by its name
        
        Args:
            name (str): Name of the asset class to retrieve
            
        Returns:
            dict|None: Asset class data as dictionary, or None if not found or error
        """
        try:
            asset_class = self.collection.find_one({"name": name})
            if asset_class:
                asset_class["_id"] = str(asset_class["_id"])
            return asset_class
        except PyMongoError as e:
            logger.error(f"Database error while getting asset class by name: {e}")
            return None

    def update_asset_class(self, asset_class_id, name):
        """Update an asset class's name
        
        Args:
            asset_class_id (str): ID of the asset class to update
            name (str): New name for the asset class
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if another asset class with the same name exists
            existing = self.collection.find_one({
                "name": name,
                "_id": {"$ne": ObjectId(asset_class_id)}
            })
            if existing:
                logger.error(f"Another asset class with name '{name}' already exists")
                return False
            
            update_data = {
                "name": name
            }
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(asset_class_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating asset class: {e}")
            return False

    def delete_asset_class(self, asset_class_id):
        """Delete an asset class from the database
        
        Args:
            asset_class_id (str): ID of the asset class to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(asset_class_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting asset class: {e}")
            return False

    def get_all_asset_class_names(self):
        """Get only the names of all asset classes
        
        Returns:
            list: List of asset class names as strings, or empty list on error
        """
        try:
            asset_classes = self.collection.find({}, {"name": 1, "_id": 0}).sort("name", 1)
            return [asset_class["name"] for asset_class in asset_classes]
        except PyMongoError as e:
            logger.error(f"Database error while fetching asset class names: {e}")
            return []