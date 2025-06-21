from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class VersionModel:
    """MongoDB model class for handling version operations and data management"""

    def __init__(self):
        """Initialize the VersionModel with the 'versions' collection"""
        self.collection = db["versions"]

    # In version_model.py, update the create_version method:

    def create_version(self, project_id, description, files_path="", version_number=0, 
                    sent_for_rule_addition=None, tag_name=None, tag_type_name=None,
                    rows_count=None, rows_added=None, rows_removed=None, 
                    total_amount=None, modified=None, bdc_multiplier=None):
        """
        Create a new version in the database with initial parameters.

        Args:
            project_id (str): ID of the project associated with the version
            description (str): Description of the version
            files_path (str, optional): Path where files are stored. Defaults to empty string.
            version_number (float|int, optional): Version number.
            sent_for_rule_addition (bool, optional): Whether sent for rule addition (for subtypes).
            tag_name (str, optional): Tag name for this version.
            tag_type_name (str, optional): Tag type for this version.
            rows_count (int, optional): Number of rows in the file.
            rows_added (int, optional): Number of rows added to this version.
            rows_removed (int, optional): Number of rows removed from this version.
            total_amount (float, optional): Total sum of the amount column.
            modified (bool, optional): Whether this version was modified by rules.
            bdc_multiplier (float, optional): BDC multiplier value. Defaults to 1.

        Returns:
            str|None: Inserted version ID as string, or None on error
        """
        try:
            version_data = {
                "project_id": ObjectId(project_id),
                "description": description,
                "files_path": files_path,
                "version_number": version_number,
            }
            
            # Add optional fields if provided
            if sent_for_rule_addition is not None:
                version_data["sent_for_rule_addition"] = sent_for_rule_addition
            if tag_name is not None:
                version_data["tag_name"] = tag_name
            if tag_type_name is not None:
                version_data["tag_type_name"] = tag_type_name
            if rows_count is not None:
                version_data["rows_count"] = rows_count
            if rows_added is not None:
                version_data["rows_added"] = rows_added
            if rows_removed is not None:
                version_data["rows_removed"] = rows_removed
            if total_amount is not None:
                version_data["total_amount"] = total_amount
            if modified is not None:
                version_data["modified"] = modified
            
            # Add bdc_multiplier with default value of 1 if not provided
            version_data["bdc_multiplier"] = bdc_multiplier if bdc_multiplier is not None else 1
                
            version_data = add_timestamps(version_data)
            result = self.collection.insert_one(version_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating version: {e}")
            return None

    # Add a new method to update bdc_multiplier
    def update_bdc_multiplier(self, version_id, bdc_multiplier):
        """
        Update version's bdc_multiplier value.

        Args:
            version_id (str): ID of the version to update
            bdc_multiplier (float): New BDC multiplier value

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "bdc_multiplier": bdc_multiplier
            }
            update_data = add_timestamps(update_data, is_update=True)

            result = self.collection.update_one(
                {"_id": ObjectId(version_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating bdc_multiplier: {e}")
            return False

    def update_version(self, version_id, files_path):
        """
        Update version's files_path information.

        Args:
            version_id (str): ID of the version to update
            files_path (str): Updated file path

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "files_path": files_path
            }
            update_data = add_timestamps(update_data, is_update=True)

            result = self.collection.update_one(
                {"_id": ObjectId(version_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating version: {e}")
            return False

    def delete_version(self, version_id):
        """
        Delete a version from the database.

        Args:
            version_id (str): ID of the version to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(version_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting version: {e}")
            return False
        
