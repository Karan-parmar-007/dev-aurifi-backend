from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class TransactionVersionModel:
    """MongoDB model class for handling transaction version operations and data management"""

    def __init__(self):
        """Initialize the TransactionVersionModel with the 'transaction_versions' collection"""
        self.collection = db["transaction_versions"]

    def create_version(self, transaction_id, description, files_path="", version_number=0, 
                    sent_for_rule_addition=None, tag_name=None, tag_type_name=None,
                    rows_count=None, rows_added=None, rows_removed=None, 
                    total_amount=None, modified=None, rbi_rules_metadata=None,
                    # New fields for rule application versioning
                    is_rule_application_version=False,
                    parent_version_id=None,
                    root_version_id=None,
                    branch_level=0,
                    branch_number=0,  # ADD THIS NEW FIELD
                    rule_applied=None,
                    stats_before_rule=None,
                    stats_after_rule=None):
        """
        Create a new transaction version with rule application support
        """
        try:
            version_data = {
                "transaction_id": ObjectId(transaction_id),
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
            if rbi_rules_metadata is not None:
                version_data["rbi_rules_metadata"] = rbi_rules_metadata
                
            if is_rule_application_version:
                version_data["is_rule_application_version"] = True
                version_data["parent_version_id"] = ObjectId(parent_version_id) if parent_version_id else None
                version_data["root_version_id"] = ObjectId(root_version_id) if root_version_id else None
                version_data["branch_level"] = branch_level
                version_data["branch_number"] = branch_number  # ADD THIS
                version_data["rule_applied"] = rule_applied
                version_data["stats_before_rule"] = stats_before_rule
                version_data["stats_after_rule"] = stats_after_rule
                
            version_data = add_timestamps(version_data)
            result = self.collection.insert_one(version_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating transaction version: {e}")
            return None

    def update_version(self, version_id, files_path):
        """
        Update transaction version's files_path information.

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
            logger.error(f"Database error while updating transaction version: {e}")
            return False

    def delete_version(self, version_id):
        """
        Delete a transaction version from the database.

        Args:
            version_id (str): ID of the version to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(version_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting transaction version: {e}")
            return False

    def get_version(self, version_id):
        """Get a transaction version by its ID
        
        Args:
            version_id (str): ID of the version to retrieve
            
        Returns:
            dict|None: Version data as dictionary, or None if not found or error
        """
        try:
            version = self.collection.find_one({"_id": ObjectId(version_id)})
            if version:
                version["_id"] = str(version["_id"])
                version["transaction_id"] = str(version["transaction_id"])
            return version
        except PyMongoError as e:
            logger.error(f"Database error while getting transaction version: {e}")
            return None

    def get_versions_by_transaction(self, transaction_id):
        """Fetch all versions for a given transaction ID
        
        Args:
            transaction_id (str): ID of the transaction whose versions are to be fetched
            
        Returns:
            list: List of versions as dictionaries, or an empty list on error
        """
        try:
            versions = self.collection.find({"transaction_id": ObjectId(transaction_id)})
            version_list = []
            for version in versions:
                version["_id"] = str(version["_id"])
                version["transaction_id"] = str(version["transaction_id"])
                version_list.append(version)
            return version_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching versions for transaction {transaction_id}: {e}")
            return []