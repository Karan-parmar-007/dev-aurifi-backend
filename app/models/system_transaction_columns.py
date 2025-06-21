# system_transaction_columns.py
from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class SystemTransactionColumnModel:
    """MongoDB model class for handling system transaction column operations and data management"""
    
    def __init__(self):
        """Initialize the SystemTransactionColumnModel with the 'system_transaction_columns' collection"""
        self.collection = db["system_transaction_columns"]

    def create_column(self, column_name, description, alt_names, asset_class, datatype, general_mandatory=False, is_currency=False):
        """Create a new system transaction column in the database
        
        Args:
            column_name (str): Name of the column
            description (str): Description of the column
            alt_names (list): List of alternative names for the column
            asset_class (str): Asset class of the column
            datatype (str): Datatype of the column
            general_mandatory (bool): Whether the column is generally mandatory (default: False)
            is_currency (bool): Whether the column represents currency values (default: False)
            
        Returns:
            str|None: Inserted column ID as string, or None on error
        """
        try:
            column_data = {
                "column_name": column_name,
                "description": description,
                "alt_names": alt_names,
                "asset_class": asset_class,
                "datatype": datatype,
                "general_mandatory": general_mandatory,
                "is_currency": is_currency,
            }
            column_data = add_timestamps(column_data)
            result = self.collection.insert_one(column_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating system transaction column: {e}")
            return None

    def update_column(self, column_id, update_data):
        """Update a system transaction column
        
        Args:
            column_id (str): ID of the column to update
            update_data (dict): Dictionary containing fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = add_timestamps(update_data, is_update=True)
            result = self.collection.update_one(
                {"_id": ObjectId(column_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating system transaction column: {e}")
            return False

    def delete_column(self, column_id):
        """Delete a system transaction column from the database
        
        Args:
            column_id (str): ID of the column to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(column_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting system transaction column: {e}")
            return False

    def get_all_columns(self):
        """Get all system transaction columns from the database
        
        Returns:
            list|None: List of all columns, or None on error
        """
        try:
            columns = list(self.collection.find())
            # Convert ObjectId to string for JSON serialization
            for column in columns:
                column["_id"] = str(column["_id"])
            return columns
        except PyMongoError as e:
            logger.error(f"Database error while getting all system transaction columns: {e}")
            return None

    def get_column(self, column_id):
        """Get a single system transaction column by ID
        
        Args:
            column_id (str): ID of the column to retrieve
            
        Returns:
            dict|None: Column data as dictionary, or None if not found or error
        """
        try:
            column = self.collection.find_one({"_id": ObjectId(column_id)})
            if column:
                column["_id"] = str(column["_id"])
            return column
        except PyMongoError as e:
            logger.error(f"Database error while getting system transaction column: {e}")
            return None 
        
    def get_all_column_names(self):
        """Get only the names of all system transaction columns
        
        Returns:
            list|None: List of column names, or None on error
        """
        try:
            columns = self.collection.find({}, {"column_name": 1, "_id": 0})
            column_names = [column["column_name"] for column in columns if "column_name" in column]
            return column_names
        except PyMongoError as e:
            logger.error(f"Database error while getting transaction column names: {e}")
            return None
    
    def migrate_add_is_currency_field(self):
        """
        Migration method to add 'is_currency' field to existing system transaction columns.
        Sets default value to False for all existing columns.
        """
        try:
            result = self.collection.update_many(
                {"is_currency": {"$exists": False}},
                {"$set": {"is_currency": False}}
            )
            logger.info(f"Migration completed: {result.modified_count} columns updated with is_currency field")
            return result.modified_count
        except PyMongoError as e:
            logger.error(f"Error during is_currency migration: {e}")
            return 0