from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class ArchiveDebtSheetModel:
    """MongoDB model class for handling archive operations and data management"""
    
    def __init__(self):
        """Initialize the ArchiveModel with the 'archives' collection"""
        self.collection = db["archives"]

    def create_archive_from_project(self, project_data):
        """
        Create an archive entry from project data
        
        Args:
            project_data (dict): Complete project data to be archived
            
        Returns:
            str|None: Inserted archive ID as string, or None on error
        """
        try:
            # Remove the _id field and store it as original_project_id
            original_project_id = project_data.pop("_id", None)
            
            # Create archive data
            archive_data = {
                **project_data,  # Include all project fields
                "original_project_id": original_project_id,  # Store original project ID
                "archived_at": datetime.now()  # Add archive timestamp
            }
            
            archive_data = add_timestamps(archive_data)
            result = self.collection.insert_one(archive_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating archive: {e}")
            return None

    def get_archive(self, archive_id):
        """
        Get an archive by its ID
        
        Args:
            archive_id (str): ID of the archive to retrieve
            
        Returns:
            dict|None: Archive data as dictionary, or None if not found or error
        """
        try:
            archive = self.collection.find_one({"_id": ObjectId(archive_id)})
            if archive:
                archive["_id"] = str(archive["_id"])
                archive["user_id"] = str(archive["user_id"])
                if "original_project_id" in archive:
                    archive["original_project_id"] = str(archive["original_project_id"])
            return archive
        except PyMongoError as e:
            logger.error(f"Database error while getting archive: {e}")
            return None

    def get_archives_by_user(self, user_id):
        """
        Fetch all archives for a given user ID
        
        Args:
            user_id (str): ID of the user whose archives are to be fetched
            
        Returns:
            list: List of archives as dictionaries, or an empty list on error
        """
        try:
            archives = self.collection.find({"user_id": ObjectId(user_id)})
            archive_list = []
            for archive in archives:
                archive["_id"] = str(archive["_id"])
                archive["user_id"] = str(archive["user_id"])
                if "original_project_id" in archive:
                    archive["original_project_id"] = str(archive["original_project_id"])
                archive_list.append(archive)
            return archive_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching archives for user {user_id}: {e}")
            return []

    def delete_archive(self, archive_id):
        """
        Delete an archive from the database
        
        Args:
            archive_id (str): ID of the archive to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(archive_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting archive: {e}")
            return False

    def restore_archive_to_project(self, archive_id):
        """
        Get archive data and prepare it for restoration as a project
        
        Args:
            archive_id (str): ID of the archive to restore
            
        Returns:
            dict|None: Archive data prepared for project restoration, or None on error
        """
        try:
            archive = self.collection.find_one({"_id": ObjectId(archive_id)})
            if not archive:
                return None
                
            # Remove archive-specific fields
            archive.pop("_id", None)
            archive.pop("archived_at", None)
            archive.pop("original_project_id", None)
            
            return archive
        except PyMongoError as e:
            logger.error(f"Database error while restoring archive: {e}")
            return None