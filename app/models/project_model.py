from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps
from app.models.version_model import VersionModel

class ProjectModel:
    """MongoDB model class for handling project operations and data management"""
    
    def __init__(self):
        """Initialize the ProjectModel with the 'projects' collection"""
        self.collection = db["projects"]

    def get_project(self, project_id):
        """Get a project by its ID
        
        Args:
            project_id (str): ID of the project to retrieve
            
        Returns:
            dict|None: Project data as dictionary, or None if not found or error
        """
        try:
            project = self.collection.find_one({"_id": ObjectId(project_id)})
            if project:
                project["_id"] = str(project["_id"])
                project["user_id"] = str(project["user_id"])
            return project
        except PyMongoError as e:
            logger.error(f"Database error while getting project: {e}")
            return None

    def create_project(self, user_id, name, base_file_path, remove_duplicates):
        """Create a new project in the database with initial parameters"""
        try:
            # Check if any project with this name exists in the entire database
            existing_project = self.collection.find_one({"name": name})
            if existing_project:
                logger.error(f"Project with name '{name}' already exists")
                return None
                
            # Also check if any transaction with this name exists
            transaction_collection = db["transactions"]
            existing_transaction = transaction_collection.find_one({"name": name})
            if existing_transaction:
                logger.error(f"A transaction with name '{name}' already exists")
                return None
                
            project_data = {
                "user_id": ObjectId(user_id),
                "name": name,
                "base_file_path": base_file_path,
                "remove_duplicates": remove_duplicates,
                "version_number": 0,
                "split_with_tags": {},
                "temp_files": [],
                "files_with_rules_applied": [],
                "rows_added_files": [],
                "rows_removed_files": [],
                "are_all_steps_complete": False,
                "base_file": None,
                "dataset_after_preprocessing": None,
                "column_name_updated": False,
                "file_with_only_renaming_done": None,
                "file_with_both_renaming_and_datatype_conversion_done": None,
                "temp_datatype_conversion": None,
                "combined_file": None,
                # NEW: Step tracking fields
                "steps_completed": {
                    "dataset_uploaded": False,
                    "header_mapping_done": False,
                    "datatype_conversion_done": False,
                    "data_validation_done": False,
                    "split_by_tags_done": False,
                    "tags_selected_for_rules": False,
                    "rules_applied": False,
                    "finalized": False
                },
                "temp_steps": {
                    "header_mapping_in_progress": False,
                    "datatype_conversion_in_progress": False,
                    "rules_application_in_progress": False
                },
                "current_step": "upload_dataset"  # Track current step for navigation
            }
            project_data = add_timestamps(project_data)
            result = self.collection.insert_one(project_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating project: {e}")
            return None
    
    def update_project(self, project_id, datatype_mapping):
        """Update project with datatype mapping information
        
        NOTE: This method is deprecated as datatype_mapping is no longer used.
        Kept for backward compatibility.
        
        Args:
            project_id (str): ID of the project to update
            datatype_mapping (list): List of dictionaries containing column_name and datatype
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.warning("update_project method called with datatype_mapping - this field is deprecated")
        return True

    def delete_project(self, project_id):
        """
        Delete a project from the database
        
        Args:
            project_id (str): ID of the project to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(project_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting project: {e}")
            return False

    def get_projects_by_user(self, user_id):
        """Fetch all projects for a given user ID
        
        Args:
            user_id (str): ID of the user whose projects are to be fetched
            
        Returns:
            list: List of projects as dictionaries, or an empty list on error
        """
        try:
            projects = self.collection.find({"user_id": ObjectId(user_id)})
            project_list = []
            for project in projects:
                project["_id"] = str(project["_id"])
                project["user_id"] = str(project["user_id"])
                project_list.append(project)
            return project_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching projects for user {user_id}: {e}")
            return []
        
    def update_all_fields(self, project_id, update_fields):
        """
        Update all modifiable fields of a project.

        Args:
            project_id (str): ID of the project to update
            update_fields (dict): Dictionary containing the fields to update

        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            # Ensure '_id' and 'user_id' are not updated through this method
            update_fields.pop("_id", None)
            update_fields.pop("user_id", None)

            update_fields = add_timestamps(update_fields, is_update=True)

            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_fields}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating all fields of project {project_id}: {e}")
            return False

    def set_base_file(self, project_id, version_id):
        """
        Set the base_file version_id for a project.

        Args:
            project_id (str): ID of the project to update
            version_id (str): Version ID to set as base_file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "base_file": version_id,
                "updated_at": datetime.now()
            }

            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while setting base_file for project {project_id}: {e}")
            return False

    def set_dataset_after_preprocessing(self, project_id, version_id):
        """
        Set the dataset_after_preprocessing version_id for a project.

        Args:
            project_id (str): ID of the project to update
            version_id (str): Version ID to set as dataset_after_preprocessing

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "dataset_after_preprocessing": version_id,
                "updated_at": datetime.now()
            }

            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while setting dataset_after_preprocessing for project {project_id}: {e}")
            return False

    def update_split_with_tags(self, project_id, tag_versions):
        """
        Update the split_with_tags field for a project.

        Args:
            project_id (str): ID of the project to update
            tag_versions (dict): Dictionary of version_number: version_id pairs

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "split_with_tags": tag_versions,
                "updated_at": datetime.now()
            }

            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating split_with_tags for project {project_id}: {e}")
            return False

    def append_temp_file(self, project_id, temp_file_entry: dict) -> bool:
        """
        Append a new temp file entry to the temp_files list of a project.

        Args:
            project_id (str): ID of the project to update
            temp_file_entry (dict): Dictionary to append to temp_files (e.g., {"version_number": "version_id"})

        Returns:
            bool: True if the append operation is successful, False otherwise
        """
        try:
            update_data = {
                "updated_at": datetime.now()
            }
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$push": {"temp_files": temp_file_entry},
                    "$set": update_data
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while appending temp_file to project {project_id}: {e}")
            return False

    def remove_temp_file(self, project_id, version_id) -> bool:
        """
        Remove a temp file entry from the temp_files list of a project by version_id.

        Args:
            project_id (str): ID of the project to update
            version_id (ObjectId): Version ID to remove from temp_files

        Returns:
            bool: True if the remove operation is successful, False otherwise
        """
        try:
            update_data = {
                "updated_at": datetime.now()
            }
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$pull": {"temp_files": {"version": ObjectId(version_id)}},
                    "$set": update_data
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while removing temp_file from project {project_id}: {e}")
            return False

    def append_files_with_rules_applied(self, project_id, file_rule_entry: dict) -> bool:
        """
        Append a new entry to the files_with_rules_applied list of a project.

        Args:
            project_id (str): ID of the project to update
            file_rule_entry (dict): Dictionary to append (e.g., {"tagname_version": "version_id"})

        Returns:
            bool: True if the append operation is successful, False otherwise
        """
        try:
            update_data = {
                "updated_at": datetime.now()
            }
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$push": {"files_with_rules_applied": file_rule_entry},
                    "$set": update_data
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while appending files_with_rules_applied to project {project_id}: {e}")
            return False

    def get_datatype_mapping(self, project_id):
        """
        DEPRECATED: This method is no longer used as datatype_mapping field has been removed.
        
        Args:
            project_id (str): ID of the project
        Returns:
            list|None: Always returns empty list for backward compatibility
        """
        logger.warning("get_datatype_mapping called - this field has been removed from the model")
        return []

    def change_project_name(self, project_id, new_name):
        """
        Change the name of a project - updates database, folder name, and all associated files
        
        Args:
            project_id (str): ID of the project to update
            new_name (str): New name for the project
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get current project details
            current_project = self.get_project(project_id)
            if not current_project:
                logger.error(f"Project {project_id} not found")
                return False
                
            old_name = current_project["name"]
            old_base_path = current_project["base_file_path"]
            
            # Check if any other project with the new name exists
            existing_project = self.collection.find_one({
                "name": new_name,
                "_id": {"$ne": ObjectId(project_id)}
            })
            if existing_project:
                logger.error(f"Project with name '{new_name}' already exists")
                return False
                
            # Check if any transaction with this name exists
            transaction_collection = db["transactions"]
            existing_transaction = transaction_collection.find_one({"name": new_name})
            if existing_transaction:
                logger.error(f"A transaction with name '{new_name}' already exists")
                return False
                
            # Calculate new paths
            import os
            from werkzeug.utils import secure_filename
            
            # Get the parent directory of the old base path
            parent_dir = os.path.dirname(old_base_path)
            
            # Create new folder name
            secure_new_name = secure_filename(new_name).replace(' ', '_')
            new_base_path = os.path.join(parent_dir, secure_new_name)
            
            # Check if new folder already exists (shouldn't happen but just in case)
            if os.path.exists(new_base_path) and new_base_path != old_base_path:
                logger.error(f"Folder {new_base_path} already exists")
                return False
                
            # Get all versions associated with this project
            version_model = VersionModel()
            versions_to_update = []
            
            # Get versions from various fields
            if current_project.get("base_file"):
                versions_to_update.append(current_project["base_file"])
            if current_project.get("dataset_after_preprocessing"):
                versions_to_update.append(current_project["dataset_after_preprocessing"])
            if current_project.get("file_with_only_renaming_done"):
                versions_to_update.append(current_project["file_with_only_renaming_done"])
            if current_project.get("file_with_both_renaming_and_datatype_conversion_done"):
                versions_to_update.append(current_project["file_with_both_renaming_and_datatype_conversion_done"])
            if current_project.get("temp_datatype_conversion"):
                versions_to_update.append(current_project["temp_datatype_conversion"])
            if current_project.get("combined_file"):
                versions_to_update.append(current_project["combined_file"])
                
            # Add versions from split_with_tags
            for tag_data in current_project.get("split_with_tags", {}).values():
                if isinstance(tag_data, str):
                    versions_to_update.append(tag_data)
                    
            # Add versions from temp_files
            for temp_file in current_project.get("temp_files", []):
                for version_id in temp_file.values():
                    versions_to_update.append(version_id)
                    
            # Add versions from files_with_rules_applied
            for rule_file in current_project.get("files_with_rules_applied", []):
                for version_id in rule_file.values():
                    versions_to_update.append(version_id)
                    
            # Rename folder
            if os.path.exists(old_base_path):
                os.rename(old_base_path, new_base_path)
                
            # Update all files within the folder
            old_name_secure = secure_filename(old_name).replace(' ', '_')
            new_name_secure = secure_filename(new_name).replace(' ', '_')
            
            for filename in os.listdir(new_base_path):
                if old_name_secure in filename:
                    old_file_path = os.path.join(new_base_path, filename)
                    new_filename = filename.replace(old_name_secure, new_name_secure)
                    new_file_path = os.path.join(new_base_path, new_filename)
                    os.rename(old_file_path, new_file_path)
                    
            # Update all version records with new file paths
            for version_id in versions_to_update:
                version = version_model.get_version(version_id)
                if version and version.get("files_path"):
                    old_file_path = version["files_path"]
                    # Replace old base path with new base path
                    new_file_path = old_file_path.replace(old_base_path, new_base_path)
                    # Replace old name with new name in filename
                    new_file_path = new_file_path.replace(old_name_secure, new_name_secure)
                    
                    version_model.collection.update_one(
                        {"_id": ObjectId(version_id)},
                        {"$set": {"files_path": new_file_path, "updated_at": datetime.now()}}
                    )
            
            # Update project in database
            update_data = {
                "name": new_name,
                "base_file_path": new_base_path
            }
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_data}
            )
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"Error while changing project name: {e}")
            return False

    def migrate_remove_deprecated_fields(self):
        """
        Migration method to remove deprecated fields from all projects.
        This removes 'datatype_mapping' and 'sub_versions' from all documents.
        """
        try:
            result = self.collection.update_many(
                {},
                {"$unset": {"datatype_mapping": "", "sub_versions": ""}}
            )
            logger.info(f"Migration completed: {result.modified_count} projects updated")
            return result.modified_count
        except PyMongoError as e:
            logger.error(f"Error during migration: {e}")
            return 0
        
    # In project_model.py, add new methods:
    def append_rows_added_file(self, project_id, file_entry: dict) -> bool:
        """
        Append a new rows_added file entry to the project.
        
        Args:
            project_id (str): ID of the project to update
            file_entry (dict): Dictionary to append (e.g., {"tag_name": "version_id"})
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "updated_at": datetime.now()
            }
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$push": {"rows_added_files": file_entry},
                    "$set": update_data
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while appending rows_added_file to project {project_id}: {e}")
            return False

    def append_rows_removed_file(self, project_id, file_entry: dict) -> bool:
        """
        Append a new rows_removed file entry to the project.
        
        Args:
            project_id (str): ID of the project to update
            file_entry (dict): Dictionary to append (e.g., {"tag_name": "version_id"})
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "updated_at": datetime.now()
            }
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$push": {"rows_removed_files": file_entry},
                    "$set": update_data
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while appending rows_removed_file to project {project_id}: {e}")
            return False

    def clear_rows_tracking_files(self, project_id):
        """Clear all existing rows_added and rows_removed files"""
        try:
            import os  # Import here to avoid circular imports
            
            project = self.get_project(project_id)
            if not project:
                logger.warning(f"Project {project_id} not found")
                return True
            
            # Import VersionModel here to avoid circular imports
            from app.models.version_model import VersionModel
            version_model = VersionModel()
            
            deleted_files_count = 0
            deleted_versions_count = 0
            
            # Check and clear rows_added_files
            rows_added_files = project.get("rows_added_files", [])
            if rows_added_files:
                logger.info(f"Found {len(rows_added_files)} existing rows_added_files entries")
                
                for file_entry in rows_added_files:
                    for tag_name, version_id in file_entry.items():
                        logger.info(f"Processing rows_added for tag: {tag_name}, version_id: {version_id}")
                        
                        # Get version details
                        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                        if version:
                            file_path = version.get("files_path", "")
                            
                            # Delete physical file if exists
                            if file_path and os.path.exists(file_path):
                                try:
                                    os.remove(file_path)
                                    deleted_files_count += 1
                                    logger.info(f"Deleted file: {file_path}")
                                except Exception as e:
                                    logger.error(f"Failed to delete file {file_path}: {str(e)}")
                            else:
                                logger.warning(f"File not found: {file_path}")
                            
                            # Delete version record
                            if version_model.delete_version(version_id):
                                deleted_versions_count += 1
                                logger.info(f"Deleted version record: {version_id}")
                        else:
                            logger.warning(f"Version {version_id} not found in database")
            
            # Check and clear rows_removed_files
            rows_removed_files = project.get("rows_removed_files", [])
            if rows_removed_files:
                logger.info(f"Found {len(rows_removed_files)} existing rows_removed_files entries")
                
                for file_entry in rows_removed_files:
                    for tag_name, version_id in file_entry.items():
                        logger.info(f"Processing rows_removed for tag: {tag_name}, version_id: {version_id}")
                        
                        # Get version details
                        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                        if version:
                            file_path = version.get("files_path", "")
                            
                            # Delete physical file if exists
                            if file_path and os.path.exists(file_path):
                                try:
                                    os.remove(file_path)
                                    deleted_files_count += 1
                                    logger.info(f"Deleted file: {file_path}")
                                except Exception as e:
                                    logger.error(f"Failed to delete file {file_path}: {str(e)}")
                            else:
                                logger.warning(f"File not found: {file_path}")
                            
                            # Delete version record
                            if version_model.delete_version(version_id):
                                deleted_versions_count += 1
                                logger.info(f"Deleted version record: {version_id}")
                        else:
                            logger.warning(f"Version {version_id} not found in database")
            
            # Clear arrays in project document
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": {
                    "rows_added_files": [], 
                    "rows_removed_files": [],
                    "updated_at": datetime.now()
                }}
            )
            
            if result.modified_count > 0:
                logger.info(f"Successfully cleared tracking arrays in project. Deleted {deleted_files_count} files and {deleted_versions_count} version records")
                return True
            else:
                logger.warning(f"No modifications made to project document")
                return True  # Still return True as files might have been deleted
            
        except Exception as e:
            logger.error(f"Error clearing rows tracking files: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    
    
    def update_step_status(self, project_id, step_name, status=True):
        """Update the status of a specific step
        
        Args:
            project_id (str): ID of the project
            step_name (str): Name of the step in steps_completed
            status (bool): Status to set (default True)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$set": {
                        f"steps_completed.{step_name}": status,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error updating step status: {e}")
            return False

    def update_temp_step_status(self, project_id, temp_step_name, status=True):
        """Update the status of a temporary step
        
        Args:
            project_id (str): ID of the project
            temp_step_name (str): Name of the temp step
            status (bool): Status to set (default True)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$set": {
                        f"temp_steps.{temp_step_name}": status,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error updating temp step status: {e}")
            return False

    def update_current_step(self, project_id, step_name):
        """Update the current step
        
        Args:
            project_id (str): ID of the project
            step_name (str): Name of the current step
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {
                    "$set": {
                        "current_step": step_name,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error updating current step: {e}")
            return False

    def reset_steps_from(self, project_id, from_step):
        """Reset all steps from a specific step onwards
        
        Args:
            project_id (str): ID of the project
            from_step (str): Step from which to reset (all steps after this will be reset)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Define step order
            step_order = [
                "dataset_uploaded",
                "header_mapping_done",
                "datatype_conversion_done", 
                "data_validation_done",
                "split_by_tags_done",
                "tags_selected_for_rules",
                "rules_applied",
                "finalized"
            ]
            
            # Find index of from_step
            if from_step not in step_order:
                return False
                
            from_index = step_order.index(from_step)
            
            # Build update operations for steps to reset
            update_ops = {}
            for i in range(from_index + 1, len(step_order)):
                update_ops[f"steps_completed.{step_order[i]}"] = False
            
            # Also reset temp steps if going back
            if from_step in ["dataset_uploaded", "header_mapping_done"]:
                update_ops["temp_steps.header_mapping_in_progress"] = False
                update_ops["temp_steps.datatype_conversion_in_progress"] = False
                update_ops["temp_steps.rules_application_in_progress"] = False
            elif from_step == "datatype_conversion_done":
                update_ops["temp_steps.datatype_conversion_in_progress"] = False
                update_ops["temp_steps.rules_application_in_progress"] = False
            elif from_step in ["split_by_tags_done", "tags_selected_for_rules"]:
                update_ops["temp_steps.rules_application_in_progress"] = False
                
            # Clear related data based on reset point
            if from_index < step_order.index("split_by_tags_done"):
                update_ops["split_with_tags"] = {}
            if from_index < step_order.index("rules_applied"):
                update_ops["temp_files"] = []
                update_ops["rows_added_files"] = []
                update_ops["rows_removed_files"] = []
            if from_index < step_order.index("finalized"):
                update_ops["files_with_rules_applied"] = []
                update_ops["combined_file"] = None
                update_ops["are_all_steps_complete"] = False
                
            update_ops["updated_at"] = datetime.now()
            update_ops["current_step"] = from_step
            
            result = self.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": update_ops}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error resetting steps: {e}")
            return False

    def get_next_step(self, project_id):
        """Get the next step user should be redirected to based on completed steps
        
        Args:
            project_id (str): ID of the project
            
        Returns:
            dict: Contains next_step and can_proceed flag
        """
        try:
            project = self.get_project(project_id)
            if not project:
                return {"next_step": None, "can_proceed": False, "error": "Project not found"}
                
            steps_completed = project.get("steps_completed", {})
            temp_steps = project.get("temp_steps", {})
            
            # Define step flow with their routes
            step_flow = [
                ("dataset_uploaded", "upload_dataset", "/upload-dataset"),
                ("header_mapping_done", "header_mapping", "/header-mapping"),
                ("datatype_conversion_done", "datatype_conversion", "/datatype-conversion"),
                ("data_validation_done", "data_validation", "/data-validation"),
                ("split_by_tags_done", "split_by_tags", "/split-by-tags"),
                ("tags_selected_for_rules", "select_tags", "/select-tags"),
                ("rules_applied", "apply_rules", "/apply-rules"),
                ("finalized", "review_and_save", "/review-and-save")
            ]
            
            # Check if there's a temp step in progress
            if temp_steps.get("header_mapping_in_progress"):
                return {"next_step": "header_mapping", "route": "/header-mapping", "can_proceed": True, "in_progress": True}
            if temp_steps.get("datatype_conversion_in_progress"):
                return {"next_step": "datatype_conversion", "route": "/datatype-conversion", "can_proceed": True, "in_progress": True}
            if temp_steps.get("rules_application_in_progress"):
                return {"next_step": "apply_rules", "route": "/apply-rules", "can_proceed": True, "in_progress": True}
            
            # Find the next incomplete step
            for i, (step_key, step_name, route) in enumerate(step_flow):
                if not steps_completed.get(step_key, False):
                    # Check if user can proceed to this step
                    can_proceed = True
                    if i > 0:
                        # Check if previous step is completed
                        prev_step_key = step_flow[i-1][0]
                        can_proceed = steps_completed.get(prev_step_key, False)
                        
                    return {
                        "next_step": step_name,
                        "route": route,
                        "can_proceed": can_proceed,
                        "completed_steps": [s[1] for s in step_flow[:i] if steps_completed.get(s[0], False)],
                        "all_steps_complete": False
                    }
            
            # All steps completed
            return {
                "next_step": "completed",
                "route": "/project-complete",
                "can_proceed": True,
                "completed_steps": [s[1] for s in step_flow],
                "all_steps_complete": True
            }
            
        except Exception as e:
            logger.error(f"Error getting next step: {e}")
            return {"next_step": None, "can_proceed": False, "error": str(e)}

