# transaction_model.py
from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps
from app.models.transaction_version_model import TransactionVersionModel


class TransactionModel:
    """MongoDB model class for handling transaction operations and data management"""
    
    def __init__(self):
        """Initialize the TransactionModel with the 'transactions' collection"""
        self.collection = db["transactions"]
        self.transaction_version_model = TransactionVersionModel()

    def get_transaction(self, transaction_id):
        """Get a transaction by its ID
        
        Args:
            transaction_id (str): ID of the transaction to retrieve
            
        Returns:
            dict|None: Transaction data as dictionary, or None if not found or error
        """
        try:
            transaction = self.collection.find_one({"_id": ObjectId(transaction_id)})
            if transaction:
                transaction["_id"] = str(transaction["_id"])
                transaction["user_id"] = str(transaction["user_id"])
            return transaction
        except PyMongoError as e:
            logger.error(f"Database error while getting transaction: {e}")
            return None
        

    def create_transaction(self, user_id, name, base_file_path, primary_asset_class=None, secondary_asset_class=None):
        """Create a new transaction in the database with initial parameters"""
        try:
            # Check if any transaction with this name exists in the entire database
            existing_transaction = self.collection.find_one({"name": name})
            if existing_transaction:
                logger.error(f"Transaction with name '{name}' already exists")
                return None
                
            # Also check if any project with this name exists
            project_collection = db["projects"]
            existing_project = project_collection.find_one({"name": name})
            if existing_project:
                logger.error(f"A project with name '{name}' already exists")
                return None
                
            transaction_data = {
                "user_id": ObjectId(user_id),
                "name": name,
                "base_file_path": base_file_path,
                "version_number": 0,
                "base_file": None,
                "preprocessed_file": None,
                "column_rename_file": None,
                "temp_changing_datatype_of_column": None,
                "changed_datatype_of_column": None,
                "are_all_steps_complete": False,
                "new_added_columns_datatype": {},
                "temp_new_column_adding": None,
                "added_new_column_final": None,
                "temp_rbi_rules_applied": None,
                "final_rbi_rules_applied": None,
                "cutoff_date": None,
                "rule_application_root_versions": [],
                # NEW: Step tracking fields
                "steps_completed": {
                    "dataset_uploaded": False,
                    "column_mapping_done": False,
                    "datatype_conversion_done": False,
                    "new_fields_added": False,
                    "rbi_rules_applied": False,
                    "rule_versions_created": False
                },
                "temp_steps": {
                    "column_mapping_in_progress": False,
                    "datatype_conversion_in_progress": False,
                    "new_fields_in_progress": False,
                    "rbi_rules_in_progress": False,
                    "rule_versions_in_progress": False
                },
                "current_step": "upload_dataset"  # Track current step for navigation
            }
            
            # Add optional fields if provided
            if primary_asset_class is not None:
                transaction_data["primary_asset_class"] = primary_asset_class
            if secondary_asset_class is not None:
                transaction_data["secondary_asset_class"] = secondary_asset_class
                
            transaction_data = add_timestamps(transaction_data)
            result = self.collection.insert_one(transaction_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating transaction: {e}")
            return None

    # Add a new method to update the new_added_columns_datatype field
    def add_new_column_datatype(self, transaction_id, column_name, datatype):
        """Add a new column and its datatype to the new_added_columns_datatype field
        
        Args:
            transaction_id (str): ID of the transaction to update
            column_name (str): Name of the new column
            datatype (str): Datatype of the new column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {
                    "$set": {
                        f"new_added_columns_datatype.{column_name}": datatype,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while adding new column datatype: {e}")
            return False

    def update_transaction(self, transaction_id, update_fields):
        """Update transaction's fields with provided data
        
        Args:
            transaction_id (str): ID of the transaction to update
            update_fields (dict): Dictionary containing fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure '_id' and 'user_id' are not updated through this method
            update_fields.pop("_id", None)
            update_fields.pop("user_id", None)
            
            update_fields = add_timestamps(update_fields, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_fields}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating transaction: {e}")
            return False

    def delete_transaction(self, transaction_id):
        """Delete a transaction from the database
        
        Args:
            transaction_id (str): ID of the transaction to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(transaction_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting transaction: {e}")
            return False

    def get_transactions_by_user(self, user_id):
        """Fetch all transactions for a given user ID with base file location
        
        Args:
            user_id (str): ID of the user whose transactions are to be fetched
            
        Returns:
            list: List of transactions as dictionaries with base file location, or an empty list on error
        """
        try:
            transactions = self.collection.find({"user_id": ObjectId(user_id)})
            transaction_list = []
            for transaction in transactions:
                transaction["_id"] = str(transaction["_id"])
                transaction["user_id"] = str(transaction["user_id"])
                
                # Add base file location if base_file exists
                if transaction.get("base_file"):
                    base_version = self.transaction_version_model.get_version(transaction["base_file"])
                    if base_version:
                        transaction["base_file_location"] = base_version.get("files_path", "")
                else:
                    transaction["base_file_location"] = ""
                    
                transaction_list.append(transaction)
            return transaction_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching transactions for user {user_id}: {e}")
            return []

    def set_base_file(self, transaction_id, version_id):
        """Set the base_file version_id for a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
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
                {"_id": ObjectId(transaction_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while setting base_file for transaction {transaction_id}: {e}")
            return False

    def set_preprocessed_file(self, transaction_id, version_id):
        """Set the preprocessed_file version_id for a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
            version_id (str): Version ID to set as preprocessed_file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "preprocessed_file": version_id,
                "updated_at": datetime.now()
            }

            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while setting preprocessed_file for transaction {transaction_id}: {e}")
            return False

    def change_transaction_name(self, transaction_id, new_name):
        """
        Change the name of a transaction - updates database, folder name, and all associated files
        
        Args:
            transaction_id (str): ID of the transaction to update
            new_name (str): New name for the transaction
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get current transaction details
            current_transaction = self.get_transaction(transaction_id)
            if not current_transaction:
                logger.error(f"Transaction {transaction_id} not found")
                return False
                
            old_name = current_transaction["name"]
            old_base_path = current_transaction["base_file_path"]
            
            # Check if any other transaction with the new name exists
            existing_transaction = self.collection.find_one({
                "name": new_name,
                "_id": {"$ne": ObjectId(transaction_id)}
            })
            if existing_transaction:
                logger.error(f"Transaction with name '{new_name}' already exists")
                return False
                
            # Check if any project with this name exists
            project_collection = db["projects"]
            existing_project = project_collection.find_one({"name": new_name})
            if existing_project:
                logger.error(f"A project with name '{new_name}' already exists")
                return False
                
            # Calculate new paths
            import os
            from werkzeug.utils import secure_filename
            
            # Get the parent directory of the old base path
            parent_dir = os.path.dirname(old_base_path)
            
            # Create new folder name
            secure_new_name = secure_filename(new_name).replace(' ', '_')
            new_base_path = os.path.join(parent_dir, secure_new_name)
            
            # Check if new folder already exists
            if os.path.exists(new_base_path) and new_base_path != old_base_path:
                logger.error(f"Folder {new_base_path} already exists")
                return False
                
            # Get all versions associated with this transaction
            versions_to_update = []
            
            # Get versions from various fields
            if current_transaction.get("base_file"):
                versions_to_update.append(current_transaction["base_file"])
            if current_transaction.get("preprocessed_file"):
                versions_to_update.append(current_transaction["preprocessed_file"])
            if current_transaction.get("column_rename_file"):
                versions_to_update.append(current_transaction["column_rename_file"])
            if current_transaction.get("temp_changing_datatype_of_column"):
                versions_to_update.append(current_transaction["temp_changing_datatype_of_column"])
            if current_transaction.get("changed_datatype_of_column"):
                versions_to_update.append(current_transaction["changed_datatype_of_column"])
            if current_transaction.get("temp_rbi_rules_applied"):
                versions_to_update.append(current_transaction["temp_rbi_rules_applied"])
            if current_transaction.get("final_rbi_rules_applied"):
                versions_to_update.append(current_transaction["final_rbi_rules_applied"])
            if current_transaction.get("added_new_column_final"):
                versions_to_update.append(current_transaction["added_new_column_final"])
                
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
                version = self.transaction_version_model.get_version(version_id)
                if version and version.get("files_path"):
                    old_file_path = version["files_path"]
                    # Replace old base path with new base path
                    new_file_path = old_file_path.replace(old_base_path, new_base_path)
                    # Replace old name with new name in filename
                    new_file_path = new_file_path.replace(old_name_secure, new_name_secure)
                    
                    self.transaction_version_model.collection.update_one(
                        {"_id": ObjectId(version_id)},
                        {"$set": {"files_path": new_file_path, "updated_at": datetime.now()}}
                    )
            
            # Update transaction in database
            update_data = {
                "name": new_name,
                "base_file_path": new_base_path
            }
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_data}
            )
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"Error while changing transaction name: {e}")
            return False

    # Add this method to transaction_model.py
    def update_cutoff_date(self, transaction_id, cutoff_date):
        """Update the cutoff date for a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
            cutoff_date (str): Cutoff date in dd/mm/yyyy format
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {
                    "$set": {
                        "cutoff_date": cutoff_date,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating cutoff date: {e}")
            return False



    def add_rule_application_root_version(self, transaction_id, version_id):
        """Add a new root version for rule application"""
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$push": {"rule_application_root_versions": version_id}}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error adding root version: {e}")
            return False

    def remove_rule_application_root_version(self, transaction_id, version_id):
        """Remove a root version and all its sub-versions"""
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$pull": {"rule_application_root_versions": version_id}}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error removing root version: {e}")
            return False
        
    def update_step_status(self, transaction_id, step_name, status=True):
        """Update the status of a specific step
        
        Args:
            transaction_id (str): ID of the transaction
            step_name (str): Name of the step in steps_completed
            status (bool): Status to set (default True)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
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

    def update_temp_step_status(self, transaction_id, temp_step_name, status=True):
        """Update the status of a temporary step
        
        Args:
            transaction_id (str): ID of the transaction
            temp_step_name (str): Name of the temp step
            status (bool): Status to set (default True)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
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

    def update_current_step(self, transaction_id, step_name):
        """Update the current step
        
        Args:
            transaction_id (str): ID of the transaction
            step_name (str): Name of the current step
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
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

    def reset_steps_from(self, transaction_id, from_step):
        """Reset all steps from a specific step onwards
        
        Args:
            transaction_id (str): ID of the transaction
            from_step (str): Step from which to reset (all steps after this will be reset)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Define step order
            step_order = [
                "dataset_uploaded",
                "column_mapping_done",
                "datatype_conversion_done",
                "new_fields_added",
                "rbi_rules_applied",
                "rule_versions_created"
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
            if from_step in ["dataset_uploaded", "column_mapping_done"]:
                update_ops["temp_steps.column_mapping_in_progress"] = False
                update_ops["temp_steps.datatype_conversion_in_progress"] = False
                update_ops["temp_steps.new_fields_in_progress"] = False
                update_ops["temp_steps.rbi_rules_in_progress"] = False
                update_ops["temp_steps.rule_versions_in_progress"] = False
            elif from_step == "datatype_conversion_done":
                update_ops["temp_steps.new_fields_in_progress"] = False
                update_ops["temp_steps.rbi_rules_in_progress"] = False
                update_ops["temp_steps.rule_versions_in_progress"] = False
            elif from_step == "new_fields_added":
                update_ops["temp_steps.rbi_rules_in_progress"] = False
                update_ops["temp_steps.rule_versions_in_progress"] = False
            elif from_step == "rbi_rules_applied":
                update_ops["temp_steps.rule_versions_in_progress"] = False
                
            # Clear related data based on reset point
            if from_index < step_order.index("column_mapping_done"):
                update_ops["column_rename_file"] = None
            if from_index < step_order.index("datatype_conversion_done"):
                update_ops["temp_changing_datatype_of_column"] = None
                update_ops["changed_datatype_of_column"] = None
            if from_index < step_order.index("new_fields_added"):
                update_ops["temp_new_column_adding"] = None
                update_ops["added_new_column_final"] = None
                update_ops["new_added_columns_datatype"] = {}
            if from_index < step_order.index("rbi_rules_applied"):
                update_ops["temp_rbi_rules_applied"] = None
                update_ops["final_rbi_rules_applied"] = None
                update_ops["cutoff_date"] = None
            if from_index < step_order.index("rule_versions_created"):
                update_ops["rule_application_root_versions"] = []
                
            update_ops["updated_at"] = datetime.now()
            update_ops["current_step"] = from_step
            
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_ops}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error resetting steps: {e}")
            return False

    def get_next_step(self, transaction_id):
        """Get the next step user should be redirected to based on completed steps
        
        Args:
            transaction_id (str): ID of the transaction
            
        Returns:
            dict: Contains next_step and can_proceed flag
        """
        try:
            transaction = self.get_transaction(transaction_id)
            if not transaction:
                return {"next_step": None, "can_proceed": False, "error": "Transaction not found"}
                
            steps_completed = transaction.get("steps_completed", {})
            temp_steps = transaction.get("temp_steps", {})
            
            # Define step flow with their routes
            step_flow = [
                ("dataset_uploaded", "upload_dataset", "/transaction/upload-dataset"),
                ("column_mapping_done", "column_mapping", "/transaction/column-mapping"),
                ("datatype_conversion_done", "datatype_conversion", "/transaction/datatype-conversion"),
                ("new_fields_added", "add_fields", "/transaction/add-fields"),
                ("rbi_rules_applied", "rbi_rules", "/transaction/rbi-rules"),
                ("rule_versions_created", "rule_versions", "/transaction/rule-versions")
            ]
            
            # Check if there's a temp step in progress
            if temp_steps.get("column_mapping_in_progress"):
                return {"next_step": "column_mapping", "route": "/transaction/column-mapping", "can_proceed": True, "in_progress": True}
            if temp_steps.get("datatype_conversion_in_progress"):
                return {"next_step": "datatype_conversion", "route": "/transaction/datatype-conversion", "can_proceed": True, "in_progress": True}
            if temp_steps.get("new_fields_in_progress"):
                return {"next_step": "add_fields", "route": "/transaction/add-fields", "can_proceed": True, "in_progress": True}
            if temp_steps.get("rbi_rules_in_progress"):
                return {"next_step": "rbi_rules", "route": "/transaction/rbi-rules", "can_proceed": True, "in_progress": True}
            if temp_steps.get("rule_versions_in_progress"):
                return {"next_step": "rule_versions", "route": "/transaction/rule-versions", "can_proceed": True, "in_progress": True}
            
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
            
            # All tracked steps completed (but more can be added later)
            return {
                "next_step": "rule_versions",  # Stay on rule versions as more can be added
                "route": "/transaction/rule-versions",
                "can_proceed": True,
                "completed_steps": [s[1] for s in step_flow],
                "all_steps_complete": False  # Never truly complete as more versions can be added
            }
            
        except Exception as e:
            logger.error(f"Error getting next step: {e}")
            return {"next_step": None, "can_proceed": False, "error": str(e)}