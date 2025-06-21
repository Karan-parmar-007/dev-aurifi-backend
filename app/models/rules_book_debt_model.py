from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps

class RulesBookDebtModel:
    """MongoDB model class for handling rules in book debt operations"""
    
    def __init__(self):
        """Initialize the RulesBookDebtModel with the 'rules_book_debt' collection"""
        self.collection = db["rules_book_debt"]
    
    def create_rule(self, user_id, rule_name, rules, pin=False, tag_name="", type_of_rule="insertion"):
        """
        Create a new rule in the database
        
        Args:
            user_id (str): ID of the user creating the rule
            rule_name (str): Name of the rule
            rules (list): List of rule conditions
            pin (bool): Whether the rule is pinned
            tag_name (str): Tag name associated with the rule
            type_of_rule (str): Type of rule (insertion/ejection)
            
        Returns:
            str|None: Inserted rule ID as string, or None on error
        """
        try:
            rule_data = {
                "user_id": ObjectId(user_id),
                "rule_name": rule_name,
                "rules": rules,
                "pin": pin,
                "tag_name": tag_name,
                "type_of_rule": type_of_rule
            }
            rule_data = add_timestamps(rule_data)
            result = self.collection.insert_one(rule_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating rule: {e}")
            return None
    
    def get_rule_by_name(self, user_id, rule_name):
        """
        Get a rule by its name for a specific user
        
        Args:
            user_id (str): ID of the user
            rule_name (str): Name of the rule
            
        Returns:
            dict|None: Rule data as dictionary, or None if not found
        """
        try:
            rule = self.collection.find_one({
                "user_id": ObjectId(user_id),
                "rule_name": rule_name
            })
            if rule:
                rule["_id"] = str(rule["_id"])
                rule["user_id"] = str(rule["user_id"])
            return rule
        except PyMongoError as e:
            logger.error(f"Database error while getting rule by name: {e}")
            return None
    
    def get_rule_by_id(self, rule_id):
        """
        Get a rule by its ID
        
        Args:
            rule_id (str): ID of the rule
            
        Returns:
            dict|None: Rule data as dictionary, or None if not found
        """
        try:
            rule = self.collection.find_one({"_id": ObjectId(rule_id)})
            if rule:
                rule["_id"] = str(rule["_id"])
                rule["user_id"] = str(rule["user_id"])
            return rule
        except PyMongoError as e:
            logger.error(f"Database error while getting rule by id: {e}")
            return None
    
    def get_all_rules_by_user(self, user_id):
        """
        Get all rules for a specific user
        
        Args:
            user_id (str): ID of the user
            
        Returns:
            list: List of rules as dictionaries
        """
        try:
            rules = self.collection.find({"user_id": ObjectId(user_id)})
            rules_list = []
            for rule in rules:
                rule["_id"] = str(rule["_id"])
                rule["user_id"] = str(rule["user_id"])
                rules_list.append(rule)
            return rules_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching rules for user {user_id}: {e}")
            return []
    
    def update_rule(self, rule_id, update_data):
        """
        Update a rule by its ID
        
        Args:
            rule_id (str): ID of the rule to update
            update_data (dict): Data to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Remove fields that shouldn't be updated
            update_data.pop("_id", None)
            update_data.pop("user_id", None)
            
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(rule_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating rule {rule_id}: {e}")
            return False
    
    def update_rule_by_name(self, user_id, rule_name, update_data):
        """
        Update a rule by its name for a specific user
        
        Args:
            user_id (str): ID of the user
            rule_name (str): Name of the rule to update
            update_data (dict): Data to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Remove fields that shouldn't be updated
            update_data.pop("_id", None)
            update_data.pop("user_id", None)
            
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"user_id": ObjectId(user_id), "rule_name": rule_name},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating rule by name: {e}")
            return False
    
    def delete_rule(self, rule_id):
        """
        Delete a rule by its ID
        
        Args:
            rule_id (str): ID of the rule to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(rule_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting rule {rule_id}: {e}")
            return False
    
    def get_rules_by_tag(self, user_id, tag_name):
        """
        Get all rules for a specific tag
        
        Args:
            user_id (str): ID of the user
            tag_name (str): Tag name to filter by
            
        Returns:
            list: List of rules as dictionaries
        """
        try:
            rules = self.collection.find({
                "user_id": ObjectId(user_id),
                "tag_name": tag_name
            })
            rules_list = []
            for rule in rules:
                rule["_id"] = str(rule["_id"])
                rule["user_id"] = str(rule["user_id"])
                rules_list.append(rule)
            return rules_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching rules by tag: {e}")
            return []
    
    def get_pinned_rules(self, user_id):
        """
        Get all pinned rules for a specific user
        
        Args:
            user_id (str): ID of the user
            
        Returns:
            list: List of pinned rules as dictionaries
        """
        try:
            rules = self.collection.find({
                "user_id": ObjectId(user_id),
                "pin": True
            })
            rules_list = []
            for rule in rules:
                rule["_id"] = str(rule["_id"])
                rule["user_id"] = str(rule["user_id"])
                rules_list.append(rule)
            return rules_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching pinned rules: {e}")
            return []