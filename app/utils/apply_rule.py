import pandas as pd
import os
from bson import ObjectId
from werkzeug.utils import secure_filename
from app.utils.logger import logger
from app.models.version_model import VersionModel
from app.models.project_model import ProjectModel
from app.models.system_column_model import SystemColumnModel
from app.utils.date_formatter import DateFormatter
from app.models.version_model import VersionModel

class ApplyRule:
    def __init__(self, project, data):
        """Initialize ApplyRule with project and rule data"""
        self.project = project
        self.data = data
        self.version_model = VersionModel()
        self.project_model = ProjectModel()
        self.system_column_model = SystemColumnModel()
        self.dfs = {}
        self.version_map = {}
        self.ejection_results = []
        self.inclusion_results = []
        self.datatype_mapping = self.get_datatype_mapping_from_system_columns()
        self.initial_untagged_count = 0
        # Add tracking for ejected/injected rows
        self.ejected_rows = {}  # tag_key: DataFrame
        self.injected_rows = {}  # tag_key: DataFrame

    def get_datatype_mapping_from_system_columns(self):
        """Get datatype mapping from system columns"""
        try:
            system_columns = self.system_column_model.get_all_columns()
            if not system_columns:
                logger.warning("No system columns found")
                return {}
            
            # Create mapping of column_name to datatype
            datatype_map = {}
            for column in system_columns:
                column_name = column.get("column_name")
                datatype = column.get("datatype")
                if column_name and datatype:
                    datatype_map[column_name] = datatype
            
            return datatype_map
        except Exception as e:
            logger.error(f"Error getting datatype mapping: {str(e)}")
            return {}

    def convert_column_type(self, df, column, dtype):
        """Convert column to specified data type"""
        try:
            if dtype in ["numeric", "number", "float", "decimal"]:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif dtype in ["integer", "int"]:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype('Int64')
            elif dtype in ["datetime", "date"]:
                # Fix: Add dayfirst=True for dd/mm/yyyy format
                df[column] = pd.to_datetime(df[column], errors="coerce", dayfirst=True)
            elif dtype in ["string", "text", "varchar"]:
                df[column] = df[column].astype(str)
            elif dtype in ["boolean", "bool"]:
                bool_map = {
                    'true': True, 'false': False,
                    'yes': True, 'no': False,
                    '1': True, '0': False,
                    't': True, 'f': False,
                    'y': True, 'n': False
                }
                df[column] = df[column].str.lower().map(bool_map)
            else:
                logger.warning(f"Unsupported datatype {dtype} for column {column}")
        except Exception as e:
            logger.error(f"Error converting {column} to {dtype}: {str(e)}")
        return df

    def apply_datatype_mapping(self, df):
        """Apply datatype mapping to dataframe columns based on system columns"""
        if not self.datatype_mapping:
            return df
            
        for col in df.columns:
            if col in self.datatype_mapping:
                dtype = self.datatype_mapping[col]
                df = self.convert_column_type(df, col, dtype)
        return df

    def load_versions(self):
        """Load all relevant versions and create dataframes"""
        version_ids = set()
        untagged_key = "untagged_unknown"

        # Collect version IDs from rules
        for rule_type in ["ejection", "acception_rules_for_all_files"]:
            for entry in self.data.get(rule_type, []):
                version_ids.add(entry.get("version_id"))

        # Initialize untagged dataframe
        self.dfs[untagged_key] = pd.DataFrame()

        # Load from split_with_tags (not temp_files or sub_versions)
        split_with_tags = self.project.get("split_with_tags", {})
        
        # Track initial untagged count
        for version_str, version_id in split_with_tags.items():
            version_obj = self.version_model.collection.find_one({"_id": ObjectId(version_id)})
            if version_obj:
                tag_name = version_obj.get("tag_name", "").strip().lower()
                tag_type = version_obj.get("tag_type_name", "").strip().lower()
                if tag_name == "untagged" and tag_type == "unknown":
                    self.initial_untagged_count = version_obj.get("rows_count", 0)
                    break
        
        for version_str, version_id in split_with_tags.items():
            version_obj = self.version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version_obj:
                continue
                
            tag_name = version_obj.get("tag_name", "").strip().lower()
            tag_type = version_obj.get("tag_type_name", "").strip().lower()
            
            # Load either versions mentioned in rules OR any untagged version
            if str(version_id) in version_ids or (tag_name == "untagged" and tag_type == "unknown"):
                key, df = self.load_version_data(version_id)
                if key and df is not None:
                    if key == untagged_key:
                        # For untagged data, append to existing untagged dataframe
                        if "from_tag" not in df.columns:
                            df["from_tag"] = ""
                        self.dfs[untagged_key] = pd.concat([self.dfs[untagged_key], df], ignore_index=True)
                    else:
                        # For tagged data, store as normal
                        self.dfs[key] = df
                        self.version_map[key] = {
                            "original_version": version_str,
                            "file_path": version_obj.get("files_path", ""),
                            "tag_name": version_obj.get("tag_name", ""),
                            "tag_type": version_obj.get("tag_type_name", "")
                        }

        # Ensure untagged dataframe has proper columns
        if self.dfs[untagged_key].empty and any(self.dfs.values()):
            sample_df = next((df for df in self.dfs.values() if not df.empty), None)
            if sample_df is not None:
                columns = sample_df.columns.tolist()
                if "from_tag" not in columns:
                    columns.append("from_tag")
                self.dfs[untagged_key] = pd.DataFrame(columns=columns)
                self.dfs[untagged_key]["from_tag"] = ""

    def load_version_data(self, version_id):
        """Load and prepare version data"""
        try:
            version = self.version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version:
                return None, None

            file_path = version.get("files_path", "")
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                return None, None

            # Read file
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
            else:
                return None, None

            # Apply datatype mapping based on system columns
            df = self.apply_datatype_mapping(df)

            # Get metadata
            tag_name = version.get("tag_name", "").strip().lower()
            tag_type = version.get("tag_type_name", "").strip().lower()
            
            # Create key from tag info
            key = f"{tag_name}_{tag_type}"

            # Initialize from_tag column if it doesn't exist
            if "from_tag" not in df.columns:
                df["from_tag"] = ""

            # Special handling for untagged_unknown - ALWAYS load it
            if key == "untagged_unknown":
                return key, df
                    
            # For other files, check sent_for_rule_addition
            if not version.get("sent_for_rule_addition", False):
                return None, None
                    
            if not tag_name or not tag_type:
                logger.error(f"Missing tags in version {version_id}")
                return None, None

            return key, df

        except Exception as e:
            logger.error(f"Error loading version {version_id}: {str(e)}")
            return None, None
    
    def build_condition(self, df, rule):
        """Build individual condition for a rule"""
        col = rule["column"].strip()
        op = rule["operator"].strip().lower()
        val = rule["value"]
        
        if col not in df.columns:
            return pd.Series([False] * len(df))

        try:
            # Check column datatype from system columns
            col_dtype = self.datatype_mapping.get(col, "string").lower()
            
            if op == "equal to":
                # Make comparison case-insensitive for string columns
                if col_dtype in ["string", "text", "varchar"]:
                    return df[col].str.lower() == str(val).lower()
                else:
                    return df[col] == str(val)
            elif op == "not equal to":
                # Make comparison case-insensitive for string columns
                if col_dtype in ["string", "text", "varchar"]:
                    return df[col].str.lower() != str(val).lower()
                else:
                    return df[col] != str(val)
            elif op == "greater than":
                if col_dtype in ["date", "datetime"]:
                    # Fix: Add dayfirst=True for date comparisons
                    return pd.to_datetime(df[col], errors="coerce", dayfirst=True) > pd.to_datetime(val, dayfirst=True)
                else:
                    # For numeric values
                    return pd.to_numeric(df[col], errors="coerce") > float(val)
            elif op == "less than":
                if col_dtype in ["date", "datetime"]:
                    # Fix: Add dayfirst=True for date comparisons
                    return pd.to_datetime(df[col], errors="coerce", dayfirst=True) < pd.to_datetime(val, dayfirst=True)
                else:
                    # For numeric values
                    return pd.to_numeric(df[col], errors="coerce") < float(val)
            elif op == "includes":
                return df[col].str.contains(str(val), case=False, na=False)
            else:
                logger.warning(f"Unsupported operator: {op}")
                return pd.Series([False] * len(df))
        except Exception as e:
            logger.error(f"Error building condition for rule {rule}: {str(e)}")
            return pd.Series([False] * len(df))
    
    def build_mask(self, df, rule_group):
        """Build combined mask for a group of rules"""
        if not rule_group:
            return pd.Series([False] * len(df))

        mask = None
        for i, rule in enumerate(rule_group):
            condition = self.build_condition(df, rule)
            connector = rule.get("connector", "AND").strip().upper()

            if i == 0:
                mask = condition
            else:
                # Only use the connector if it's not "THEN"
                if connector == "THEN":
                    # THEN is just used to indicate the final rule with accept/reject
                    # Continue using AND logic
                    mask &= condition
                elif connector == "OR":
                    mask |= condition
                else:  # Default to AND
                    mask &= condition

        return mask
    
    def apply_ejection_rules(self):
        """Process ejection rules"""
        untagged_key = "untagged_unknown"
        
        for ejection_rule in self.data.get("ejection", []):
            tag_name = ejection_rule.get("tag_name", "").strip().lower()
            tag_type = ejection_rule.get("tag_type", "").strip().lower()
            key = f"{tag_name}_{tag_type}"
            
            if key not in self.dfs:
                logger.warning(f"Skipping ejection for missing key: {key}")
                continue

            for rule_group in ejection_rule.get("rules", []):
                # Extract the 'then' value from the last rule in the group
                rule_type = "reject"  # default
                if rule_group and len(rule_group) > 0:
                    last_rule = rule_group[-1]
                    rule_type = last_rule.get("then", "reject").lower()
                
                self.dfs[key], self.dfs[untagged_key], count = self.perform_ejection(
                    self.dfs[key], self.dfs[untagged_key], rule_group, tag_name, rule_type
                )
                self.ejection_results.append({
                    "tag_name": tag_name,
                    "tag_type": tag_type,
                    "rule_type": rule_type,
                    "rule_group": rule_group,
                    "ejected_rows": count
                })

    def perform_ejection(self, source_df, untagged_df, rule_group, source_tag, rule_type="reject"):
        """Perform ejection operation"""
        try:
            mask = self.build_mask(source_df, rule_group)
            
            # Apply logic based on rule_type
            if rule_type == "accept":
                # Accept: Keep only matching rows, eject all others
                ejected_rows = source_df[~mask].copy()  # Invert mask
                updated_source = source_df[mask].reset_index(drop=True)
            else:  # reject (default)
                # Reject: Eject matching rows, keep all others
                ejected_rows = source_df[mask].copy()
                updated_source = source_df[~mask].reset_index(drop=True)
            
            # Keep remaining rows from source
            # updated_source = source_df[~mask].reset_index(drop=True)
            
            # Add from_tag to ejected rows 
            ejected_rows["from_tag"] = source_tag

            # Track ejected rows for the SOURCE tag
            tag_key = f"{source_tag}_ejected"
            if tag_key not in self.ejected_rows:
                self.ejected_rows[tag_key] = pd.DataFrame()
            self.ejected_rows[tag_key] = pd.concat([self.ejected_rows[tag_key], ejected_rows], ignore_index=True)

            # NEW: Also track these as rows ADDED TO untagged
            untagged_added_key = "untagged_unknown_injected"
            if untagged_added_key not in self.injected_rows:
                self.injected_rows[untagged_added_key] = pd.DataFrame()
            self.injected_rows[untagged_added_key] = pd.concat([self.injected_rows[untagged_added_key], ejected_rows], ignore_index=True)

            # If untagged_df is empty or doesn't have from_tag column, initialize it
            if untagged_df.empty:
                untagged_df = pd.DataFrame(columns=ejected_rows.columns)
                if "from_tag" not in untagged_df.columns:
                    untagged_df["from_tag"] = ""
            elif "from_tag" not in untagged_df.columns:
                untagged_df["from_tag"] = ""
                
            # Combine existing untagged rows with newly ejected rows
            updated_untagged = pd.concat([untagged_df, ejected_rows], ignore_index=True)
            
            return updated_source, updated_untagged, len(ejected_rows)
        except Exception as e:
            logger.error(f"Ejection failed: {str(e)}")
            return source_df, untagged_df, 0

    def apply_inclusion_rules(self):
        """Process inclusion rules"""
        untagged_key = "untagged_unknown"
        
        for inclusion_rule in self.data.get("acception_rules_for_all_files", []):
            tag_name = inclusion_rule.get("tag_name", "").strip().lower()
            tag_type = inclusion_rule.get("tag_type", "").strip().lower()
            key = f"{tag_name}_{tag_type}"
            
            if key not in self.dfs:
                logger.warning(f"Creating new dataframe for key: {key}")
                self.dfs[key] = pd.DataFrame()

            for rule_group in inclusion_rule.get("rules", []):
                # Extract the 'then' value from the last rule in the group
                rule_type = "accept"  # default
                if rule_group and len(rule_group) > 0:
                    last_rule = rule_group[-1]
                    rule_type = last_rule.get("then", "accept").lower()
                
                self.dfs[key], self.dfs[untagged_key], count = self.perform_inclusion(
                    self.dfs[key], self.dfs[untagged_key], rule_group, tag_name, tag_type, rule_type
                )
                self.inclusion_results.append({
                    "tag_name": tag_name,
                    "tag_type": tag_type,
                    "rule_type": rule_type,
                    "rule_group": rule_group,
                    "added_rows": count
                })

    def perform_inclusion(self, target_df, untagged_df, rule_group, target_tag_name, target_tag_type, rule_type="accept"):
        """Perform inclusion operation"""
        try:
            if untagged_df.empty:
                return target_df, untagged_df, 0
                
            mask = self.build_mask(untagged_df, rule_group)
            
            # Apply logic based on rule_type
            if rule_type == "accept":
                # Accept: Include rows that match the condition
                included_rows = untagged_df[mask].copy()
                updated_untagged = untagged_df[~mask].reset_index(drop=True)
            else:  # reject
                # Reject: Include rows that DON'T match the condition
                included_rows = untagged_df[~mask].copy()  # Invert mask
                updated_untagged = untagged_df[mask].reset_index(drop=True)
            
            # Only continue if we have rows to include
            if len(included_rows) == 0:
                return target_df, untagged_df, 0
                
            # Mark included rows with their source
            if "from_tag" not in included_rows.columns:
                included_rows["from_tag"] = "untagged"
            elif included_rows["from_tag"].isna().all():
                included_rows.loc[included_rows["from_tag"].isna(), "from_tag"] = "untagged"
                
            # Track injected rows for the TARGET tag
            tag_key = f"{target_tag_name}_{target_tag_type}_injected"
            if tag_key not in self.injected_rows:
                self.injected_rows[tag_key] = pd.DataFrame()
            self.injected_rows[tag_key] = pd.concat([self.injected_rows[tag_key], included_rows], ignore_index=True)

            # NEW: Also track these as rows REMOVED FROM untagged
            untagged_removed_key = "untagged_ejected"
            if untagged_removed_key not in self.ejected_rows:
                self.ejected_rows[untagged_removed_key] = pd.DataFrame()
            self.ejected_rows[untagged_removed_key] = pd.concat([self.ejected_rows[untagged_removed_key], included_rows], ignore_index=True)
                
            # Add included rows to target dataframe
            if target_df.empty:
                # If target is empty, initialize with the same columns
                target_df = pd.DataFrame(columns=included_rows.columns)
            
            # Make sure target has from_tag column
            if "from_tag" not in target_df.columns:
                target_df["from_tag"] = ""
                
            updated_target = pd.concat([target_df, included_rows], ignore_index=True)
            return updated_target, updated_untagged, len(included_rows)
        except Exception as e:
            logger.error(f"Inclusion failed: {str(e)}")
            return target_df, untagged_df, 0

    def save_new_versions(self):
        """Create and save new versions to temp_files with tag_name as key"""
        project_name = secure_filename(self.project.get("name", f"project_{self.project['_id']}"))
        # Replace spaces with underscores
        project_name = project_name.replace(' ', '_')
        upload_folder = os.path.join(os.getcwd(), 'datasets')
        project_folder = os.path.join(upload_folder, project_name)
        untagged_key = "untagged_unknown"
        new_temp_versions = []
        processed_keys = set()
        
        # Start versioning at 4.1
        version_counter = 1

        # Create project folder if it doesn't exist
        if not os.path.exists(project_folder):
            os.makedirs(project_folder)

        # Process files with modifications first
        for key, df in self.dfs.items():
            if key == untagged_key or key not in self.version_map:
                continue
                
            processed_keys.add(key)
            original_info = self.version_map[key]
            tag_name = original_info.get("tag_name", "")
            tag_type = original_info.get("tag_type", "")
            
            # Create version string v4.x (still needed for version_number)
            version_str = f"v4.{version_counter}"
            
            # Updated file naming convention with proper format
            ext = os.path.splitext(original_info["file_path"])[1]
            # Convert tag_name to proper case for filename (capitalize first letter)
            tag_name_for_file = tag_name.capitalize() if tag_name.lower() != "untagged" else "Untagged"
            filename = f"{project_name}_original_preprocessed_updated_column_names_datatype_converted_tags_{tag_name_for_file}_temp{ext}"
            filepath = os.path.join(project_folder, filename)

            # Save the file
            self.save_dataframe(df, filepath, ext)
            
            # Create version entry
            version_id = self.version_model.create_version(
                project_id=str(self.project["_id"]),
                description=f"Temporary rules applied for {tag_name} - {tag_type}",
                files_path=filepath,
                version_number=4.0 + (version_counter * 0.1),
                tag_name=tag_name,
                tag_type_name=tag_type,
                sent_for_rule_addition=False,
                rows_count=len(df),
                rows_added=self.get_rows_added_count(tag_name, tag_type),
                rows_removed=self.get_rows_removed_count(tag_name, tag_type),
                modified=True
            )
            
            if version_id:
                # Store with format tag_name: version_id (changed from version_str: version_id)
                new_temp_versions.append({
                    tag_name: version_id
                })
            
            version_counter += 1

        # Handle untagged data
        if untagged_key in self.dfs:  # Remove the empty check to handle even if it becomes empty
            version_str = f"v4.{version_counter}"
            
            ext = self.get_file_extension()
            filename = f"{project_name}_original_preprocessed_updated_column_names_tags_Untagged_temp{ext}"
            filepath = os.path.join(project_folder, filename)

            # Save the file (even if empty)
            self.save_dataframe(self.dfs[untagged_key], filepath, ext)
            
            # Calculate actual changes
            current_count = len(self.dfs[untagged_key])
            total_ejected = sum(result["ejected_rows"] for result in self.ejection_results)
            total_included_from_untagged = sum(result["added_rows"] for result in self.inclusion_results)
            
            # Net change calculation
            net_added = total_ejected  # Rows ejected TO untagged
            net_removed = total_included_from_untagged  # Rows included FROM untagged
            
            # Create version entry
            version_id = self.version_model.create_version(
                project_id=str(self.project["_id"]),
                description="Temporary untagged data after rules",
                files_path=filepath,
                version_number=4.0 + (version_counter * 0.1),
                tag_name="untagged",
                tag_type_name="unknown",
                sent_for_rule_addition=False,
                rows_count=current_count,
                rows_added=net_added,
                rows_removed=net_removed,
                modified=(net_added > 0 or net_removed > 0)  # Modified if any changes
            )
            
            if version_id:
                # Use "untagged" as the key
                new_temp_versions.append({
                    "untagged": version_id
                })
            
            version_counter += 1

        # Process remaining files (not modified by rules)
        for version_str, version_id in self.project.get("split_with_tags", {}).items():
            version_obj = self.version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version_obj or not version_obj.get("sent_for_rule_addition", False):
                continue
                
            tag_name = version_obj.get("tag_name", "").strip().lower()
            tag_type = version_obj.get("tag_type_name", "").strip().lower()
            key = f"{tag_name}_{tag_type}"
            
            # Skip if already processed or is untagged
            if key in processed_keys or key == untagged_key:
                continue
                
            # Load the dataframe if not already loaded
            if key not in self.dfs:
                _, df = self.load_version_data(version_id)
                if df is None:
                    continue
                self.dfs[key] = df
            
            version_str = f"v4.{version_counter}"
            
            ext = os.path.splitext(version_obj.get("files_path", ""))[1]
            # Convert tag_name to proper case for filename
            tag_name_for_file = tag_name.capitalize() if tag_name.lower() != "untagged" else "Untagged"
            filename = f"{project_name}_original_preprocessed_updated_column_names_tags_{tag_name_for_file}_temp{ext}"
            filepath = os.path.join(project_folder, filename)
            
            # Save the file - no modifications were made to this file
            self.save_dataframe(self.dfs[key], filepath, ext)
            
            # Create version entry
            version_id = self.version_model.create_version(
                project_id=str(self.project["_id"]),
                description=f"Temporary version for {tag_name} - {tag_type} (no rules applied)",
                files_path=filepath,
                version_number=4.0 + (version_counter * 0.1),
                tag_name=tag_name,
                tag_type_name=tag_type,
                sent_for_rule_addition=False,
                rows_count=len(self.dfs[key]),
                rows_added=0,
                rows_removed=0,
                modified=False
            )
            
            if version_id:
                # Use tag_name as the key
                new_temp_versions.append({
                    tag_name: version_id
                })
            
            version_counter += 1
        
        # Add new temp versions to project
        for temp_version in new_temp_versions:
            self.project_model.append_temp_file(
                str(self.project["_id"]),
                temp_version
            )

        return new_temp_versions

    def get_rows_added_count(self, tag_name, tag_type):
        """Get count of rows added to this tag from inclusion rules"""
        return sum(
            result["added_rows"] 
            for result in self.inclusion_results 
            if result["tag_name"].lower() == tag_name.lower() and 
            result["tag_type"].lower() == tag_type.lower()
        )

    def get_rows_removed_count(self, tag_name, tag_type):
        """Get count of rows removed from this tag from ejection rules"""
        return sum(
            result["ejected_rows"] 
            for result in self.ejection_results 
            if result["tag_name"].lower() == tag_name.lower() and 
            result["tag_type"].lower() == tag_type.lower()
        )

    def get_file_extension(self):
        """Get file extension from existing files"""
        if self.version_map:
            sample_path = next(iter(self.version_map.values()))["file_path"]
            return os.path.splitext(sample_path)[1]
        return ".csv"

    def save_dataframe(self, df, filepath, ext):
        """Save dataframe to file with proper date formatting"""
        try:
            # Get date columns from datatype mapping
            date_columns = [col for col in df.columns 
                        if col in self.datatype_mapping 
                        and self.datatype_mapping[col].lower() in ["date", "datetime"]]
            
            # Format date columns
            df_to_save = DateFormatter.format_dataframe_dates(df, date_columns)
            
            if ext.lower() == ".xlsx":
                df_to_save.to_excel(filepath, index=False, engine="openpyxl")
            else:
                df_to_save.to_csv(filepath, index=False, encoding="utf-8")
        except Exception as e:
            logger.error(f"Error saving file {filepath}: {str(e)}")
            raise

    def save_rows_tracking_files(self):
        """Save ejected and injected rows to separate files"""
        project_name = secure_filename(self.project.get("name", f"project_{self.project['_id']}"))
        upload_folder = os.path.join(os.getcwd(), 'datasets')
        project_folder = os.path.join(upload_folder, project_name)
        
        # Clear existing tracking files with detailed logging
        logger.info(f"Starting to clear existing tracking files for project {self.project['_id']}")
        clear_success = self.project_model.clear_rows_tracking_files(str(self.project["_id"]))
        
        if not clear_success:
            logger.error("Failed to clear existing tracking files, but continuing with new file creation")
        
        # Log what we're about to save
        logger.info(f"Ejected rows to save: {list(self.ejected_rows.keys())}")
        logger.info(f"Injected rows to save: {list(self.injected_rows.keys())}")
        
        # Save ejected rows files
        for tag_key, df in self.ejected_rows.items():
            if df.empty:
                logger.info(f"Skipping empty ejected rows for {tag_key}")
                continue
                
            tag_name = tag_key.replace("_ejected", "")
            ext = self.get_file_extension()
            filename = f"{project_name}_rows_removed_{tag_name}{ext}"
            filepath = os.path.join(project_folder, filename)
            
            logger.info(f"Saving ejected rows for {tag_name}: {len(df)} rows to {filepath}")
            self.save_dataframe(df, filepath, ext)
            
            # Create version entry
            version_id = self.version_model.create_version(
                project_id=str(self.project["_id"]),
                description=f"Rows removed from {tag_name}",
                files_path=filepath,
                version_number=5.0,  # Use 5.x for tracking files
                tag_name=tag_name,
                tag_type_name="removed",
                rows_count=len(df)
            )
            
            if version_id:
                self.project_model.append_rows_removed_file(
                    str(self.project["_id"]),
                    {tag_name: version_id}
                )
                logger.info(f"Created version {version_id} for removed rows from {tag_name}")
        
        # Save injected rows files
        for tag_key, df in self.injected_rows.items():
            if df.empty:
                logger.info(f"Skipping empty injected rows for {tag_key}")
                continue
                
            # Extract tag name
            parts = tag_key.replace("_injected", "").split("_")
            tag_name = parts[0] if parts else "unknown"
            
            ext = self.get_file_extension()
            filename = f"{project_name}_rows_added_{tag_name}{ext}"
            filepath = os.path.join(project_folder, filename)
            
            logger.info(f"Saving injected rows for {tag_name}: {len(df)} rows to {filepath}")
            self.save_dataframe(df, filepath, ext)
            
            # Create version entry
            version_id = self.version_model.create_version(
                project_id=str(self.project["_id"]),
                description=f"Rows added to {tag_name}",
                files_path=filepath,
                version_number=5.1,  # Use 5.x for tracking files
                tag_name=tag_name,
                tag_type_name="added",
                rows_count=len(df)
            )
            
            if version_id:
                self.project_model.append_rows_added_file(
                    str(self.project["_id"]),
                    {tag_name: version_id}
                )
                logger.info(f"Created version {version_id} for added rows to {tag_name}")
        
        logger.info("Completed saving tracking files")

    def apply_rules(self):
        """Main function to orchestrate the rule application process"""
        try:
            # Step 1: Load all versions and create dataframes
            self.load_versions()
            
            # Step 2: Apply ejection rules first
            self.apply_ejection_rules()
            
            # Step 3: Then apply inclusion rules
            self.apply_inclusion_rules()
            
            # Step 4: Save new versions to temp_files
            new_temp_versions = self.save_new_versions()
            
            # Step 5: Save tracking files
            self.save_rows_tracking_files()
            
            return {
                "ejection_results": self.ejection_results,
                "inclusion_results": self.inclusion_results,
                "new_versions": new_temp_versions
            }
                        
        except Exception as e:
            logger.error(f"Error in apply_rules: {str(e)}")
            raise