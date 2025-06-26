from app.blueprints.dataset import dataset_bp
from app.models.project_model import ProjectModel
from app.models.version_model import VersionModel
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps
from app.models.user_model import UserModel
from app.models.rules_book_debt_model import RulesBookDebtModel
import os
from werkzeug.utils import secure_filename
from flask import request, jsonify, send_file
import pandas as pd
from bson import ObjectId
from datetime import datetime
from app.utils.column_names import (DEBTSHEET_LOAN_AMOUNT, DEBTSHEET_TAG_NAME, DEBTSHEET_TAG_TYPE, TRANSACTION_LOAN_AMOUNT)

# Initialize models
project_model = ProjectModel()
user_model = UserModel()


UPLOAD_FOLDER = os.path.join(os.getcwd(), 'datasets')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def save_file(file, filename, project_name):
    """Save uploaded file to a project-specific folder in the datasets directory
    
    Args:
        file: File object from request
        filename: Desired filename
        project_name: Name of the project (used as folder name)
        
    Returns:
        tuple: (bool, str, str) - (success status, file path or error message, base folder path)
    """
    try:
        # Secure the project name and filename
        secure_project_name = secure_filename(project_name)
        
        # Create project-specific folder path
        project_folder = os.path.join(UPLOAD_FOLDER, secure_project_name)
        
        # Check if project folder already exists
        if os.path.exists(project_folder):
            return False, "A project with this name already exists. Please choose a different project name.", None
        
        # Create project folder
        os.makedirs(project_folder)
        
        # Get file extension
        _, ext = os.path.splitext(filename)
        
        # Create filename with _original suffix
        secure_name = secure_filename(f"{project_name}_original{ext}")
        
        # Create file path within project folder
        file_path = os.path.join(project_folder, secure_name)
        
        # Save the file
        file.save(file_path)
        return True, file_path, project_folder
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        return False, "Error saving file", None
    
@dataset_bp.route('/get_column_names', methods=['GET'])
def get_column_names():
    """Get column names from the uploaded dataset file
    
    Returns:
        JSON response with column names or error message
    """
    project_id = request.args.get('project_id')
    project = project_model.get_project(project_id)
    
    if not project:
        return jsonify({"error": "Project not found"}), 404
    
    # Get the appropriate version based on priority
    version_model = VersionModel()
    
    # Priority order: datatype conversion done > column rename done > preprocessed > base
    if project.get('file_with_both_renaming_and_datatype_conversion_done'):
        version_id = project['file_with_both_renaming_and_datatype_conversion_done']
    elif project.get('file_with_only_renaming_done'):
        version_id = project['file_with_only_renaming_done']
    elif project.get('dataset_after_preprocessing'):
        version_id = project['dataset_after_preprocessing']
    elif project.get('base_file'):
        version_id = project['base_file']
    else:
        return jsonify({"error": "No file associated with project"}), 404
        
    # Fetch version details
    version = version_model.collection.find_one({"_id": ObjectId(version_id)})
    if not version:
        return jsonify({"error": "Version not found"}), 404
        
    file_path = version.get('files_path')
    
    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404
    
    try:
        # Read the dataset and extract column names
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str, nrows=1)  # Read only the first row
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, nrows=1)  # Read only the first row
        else:
            return jsonify({"error": "Unsupported file format"}), 400

        # Extract column names
        column_names = df.columns.tolist()

        return jsonify({"column_names": column_names}), 200
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}")
        return jsonify({"error": "Error reading file", "details": str(e)}), 500

@dataset_bp.route('/update_column_names', methods=['POST'])
def update_column_names():
    """
    Update column names in the dataset file based on the provided mapping.
    This now only handles column renaming, not datatype conversion.
    
    Form Data:
        - project_id: The ID of the project.
        - mapped_columns: A nested dictionary containing old column names as keys and new column names as values.
    
    Returns:
        JSON response with success message or error details.
    """
    try:
        # Parse form data
        project_id = request.form.get("project_id")
        mapped_columns = request.form.get("mapped_columns")

        if not project_id or not mapped_columns:
            return jsonify({"error": "Missing required fields: project_id or mapped_columns"}), 400

        # Convert mapped_columns from string to dictionary
        try:
            import json
            column_mapping = json.loads(mapped_columns)
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid format for mapped_columns"}), 400

        # Filter out mappings where the new column name is an empty string
        filtered_mapping = {old: new for old, new in column_mapping.items() if new.strip()}

        # Step 1: Fetch the project details
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        # Get the dataset_after_preprocessing version
        if not project.get("dataset_after_preprocessing"):
            return jsonify({"error": "No preprocessed dataset found"}), 404
            
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(project["dataset_after_preprocessing"])})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404

        # Step 2: Load the dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500

        # Step 3: Update column names
        df.rename(columns=filtered_mapping, inplace=True)
        
        # Step 4: Drop columns that were not renamed (not in the mapping)
        columns_to_keep = list(filtered_mapping.values())
        df = df[columns_to_keep]

        # Step 5: Save the renamed file
        project_folder = os.path.dirname(file_path)
        _, ext = os.path.splitext(file_path)
        rename_filename = f"{project['name'].replace(' ', '_')}_original_preprocessed_updated_column_names{ext}"
        rename_file_path = os.path.join(project_folder, rename_filename)

        if ext == ".xlsx":
            df.to_excel(rename_file_path, index=False, engine="openpyxl")
        elif ext == ".csv":
            df.to_csv(rename_file_path, index=False, encoding="utf-8")

        # Step 6: Create version for renamed file
        rename_version_id = version_model.create_version(
            project_id=project_id,
            description="Columns renamed",
            files_path=rename_file_path,
            version_number=2
        )
        if not rename_version_id:
            os.remove(rename_file_path)
            return jsonify({"error": "Failed to create renamed version"}), 500

        # Update project with rename version
        update_fields = {
            "file_with_only_renaming_done": rename_version_id,
            "column_name_updated": True,
            "version_number": 2
        }
        
        project_model.update_all_fields(project_id, update_fields)
        
        # NEW: Mark header mapping as complete and set temp step for datatype conversion
        project_model.update_step_status(project_id, "header_mapping_done", True)
        project_model.update_temp_step_status(project_id, "header_mapping_in_progress", False)
        project_model.update_current_step(project_id, "datatype_conversion")
        
        return jsonify({
            "status": "success",
            "message": "Column names updated successfully",
            "rename_version_id": rename_version_id,
            "column_name_updated": True,
            "next_step": "datatype_conversion"
        }), 200

    except Exception as e:
        logger.error(f"Error in update_column_names: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    
@dataset_bp.route('/partition_by_tags', methods=["POST"])
def partition_by_tags():
    '''
    Partition the latest file from project and create separate versions for each tag, including untagged.
    '''
    try:
        data = request.json
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # Step 1: Fetch the project details
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        # Get the file_with_both_renaming_and_datatype_conversion_done version
        if not project.get('file_with_both_renaming_and_datatype_conversion_done'):
            return jsonify({"error": "Column renamed and datatype converted file not found"}), 404
            
        version_model = VersionModel()
        version = version_model.collection.find_one({
            "_id": ObjectId(project['file_with_both_renaming_and_datatype_conversion_done'])
        })
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404

        tag_column = DEBTSHEET_TAG_NAME
        tag_type_column = DEBTSHEET_TAG_TYPE

        project_name = project.get("name", f"project_{project_id}")

        # Step 2: Load the Dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
                ext = ".xlsx"
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
                ext = ".csv"
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500

        # Step 3: Check if Tags and Tag Type columns exist
        if tag_column not in df.columns:
            return jsonify({"error": f"Tags column not found in file"}), 400
        if tag_type_column not in df.columns:
            return jsonify({"error": f"Tag Type column not found in file"}), 400

        # Step 4: Partition and Save Files
        split_versions = {}
        tag_groups = {}
        project_folder = os.path.dirname(file_path)

        grouped = df.groupby([tag_column, tag_type_column], dropna=False)
        version_counter = 1

        for (tag, tag_type), group in grouped:
            tag_name = str(tag) if pd.notna(tag) and tag != "" else "Untagged"
            tag_type_name = str(tag_type) if pd.notna(tag_type) and tag_type != "" else "Unknown"
            
            # Create filename with new naming convention
            project_name_clean = project_name.replace(' ', '_')
            file_base = f"{project_name_clean}_original_preprocessed_updated_column_names_datatype_converted_tags_{tag_name}{ext}"
            file_save_path = os.path.join(project_folder, file_base)

            # Save the partitioned file
            if ext == ".xlsx":
                group.to_excel(file_save_path, index=False, engine="openpyxl")
            else:
                group.to_csv(file_save_path, index=False, encoding="utf-8")

            # Create a new version for this partition
            version_number = float(f"3.{version_counter}")
            version_number_str = f"v3.{version_counter}"
            
            # Set sent_for_rule_addition based on tag type
            sent_for_rule_addition = False
            if tag_type_name != "Unknown" and tag_name != "Untagged":
                sent_for_rule_addition = False

            version_id = version_model.create_version(
                project_id=project_id,
                description=f"Partitioned by {tag_name} - {tag_type_name}",
                files_path=file_save_path,
                version_number=version_number,
                sent_for_rule_addition=sent_for_rule_addition,
                tag_name=tag_name,
                tag_type_name=tag_type_name,
                rows_count=len(group),
                bdc_multiplier=1  # Set default BDC multiplier to 1
            )
            
            if version_id:
                split_versions[version_number_str] = version_id

            # Update tag_groups info
            tag_groups[f"{tag_name}_{tag_type_name}"] = {
                "entries": int(len(group)),
                "tag_type": tag_type_name
            }
            version_counter += 1

        # Step 5: Update the project with split_with_tags
        project_model.update_split_with_tags(project_id, split_versions)
        project_model.update_all_fields(
            project_id=project_id,
            update_fields={"version_number": 3}
        )
        
        # NEW: Mark split by tags as complete
        project_model.update_step_status(project_id, "split_by_tags_done", True)
        project_model.update_current_step(project_id, "select_tags")

        # Fetch split files info (same as get_split_files_info)
        split_files_info = []
        for version_number_str, version_id in split_versions.items():
            version = version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version:
                continue
                
            split_files_info.append({
                "tag_name": version.get("tag_name", ""),
                "tag_type_name": version.get("tag_type_name", ""),
                "file_path": version.get("files_path", ""),
                "number_of_rows": version.get("rows_count", 0),
                "version_id": str(version_id)
            })

        return jsonify(split_files_info), 200

    except Exception as e:
        logger.error(f"Error in partition_by_tags: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@dataset_bp.route('/get_split_files_info', methods=['GET'])
def get_split_files_info():
    """
    Fetch info about all split files (split_with_tags) for a project.
    Includes the total of the DEBTSHEET_LOAN_AMOUNT column for each file.
    Also fetches all pinned rules for the user.
    """
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # 1. Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        split_with_tags = project.get("split_with_tags", {})
        if not split_with_tags:
            return jsonify({"error": "No split files found"}), 404

        version_model = VersionModel()
        split_files_info = []

        for version_number, version_id in split_with_tags.items():
            version = version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version:
                continue
                
            tag_name = version.get("tag_name", "")
            tag_type_name = version.get("tag_type_name", "")
            file_path = version.get("files_path", "")

            # Count rows and calculate Loan Amount total
            num_rows = 0
            loan_amount_total = 0
            
            try:
                if file_path and os.path.exists(file_path):
                    if file_path.endswith(".xlsx"):
                        df = pd.read_excel(file_path, dtype=str)
                    elif file_path.endswith(".csv"):
                        df = pd.read_csv(file_path, dtype=str)
                    else:
                        df = None
                        
                    if df is not None:
                        num_rows = len(df)
                        
                        # Calculate Loan Amount total
                        if DEBTSHEET_LOAN_AMOUNT in df.columns:
                            # Convert to numeric and sum, ignoring non-numeric values
                            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                            loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                        else:
                            logger.warning(f"'Loan Amount' column not found in file {file_path}")
                            
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {str(e)}")
                num_rows = -1  # Indicate error
                loan_amount_total = 0

            split_files_info.append({
                "tag_name": tag_name,
                "tag_type_name": tag_type_name,
                "file_path": file_path,
                "number_of_rows": num_rows,
                "loan_amount_total": loan_amount_total,
                "version_id": str(version_id),
                "bdc_multiplier": version.get("bdc_multiplier", 1)  # Include bdc_multiplier
            })

        # 2. Fetch pinned rules for the user
        user_id = project.get("user_id")
        pinned_rules = []
        
        if user_id:
            # Import and initialize the RulesBookDebtModel
            from app.models.rules_book_debt_model import RulesBookDebtModel
            rules_model = RulesBookDebtModel()
            
            # Get all pinned rules for the user
            pinned_rules_data = rules_model.get_pinned_rules(user_id)
            
            # Format the pinned rules for response
            for rule in pinned_rules_data:
                pinned_rules.append({
                    "rule_id": rule.get("_id"),
                    "rule_name": rule.get("rule_name"),
                    "tag_name": rule.get("tag_name"),
                    "type_of_rule": rule.get("type_of_rule"),
                    "rules": rule.get("rules", [])
                })

        return jsonify({
            "split_files_info": split_files_info,
            "pinned_rules": pinned_rules
        }), 200

    except Exception as e:
        logger.error(f"Error in get_split_files_info: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@dataset_bp.route('/set_sent_for_rule_addition', methods=['POST'])
def set_sent_for_rule_addition():
    """
    Set sent_for_rule_addition to True for all provided version IDs.
    Expects JSON: { "version_id": ["id1", "id2", ...] }
    """
    try:
        data = request.json
        version_ids = data.get("version_id")
        if not version_ids or not isinstance(version_ids, list):
            return jsonify({"error": "version_id must be a list of IDs"}), 400

        version_model = VersionModel()
        updated_ids = []
        for vid in version_ids:
            result = version_model.collection.update_one(
                {"_id": ObjectId(vid)},
                {"$set": {"sent_for_rule_addition": True}}
            )
            if result.modified_count > 0:
                updated_ids.append(str(vid))
                
        if updated_ids:
            # Get project_id from one of the versions
            version_model = VersionModel()
            sample_version = version_model.collection.find_one({"_id": ObjectId(updated_ids[0])})
            if sample_version:
                project_id = str(sample_version.get("project_id"))
                project_model.update_step_status(project_id, "tags_selected_for_rules", True)
                project_model.update_current_step(project_id, "apply_rules")

        return jsonify({
            "status": "success",
            "updated_version_ids": updated_ids,
            "next_step": "apply_rules"
        }), 200

    except Exception as e:
        logger.error(f"Error in set_sent_for_rule_addition: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

from app.utils.apply_rule import ApplyRule


def clear_existing_temp_files(project_id):
    """Clear all existing temp files for a project before creating new ones
    
    Args:
        project_id (str): ID of the project
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get project to access temp_files
        project = project_model.get_project(project_id)
        if not project or "temp_files" not in project:
            return True  # No temp files to clear
            
        version_model = VersionModel()
        
        # Extract version IDs from temp_files
        version_ids = []
        for temp_file in project.get("temp_files", []):
            for _, version_id in temp_file.items():
                version_ids.append(version_id)
                
        # Delete each version from version collection
        for version_id in version_ids:
            version = version_model.collection.find_one({"_id": ObjectId(version_id)})
            if version:
                # Remove the file if it exists
                file_path = version.get("files_path", "")
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.warning(f"Failed to delete file {file_path}: {str(e)}")
                        
                # Delete version from collection
                version_model.delete_version(version_id)
                
        # Clear temp_files list in project
        result = project_model.collection.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": {"temp_files": []}}
        )
        
        return result.modified_count > 0
        
    except Exception as e:
        logger.error(f"Error clearing temp files: {str(e)}")
        return False

@dataset_bp.route('/apply_rules', methods=['POST'])
def apply_rules():
    """API endpoint for applying rules"""
    try:
        data = request.get_json()
        project_id = data.get("project_id")
        
        # Validate project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        # Clear existing temp files before creating new ones
        clear_existing_temp_files(project_id)

        # Create ApplyRule instance and process rules
        rule_processor = ApplyRule(project, data)
        result = rule_processor.apply_rules()
        
        # NEW: Mark rules applied and set temp step
        project_model.update_step_status(project_id, "rules_applied", True)
        project_model.update_temp_step_status(project_id, "rules_application_in_progress", True)
        project_model.update_current_step(project_id, "review_rules")

        return jsonify({
            "status": "success",
            "ejection_results": result["ejection_results"],
            "inclusion_results": result["inclusion_results"],
            "new_versions": result["new_versions"],
            "next_step": "review_rules"
        }), 200

    except Exception as e:
        logger.error(f"Error in apply_rules: {str(e)}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500
    
@dataset_bp.route('/fetch_data_after_applied_rules', methods=['GET'])
def fetch_data_after_applied_rules():
    """
    Fetch data after rules have been applied to a project.
    Now includes information about rows added and removed files.
    
    Query Parameters:
        - project_id: The ID of the project
        
    Returns:
        JSON response with version details, loan amount totals, and tracking files info
    """
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # 1. Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        # 2. Check if split_with_tags and temp_files exist
        split_with_tags = project.get("split_with_tags", {})
        temp_files = project.get("temp_files", [])
        
        if not split_with_tags or not temp_files:
            return jsonify({"error": "This project is not unlocked"}), 400

        # 3. Process all versions (both temp and original split_with_tags)
        version_model = VersionModel()
        combined_versions = []
        processed_tag_types = set()  # Track tag+type combinations already processed

        # 3.1 Process temp versions first (they take precedence)
        for temp_file in temp_files:
            for tag_name, version_id in temp_file.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if not version:
                    continue
                    
                tag_name_lower = version.get("tag_name", "").lower()
                tag_type = version.get("tag_type_name", "").lower()
                tag_key = f"{tag_name_lower}_{tag_type}"
                processed_tag_types.add(tag_key)
                
                # Process file
                file_info = {
                    "version_id": str(version_id),
                    "version_number": f"v{version.get('version_number', '4.0')}",
                    "tag_name": version.get("tag_name", ""),
                    "tag_type": version.get("tag_type_name", ""),
                    "description": version.get("description", ""),
                    "file_path": version.get("files_path", ""),
                    "rows_count": version.get("rows_count", 0),
                    "rows_added": version.get("rows_added", 0),
                    "rows_removed": version.get("rows_removed", 0),
                    "modified": version.get("modified", False),
                    "from_temp": True,
                    "loan_amount_total": 0,
                    "bdc_multiplier": version.get("bdc_multiplier", 1)
                }
                
                # Load file and calculate loan amount column total
                file_path = version.get("files_path", "")
                if file_path and os.path.exists(file_path):
                    try:
                        if file_path.endswith(".xlsx"):
                            df = pd.read_excel(file_path)
                        elif file_path.endswith(".csv"):
                            df = pd.read_csv(file_path)
                        else:
                            df = None
                            
                        if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                            file_info["loan_amount_total"] = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {str(e)}")
                
                combined_versions.append(file_info)

        # 3.2 Process original split_with_tags (only those not in temp)
        for version_number, version_id in split_with_tags.items():
            version = version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version:
                continue
            
            tag_name = version.get("tag_name", "").lower()
            tag_type = version.get("tag_type_name", "").lower()
            tag_key = f"{tag_name}_{tag_type}"
            
            # Skip if we already processed this tag combination from temp_files
            if tag_key in processed_tag_types:
                continue
                
            # Skip untagged files from original split_with_tags
            if tag_name in ['untagged'] or tag_type in ['unknown']:
                continue
            
            # Process file
            file_info = {
                "version_id": str(version_id),
                "version_number": version_number,
                "tag_name": version.get("tag_name", ""),
                "tag_type": version.get("tag_type_name", ""),
                "description": version.get("description", ""),
                "file_path": version.get("files_path", ""),
                "rows_count": version.get("rows_count", 0),
                "rows_added": 0,
                "rows_removed": 0,
                "modified": False,
                "from_temp": False,
                "loan_amount_total": 0
            }
            
            # Load file and calculate loan amount column total
            file_path = version.get("files_path", "")
            if file_path and os.path.exists(file_path):
                try:
                    if file_path.endswith(".xlsx"):
                        df = pd.read_excel(file_path)
                    elif file_path.endswith(".csv"):
                        df = pd.read_csv(file_path)
                    else:
                        df = None
                        
                    if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                        loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                        file_info["loan_amount_total"] = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")
            
            combined_versions.append(file_info)

        # 4. Get tracking files information
        tracking_info = {
            "rows_added_files": [],
            "rows_removed_files": []
        }
        
        # Process rows added files
        for file_entry in project.get("rows_added_files", []):
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    file_path = version.get("files_path", "")
                    rows_count = version.get("rows_count", 0)
                    
                    # Calculate loan amount total
                    loan_amount_total = 0
                    if file_path and os.path.exists(file_path):
                        try:
                            if file_path.endswith(".xlsx"):
                                df = pd.read_excel(file_path)
                            elif file_path.endswith(".csv"):
                                df = pd.read_csv(file_path)
                            else:
                                df = None
                                
                            if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                                loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                                loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                        except Exception as e:
                            logger.error(f"Error reading tracking file {file_path}: {str(e)}")
                    
                    tracking_info["rows_added_files"].append({
                        "tag_name": tag_name,
                        "version_id": str(version_id),
                        "rows_count": rows_count,
                        "loan_amount_total": loan_amount_total,
                        "file_location": file_path,
                        "description": version.get("description", "")
                    })
                    
        # Process rows removed files
        for file_entry in project.get("rows_removed_files", []):
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    file_path = version.get("files_path", "")
                    rows_count = version.get("rows_count", 0)
                    
                    # Calculate loan amount total
                    loan_amount_total = 0
                    if file_path and os.path.exists(file_path):
                        try:
                            if file_path.endswith(".xlsx"):
                                df = pd.read_excel(file_path)
                            elif file_path.endswith(".csv"):
                                df = pd.read_csv(file_path)
                            else:
                                df = None
                                
                            if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                                loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                                loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                        except Exception as e:
                            logger.error(f"Error reading tracking file {file_path}: {str(e)}")
                    
                    tracking_info["rows_removed_files"].append({
                        "tag_name": tag_name,
                        "version_id": str(version_id),
                        "rows_count": rows_count,
                        "loan_amount_total": loan_amount_total,
                        "file_location": file_path,
                        "description": version.get("description", "")
                    })

        # 5. Return the combined result with tracking info
        return jsonify({
            "status": "success",
            "versions": combined_versions,
            "project_name": project.get("name", ""),
            "tracking_files": tracking_info,
            "summary": {
                "total_rows_added": sum(f["rows_count"] for f in tracking_info["rows_added_files"]),
                "total_rows_removed": sum(f["rows_count"] for f in tracking_info["rows_removed_files"]),
                "total_added_loan_amount": sum(f["loan_amount_total"] for f in tracking_info["rows_added_files"]),
                "total_removed_loan_amount": sum(f["loan_amount_total"] for f in tracking_info["rows_removed_files"])
            }
        }), 200

    except Exception as e:
        logger.error(f"Error in fetch_data_after_applied_rules: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

    
@dataset_bp.route('/finalize_temp_versions', methods=['POST'])
def finalize_temp_versions():
    """
    Transfer temporary versions to final versions in the project.
    This API first removes any existing entries in files_with_rules_applied,
    then takes temp versions, modifies the filenames to replace _temp with _final,
    adds them to files_with_rules_applied, includes versions from split_with_tags
    that aren't in temp_files, and finally combines all files into one.
    
    Request:
        - project_id: The ID of the project
        
    Returns:
        JSON response with status and list of final versions
    """
    try:
        data = request.json
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # 1. Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        # 2. Check if temp_files exist
        temp_files = project.get("temp_files", [])
        split_with_tags = project.get("split_with_tags", {})
        
        if not split_with_tags or not temp_files:
            return jsonify({"error": "This project is not unlocked"}), 400
            
        # 3. First, check and remove any existing entries in files_with_rules_applied
        existing_files_with_rules = project.get("files_with_rules_applied", [])
        if existing_files_with_rules:
            # Get list of version IDs to delete
            version_ids_to_delete = []
            for file_entry in existing_files_with_rules:
                for _, version_id in file_entry.items():
                    version_ids_to_delete.append(version_id)
            
            # Find and delete the actual files
            version_model = VersionModel()
            for version_id in version_ids_to_delete:
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    # Delete the file if it exists
                    file_path = version.get("files_path", "")
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            logger.warning(f"Error removing file {file_path}: {str(e)}")
                    
                    # Delete the version record
                    version_model.delete_version(version_id)
            
            # Clear the files_with_rules_applied array in project
            project_model.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": {"files_with_rules_applied": []}}
            )
            
            # Refetch the project after updating
            project = project_model.get_project(project_id)

        # 4. Process and transfer each temp file
        version_model = VersionModel()
        final_versions = []
        processed_tag_types = set()  # Track tag+type combinations already processed
        all_dataframes = []  # List to store all dataframes for combining
        
        # 4.1 Process temp versions first (they take precedence)
        for temp_file in temp_files:
            for tag_name, version_id in temp_file.items():  # Changed from version_number to tag_name
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if not version:
                    continue
                
                tag_name_lower = version.get("tag_name", "").lower()  # Normalize to lowercase for comparison
                tag_type = version.get("tag_type_name", "").lower()
                tag_key = f"{tag_name_lower}_{tag_type}"
                processed_tag_types.add(tag_key)
                
                # Get file path and create new path with _final instead of _temp
                old_file_path = version.get("files_path", "")
                if not old_file_path or not os.path.exists(old_file_path):
                    continue
                
                # Replace _temp with _final in the file path - handle both _temp. and _temp_
                new_file_path = old_file_path.replace("_temp.", "_final.")
                if "_temp_" in new_file_path:
                    new_file_path = new_file_path.replace("_temp_", "_final_")
                
                # Copy the file with the new name
                try:
                    import shutil
                    shutil.copy2(old_file_path, new_file_path)
                    
                    # Read the file for combining later
                    if new_file_path.endswith(".xlsx"):
                        df = pd.read_excel(new_file_path, dtype=str)
                    elif new_file_path.endswith(".csv"):
                        df = pd.read_csv(new_file_path, dtype=str)
                    else:
                        continue
                    all_dataframes.append(df)
                    
                except Exception as e:
                    logger.error(f"Error copying file from {old_file_path} to {new_file_path}: {str(e)}")
                    continue
                
                # Create a new version record for the final version
                new_version_id = version_model.create_version(
                    project_id=str(project["_id"]),
                    description=version.get("description", "").replace("Temporary", "Final"),
                    files_path=new_file_path,
                    version_number=version.get("version_number", 4.0),
                    tag_name=version.get("tag_name"),
                    tag_type_name=version.get("tag_type_name"),
                    rows_count=version.get("rows_count"),
                    rows_added=version.get("rows_added"),
                    rows_removed=version.get("rows_removed"),
                    modified=version.get("modified"),
                    sent_for_rule_addition=True,
                    bdc_multiplier=version.get("bdc_multiplier", 1)  # Carry forward the bdc_multiplier
                )
                
                if new_version_id:
                    # Add to files_with_rules_applied in project - now using tag_name as key
                    project_model.append_files_with_rules_applied(
                        project_id,
                        {version.get("tag_name", tag_name): new_version_id}
                    )
                    
                    # Add to our response list
                    final_versions.append({
                        "version_id": new_version_id,
                        "tag_name": version.get("tag_name"),
                        "tag_type": version.get("tag_type_name"),
                        "file_path": new_file_path
                    })
                    
                    # Delete the old temp file after successful transfer
                    try:
                        os.remove(old_file_path)
                    except Exception as e:
                        logger.warning(f"Error removing temp file {old_file_path}: {str(e)}")
        
        # 4.2 Process original split_with_tags (only those not in temp)
        for version_number, version_id in split_with_tags.items():
            version = version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version:
                continue
            
            tag_name = version.get("tag_name", "").lower()  # Normalize to lowercase for comparison
            tag_type = version.get("tag_type_name", "").lower()
            tag_key = f"{tag_name}_{tag_type}"
            
            # Skip if we already processed this tag combination from temp_files
            if tag_key in processed_tag_types:
                continue
                
            # Skip untagged files from original split_with_tags
            if tag_name in ['untagged'] or tag_type in ['unknown']:
                continue
            
            # Get file path and create new path with _final suffix
            old_file_path = version.get("files_path", "")
            if not old_file_path or not os.path.exists(old_file_path):
                continue
            
            # Create new file path with _final suffix, matching the naming convention
            file_dir = os.path.dirname(old_file_path)
            file_name = os.path.basename(old_file_path)
            base_name, ext = os.path.splitext(file_name)
            
            # For split_with_tags files, we need to add _final before the extension
            new_file_name = f"{base_name}_final{ext}"
            new_file_path = os.path.join(file_dir, new_file_name)
            
            # Copy the file with the new name
            try:
                import shutil
                shutil.copy2(old_file_path, new_file_path)
                
                # Read the file for combining later
                if new_file_path.endswith(".xlsx"):
                    df = pd.read_excel(new_file_path, dtype=str)
                elif new_file_path.endswith(".csv"):
                    df = pd.read_csv(new_file_path, dtype=str)
                else:
                    continue
                all_dataframes.append(df)
                
            except Exception as e:
                logger.error(f"Error copying file from {old_file_path} to {new_file_path}: {str(e)}")
                continue
            
            # Create a new version record for the final version
            new_version_id = version_model.create_version(
                project_id=str(project["_id"]),
                description=f"Final version of {version.get('tag_name')} - {version.get('tag_type_name')}",
                files_path=new_file_path,
                version_number=float(version_number.replace('v', '')),  # Convert v3.1 to 3.1
                tag_name=version.get("tag_name"),
                tag_type_name=version.get("tag_type_name"),
                rows_count=version.get("rows_count", 0),
                rows_added=0,  # These aren't in temp, so no changes
                rows_removed=0,
                modified=False,
                sent_for_rule_addition=True
            )
            
            if new_version_id:
                # Add to files_with_rules_applied in project - use tag_name as key
                project_model.append_files_with_rules_applied(
                    project_id,
                    {version.get("tag_name"): new_version_id}
                )
                
                # Add to our response list
                final_versions.append({
                    "version_id": new_version_id,
                    "tag_name": version.get("tag_name"),
                    "tag_type": version.get("tag_type_name"),
                    "file_path": new_file_path
                })
        
        # 5. Combine all dataframes and create a combined file
        combined_version_id = None
        if all_dataframes:
            try:
                # Combine all dataframes
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                
                # Create filename for combined file
                project_folder = project.get("base_file_path")
                project_name = project.get("name")
                # Determine file extension based on first file
                ext = ".xlsx" if final_versions[0]["file_path"].endswith(".xlsx") else ".csv"
                combined_filename = f"{project_name}_combined_final{ext}"
                combined_file_path = os.path.join(project_folder, combined_filename)
                
                # Save combined file
                if ext == ".xlsx":
                    combined_df.to_excel(combined_file_path, index=False, engine="openpyxl")
                else:
                    combined_df.to_csv(combined_file_path, index=False, encoding="utf-8")
                
                # Calculate total loan amount for combined file
                total_loan_amount = 0
                if DEBTSHEET_LOAN_AMOUNT in combined_df.columns:
                    total_loan_amount = pd.to_numeric(combined_df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                    total_loan_amount = float(total_loan_amount) if not pd.isna(total_loan_amount) else 0
                
                # Create version for combined file
                combined_version_id = version_model.create_version(
                    project_id=str(project["_id"]),
                    description="Combined final file with all tags",
                    files_path=combined_file_path,
                    version_number=99,  # Use a high version number for combined file
                    tag_name="Combined",
                    tag_type_name="All",
                    rows_count=len(combined_df),
                    total_amount=total_loan_amount,
                    sent_for_rule_addition=False  # Combined file not for rule addition
                )
                
                # Update project with combined file version
                if combined_version_id:
                    project_model.collection.update_one(
                        {"_id": ObjectId(project_id)},
                        {"$set": {"combined_file": combined_version_id}}
                    )
                    
            except Exception as e:
                logger.error(f"Error creating combined file: {str(e)}")
        
        # 6. Clear temp_files from project after transferring all
        if final_versions:
            # Update both temp_files (empty array) and are_all_steps_complete (set to True)
            project_model.collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": {
                    "temp_files": [],
                    "are_all_steps_complete": True
                }}
            )
            
            # NEW: Mark finalized step as complete
            project_model.update_step_status(project_id, "finalized", True)
            project_model.update_temp_step_status(project_id, "rules_application_in_progress", False)
            project_model.update_current_step(project_id, "completed")
        
        return jsonify({
            "status": "success",
            "message": f"Successfully finalized {len(final_versions)} versions",
            "final_versions": final_versions,
            "combined_version_id": combined_version_id,
            "next_step": "completed"
        }), 200

    except Exception as e:
        logger.error(f"Error in finalize_temp_versions: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500
    
@dataset_bp.route('/get_finalized_data', methods=['GET'])
def get_finalized_data():
    """
    Fetch all finalized data from a project after rules have been applied and temp versions
    have been transferred to files_with_rules_applied.
    Now includes tracking files info and all features from fetch_data_after_applied_rules.
    
    Query Parameters:
        - project_id: The ID of the project
        
    Returns:
        JSON response with all finalized version details, tracking files, and summary
    """
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # 1. Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        # 2. Check if files_with_rules_applied exist
        files_with_rules = project.get("files_with_rules_applied", [])
        
        if not files_with_rules:
            return jsonify({"error": "No finalized files found for this project"}), 400

        # 3. Process all finalized versions
        version_model = VersionModel()
        finalized_versions = []
        
        for file_entry in files_with_rules:
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if not version:
                    continue
                
                # Process file info
                file_info = {
                    "version_id": str(version_id),
                    "version_number": f"v{version.get('version_number', '4.0')}",
                    "tag_name": version.get("tag_name", ""),
                    "tag_type": version.get("tag_type_name", ""),
                    "description": version.get("description", ""),
                    "file_path": version.get("files_path", ""),
                    "rows_count": version.get("rows_count", 0),
                    "rows_added": version.get("rows_added", 0),
                    "rows_removed": version.get("rows_removed", 0),
                    "modified": version.get("modified", False),
                    "from_temp": True,  # These are finalized versions from temp
                    "loan_amount_total": 0,
                    "bdc_multiplier": version.get("bdc_multiplier", 1)  # Include BDC multiplier
                }
                
                # Load file and calculate loan amount column total
                file_path = version.get("files_path", "")
                if file_path and os.path.exists(file_path):
                    try:
                        if file_path.endswith(".xlsx"):
                            df = pd.read_excel(file_path)
                        elif file_path.endswith(".csv"):
                            df = pd.read_csv(file_path)
                        else:
                            df = None
                            
                        if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                            file_info["loan_amount_total"] = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {str(e)}")
                
                finalized_versions.append(file_info)

        # 4. Get combined file information if exists
        combined_file_info = None
        if project.get("combined_file"):
            combined_version = version_model.collection.find_one({"_id": ObjectId(project["combined_file"])})
            if combined_version:
                file_path = combined_version.get("files_path", "")
                combined_file_info = {
                    "version_id": str(project["combined_file"]),
                    "file_path": file_path,
                    "rows_count": combined_version.get("rows_count", 0),
                    "total_amount": combined_version.get("total_amount", 0),
                    "description": combined_version.get("description", ""),
                    "loan_amount_total": 0
                }
                
                # Calculate loan amount total for combined file
                if file_path and os.path.exists(file_path):
                    try:
                        if file_path.endswith(".xlsx"):
                            df = pd.read_excel(file_path)
                        elif file_path.endswith(".csv"):
                            df = pd.read_csv(file_path)
                        else:
                            df = None
                            
                        if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                            combined_file_info["loan_amount_total"] = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                    except Exception as e:
                        logger.error(f"Error processing combined file {file_path}: {str(e)}")

        # 5. Get tracking files information
        tracking_info = {
            "rows_added_files": [],
            "rows_removed_files": []
        }
        
        # Process rows added files
        for file_entry in project.get("rows_added_files", []):
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    file_path = version.get("files_path", "")
                    rows_count = version.get("rows_count", 0)
                    
                    # Calculate loan amount total
                    loan_amount_total = 0
                    if file_path and os.path.exists(file_path):
                        try:
                            if file_path.endswith(".xlsx"):
                                df = pd.read_excel(file_path)
                            elif file_path.endswith(".csv"):
                                df = pd.read_csv(file_path)
                            else:
                                df = None
                                
                            if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                                loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                                loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                        except Exception as e:
                            logger.error(f"Error reading tracking file {file_path}: {str(e)}")
                    
                    tracking_info["rows_added_files"].append({
                        "tag_name": tag_name,
                        "version_id": str(version_id),
                        "rows_count": rows_count,
                        "loan_amount_total": loan_amount_total,
                        "file_location": file_path,
                        "description": version.get("description", "")
                    })
                    
        # Process rows removed files
        for file_entry in project.get("rows_removed_files", []):
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    file_path = version.get("files_path", "")
                    rows_count = version.get("rows_count", 0)
                    
                    # Calculate loan amount total
                    loan_amount_total = 0
                    if file_path and os.path.exists(file_path):
                        try:
                            if file_path.endswith(".xlsx"):
                                df = pd.read_excel(file_path)
                            elif file_path.endswith(".csv"):
                                df = pd.read_csv(file_path)
                            else:
                                df = None
                                
                            if df is not None and DEBTSHEET_LOAN_AMOUNT in df.columns:
                                loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                                loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                        except Exception as e:
                            logger.error(f"Error reading tracking file {file_path}: {str(e)}")
                    
                    tracking_info["rows_removed_files"].append({
                        "tag_name": tag_name,
                        "version_id": str(version_id),
                        "rows_count": rows_count,
                        "loan_amount_total": loan_amount_total,
                        "file_location": file_path,
                        "description": version.get("description", "")
                    })

        # 6. Calculate summary statistics
        summary = {
            "total_rows_added": sum(f["rows_count"] for f in tracking_info["rows_added_files"]),
            "total_rows_removed": sum(f["rows_count"] for f in tracking_info["rows_removed_files"]),
            "total_added_loan_amount": sum(f["loan_amount_total"] for f in tracking_info["rows_added_files"]),
            "total_removed_loan_amount": sum(f["loan_amount_total"] for f in tracking_info["rows_removed_files"]),
            "total_final_rows": sum(v["rows_count"] for v in finalized_versions),
            "total_final_loan_amount": sum(v["loan_amount_total"] for v in finalized_versions)
        }

        # 7. Return the comprehensive result
        return jsonify({
            "status": "success",
            "versions": finalized_versions,
            "combined_file": combined_file_info,
            "project_name": project.get("name", ""),
            "project_id": project_id,
            "tracking_files": tracking_info,
            "summary": summary,
            "is_complete": project.get("are_all_steps_complete", False)
        }), 200

    except Exception as e:
        logger.error(f"Error in get_finalized_data: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500
    
@dataset_bp.route('/check_completion_status', methods=['GET'])
def check_completion_status():
    """
    Check if all steps are completed for a project
    
    Query Parameters:
        - project_id: The ID of the project
        
    Returns:
        JSON response with completion status
    """
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        return jsonify({
            "status": "success",
            "is_complete": project.get("are_all_steps_complete", False),
            "project_name": project.get("name", "")
        }), 200

    except Exception as e:
        logger.error(f"Error in check_completion_status: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500
    
@dataset_bp.route('/update_bdc_multiplier', methods=['POST'])
def update_bdc_multiplier():
    """
    Update the bdc_multiplier value for multiple versions.
    
    Request body:
    {
        "updates": [
            {"version": "version_id_1", "bdc_value": 1.5},
            {"version": "version_id_2", "bdc_value": 2.0},
            {"version": "version_id_3", "bdc_value": 1.2}
        ]
    }
    
    Returns:
        JSON response with status and results for each update
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        updates = data.get('updates')
        
        if not updates or not isinstance(updates, list):
            return jsonify({"error": "Missing or invalid 'updates' field. Expected a list of updates."}), 400
            
        if len(updates) == 0:
            return jsonify({"error": "Updates list is empty"}), 400
        
        # Process each update
        version_model = VersionModel()
        results = []
        successful_updates = 0
        failed_updates = 0
        
        for update in updates:
            # Validate each update item
            if not isinstance(update, dict):
                results.append({
                    "error": "Invalid update format",
                    "success": False
                })
                failed_updates += 1
                continue
                
            version_id = update.get('version')
            bdc_value = update.get('bdc_value')
            
            if not version_id:
                results.append({
                    "version": None,
                    "error": "Missing version ID",
                    "success": False
                })
                failed_updates += 1
                continue
                
            if bdc_value is None:
                results.append({
                    "version": version_id,
                    "error": "Missing bdc_value",
                    "success": False
                })
                failed_updates += 1
                continue
            
            # Validate bdc_value is a number
            try:
                bdc_multiplier = float(bdc_value)
            except (TypeError, ValueError):
                results.append({
                    "version": version_id,
                    "error": "bdc_value must be a number",
                    "success": False
                })
                failed_updates += 1
                continue
            
            # Update the bdc_multiplier for this version
            success = version_model.update_bdc_multiplier(version_id, bdc_multiplier)
            
            if success:
                results.append({
                    "version": version_id,
                    "bdc_multiplier": bdc_multiplier,
                    "success": True
                })
                successful_updates += 1
            else:
                results.append({
                    "version": version_id,
                    "error": "Failed to update in database",
                    "success": False
                })
                failed_updates += 1
        
        # Determine overall status
        if successful_updates == len(updates):
            status_code = 200
            overall_status = "success"
            message = f"All {successful_updates} updates completed successfully"
        elif successful_updates > 0:
            status_code = 207  # Multi-status
            overall_status = "partial_success"
            message = f"{successful_updates} updates succeeded, {failed_updates} failed"
        else:
            status_code = 400
            overall_status = "error"
            message = f"All {failed_updates} updates failed"
        
        return jsonify({
            "status": overall_status,
            "message": message,
            "summary": {
                "total": len(updates),
                "successful": successful_updates,
                "failed": failed_updates
            },
            "results": results
        }), status_code
            
    except Exception as e:
        logger.error(f"Error in update_bdc_multiplier: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@dataset_bp.route('/get_split_files_for_rule_addition', methods=['GET'])
def get_split_files_for_rule_addition():
    """
    Fetch all split files (split_with_tags) that have sent_for_rule_addition set to true.
    Also fetches all pinned rules for the user and filters them based on column availability.
    
    Query Parameters:
        - project_id: The ID of the project
        
    Returns:
        JSON response with split files data where sent_for_rule_addition is true and filtered pinned rules
    """
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "Missing Project ID"}), 400

        # 1. Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        split_with_tags = project.get("split_with_tags", {})
        if not split_with_tags:
            return jsonify({"error": "No split files found"}), 404

        version_model = VersionModel()
        split_files_for_rules = []
        
        # Variable to store column names from the first valid file
        dataset_columns = None

        # 2. Process each split file and check sent_for_rule_addition
        for version_number, version_id in split_with_tags.items():
            version = version_model.collection.find_one({"_id": ObjectId(version_id)})
            if not version:
                continue
            
            # Check if sent_for_rule_addition is true
            if version.get("sent_for_rule_addition", False) != True:
                continue
                
            tag_name = version.get("tag_name", "")
            tag_type_name = version.get("tag_type_name", "")
            file_path = version.get("files_path", "")

            # Count rows and calculate Loan Amount total
            num_rows = 0
            loan_amount_total = 0
            
            try:
                if file_path and os.path.exists(file_path):
                    if file_path.endswith(".xlsx"):
                        df = pd.read_excel(file_path, dtype=str)
                    elif file_path.endswith(".csv"):
                        df = pd.read_csv(file_path, dtype=str)
                    else:
                        df = None
                        
                    if df is not None:
                        num_rows = len(df)
                        
                        # Get column names from the first valid file
                        if dataset_columns is None:
                            dataset_columns = set(df.columns.tolist())
                        
                        # Calculate Loan Amount total
                        if DEBTSHEET_LOAN_AMOUNT in df.columns:
                            # Convert to numeric and sum, ignoring non-numeric values
                            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                            loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                        else:
                            logger.warning(f"'Loan Amount' column not found in file {file_path}")
                            
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {str(e)}")
                num_rows = -1  # Indicate error
                loan_amount_total = 0

            split_files_for_rules.append({
                "version_id": str(version_id),
                "version_number": version_number,
                "tag_name": tag_name,
                "tag_type_name": tag_type_name,
                "file_path": file_path,
                "number_of_rows": num_rows,
                "loan_amount_total": loan_amount_total,
                "sent_for_rule_addition": True,
                "bdc_multiplier": version.get("bdc_multiplier", 1),
                "description": version.get("description", ""),
                "created_at": version.get("created_at"),
                "updated_at": version.get("updated_at")
            })

        # 3. Sort by version number for consistent ordering
        split_files_for_rules.sort(key=lambda x: x["version_number"])

        # 4. Fetch pinned rules for the user and filter based on column availability
        user_id = project.get("user_id")
        pinned_rules = []
        
        if user_id and dataset_columns:
            # Import and initialize the RulesBookDebtModel
            from app.models.rules_book_debt_model import RulesBookDebtModel
            rules_model = RulesBookDebtModel()
            
            # Get all pinned rules for the user
            pinned_rules_data = rules_model.get_pinned_rules(user_id)
            
            # Filter rules based on column availability
            for rule in pinned_rules_data:
                # Check if all columns referenced in the rule exist in the dataset
                rule_valid = True
                columns_not_found = []
                
                for rule_group in rule.get("rules", []):
                    for condition in rule_group:
                        column_name = condition.get("column", "").strip()
                        if column_name and column_name not in dataset_columns:
                            rule_valid = False
                            columns_not_found.append(column_name)
                
                # Only include rules where all columns are present
                if rule_valid:
                    pinned_rules.append({
                        "rule_id": rule.get("_id"),
                        "rule_name": rule.get("rule_name"),
                        "tag_name": rule.get("tag_name"),
                        "type_of_rule": rule.get("type_of_rule"),
                        "rules": rule.get("rules", [])
                    })
                else:
                    logger.info(f"Rule '{rule.get('rule_name')}' excluded - columns not found: {columns_not_found}")

        # Return in the EXACT SAME FORMAT as before
        return jsonify({
            "status": "success",
            "project_name": project.get("name", ""),
            "project_id": project_id,
            "total_files": len(split_files_for_rules),
            "split_files": split_files_for_rules,
            "pinned_rules": pinned_rules
        }), 200

    except Exception as e:
        logger.error(f"Error in get_split_files_for_rule_addition: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500
    

@dataset_bp.route('/disable_rule_addition_for_project', methods=['POST'])
def disable_rule_addition_for_project():
    """
    Set sent_for_rule_addition to False for all versions in split_with_tags.
    
    Request body:
    {
        "project_id": "project_id_here"
    }
    
    Returns:
        JSON response with status and number of versions updated
    """
    try:
        data = request.json
        project_id = data.get('project_id')
        
        print(f"\n=== DEBUG: disable_rule_addition_for_project called ===")
        print(f"Project ID: {project_id}")
        
        if not project_id:
            return jsonify({"error": "Missing project_id"}), 400

        # 1. Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404

        print(f"\nProject found: {project.get('name')}")
        
        version_model = VersionModel()
        updated_count = 0
        skipped_count = 0
        version_details = []

        # 2. Only process versions from split_with_tags
        split_with_tags = project.get("split_with_tags", {})
        
        print(f"\nsplit_with_tags content: {split_with_tags}")
        
        if not split_with_tags:
            return jsonify({
                "status": "success",
                "message": "No split_with_tags versions found in project",
                "project_id": project_id,
                "total_versions_found": 0,
                "versions_updated": 0
            }), 200

        # 3. Update sent_for_rule_addition to False for split_with_tags versions
        for version_number, version_id in split_with_tags.items():
            print(f"\n--- Processing version {version_number}: {version_id} ---")
            
            try:
                # First check if the version exists
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                
                if not version:
                    print(f"Version {version_id} NOT FOUND in database!")
                    skipped_count += 1
                    version_details.append({
                        "version_id": str(version_id),
                        "version_number": version_number,
                        "updated": False,
                        "reason": "Version not found in database"
                    })
                    continue
                
                # Print current version data
                print(f"Version found! Current data:")
                print(f"  - tag_name: {version.get('tag_name')}")
                print(f"  - tag_type_name: {version.get('tag_type_name')}")
                print(f"  - sent_for_rule_addition: {version.get('sent_for_rule_addition', 'FIELD NOT PRESENT')}")
                print(f"  - version_number: {version.get('version_number')}")
                
                # Check current value of sent_for_rule_addition
                current_value = version.get("sent_for_rule_addition")
                
                # Update the version - using $set will create the field if it doesn't exist
                result = version_model.collection.update_one(
                    {"_id": ObjectId(version_id)},
                    {"$set": {
                        "sent_for_rule_addition": False,
                        "updated_at": datetime.now()
                    }}
                )
                
                print(f"Update result - matched: {result.matched_count}, modified: {result.modified_count}")
                
                if result.modified_count > 0:
                    updated_count += 1
                    version_details.append({
                        "version_id": str(version_id),
                        "version_number": version_number,
                        "tag_name": version.get("tag_name", ""),
                        "tag_type": version.get("tag_type_name", ""),
                        "updated": True,
                        "previous_value": current_value
                    })
                    print(f"Successfully updated version {version_id}")
                else:
                    # Version exists but wasn't modified
                    version_details.append({
                        "version_id": str(version_id),
                        "version_number": version_number,
                        "tag_name": version.get("tag_name", ""),
                        "tag_type": version.get("tag_type_name", ""),
                        "updated": False,
                        "reason": f"No modification - current value: {current_value}",
                        "current_value": current_value
                    })
                    print(f"Version {version_id} not modified - already False or same value")
                    
                # Verify the update by fetching again
                updated_version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                print(f"After update - sent_for_rule_addition: {updated_version.get('sent_for_rule_addition')}")
                    
            except Exception as e:
                print(f"ERROR processing version {version_id}: {str(e)}")
                import traceback
                traceback.print_exc()
                skipped_count += 1
                version_details.append({
                    "version_id": str(version_id),
                    "version_number": version_number,
                    "updated": False,
                    "reason": f"Error: {str(e)}"
                })
                continue

        # 4. Return detailed response
        print(f"\n=== FINAL SUMMARY ===")
        print(f"Total versions in split_with_tags: {len(split_with_tags)}")
        print(f"Versions updated: {updated_count}")
        print(f"Versions skipped: {skipped_count}")
        print(f"Version details: {version_details}")
        
        return jsonify({
            "status": "success",
            "message": f"Successfully disabled rule addition for {updated_count} versions",
            "project_id": project_id,
            "total_versions_in_split_with_tags": len(split_with_tags),
            "versions_updated": updated_count,
            "versions_skipped": skipped_count,
            "version_details": version_details
        }), 200

    except Exception as e:
        print(f"ERROR in disable_rule_addition_for_project: {str(e)}")
        logger.error(f"Error in disable_rule_addition_for_project: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@dataset_bp.route('/start_datatype_conversion_temp', methods=['POST'])
def start_datatype_conversion_temp():
    """
    Create a temporary version for datatype conversion process.
    This copies the file_with_only_renaming_done and creates a new version for temporary datatype changes.
    
    Request Body:
    {
        "project_id": "xxx"
    }
    
    Returns:
        JSON response with temporary version ID
    """
    try:
        data = request.get_json()
        project_id = data.get("project_id")
        
        if not project_id:
            return jsonify({"error": "Missing required field: project_id"}), 400
        
        # Get project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
        
        # Get the file_with_only_renaming_done version
        if not project.get("file_with_only_renaming_done"):
            return jsonify({"error": "Column renaming not completed yet"}), 400
        
        version_model = VersionModel()
        source_version = version_model.collection.find_one({"_id": ObjectId(project["file_with_only_renaming_done"])})
        if not source_version:
            return jsonify({"error": "Source version not found"}), 404
        
        source_file_path = source_version.get("files_path")
        if not source_file_path or not os.path.exists(source_file_path):
            return jsonify({"error": "Source file not found"}), 404
        
        # Create a copy of the file
        project_folder = os.path.dirname(source_file_path)
        _, ext = os.path.splitext(source_file_path)
        temp_filename = f"{project['name'].replace(' ', '_')}_temp_datatype_conversion{ext}"
        temp_file_path = os.path.join(project_folder, temp_filename)
        
        # Copy the file
        import shutil
        shutil.copy2(source_file_path, temp_file_path)
        
        # Create version for temp file
        temp_version_id = version_model.create_version(
            project_id=project_id,
            description="Temporary file for datatype conversion",
            files_path=temp_file_path,
            version_number=2.5  # Intermediate version
        )
        
        if not temp_version_id:
            os.remove(temp_file_path)
            return jsonify({"error": "Failed to create temporary version"}), 500
        
        # Update project
        update_fields = {
            "temp_datatype_conversion": temp_version_id
        }
        project_model.update_all_fields(project_id, update_fields)
        
        # NEW: Mark temp step as in progress
        project_model.update_temp_step_status(project_id, "datatype_conversion_in_progress", True)
        
        return jsonify({
            "status": "success",
            "message": "Temporary datatype conversion file created",
            "temp_version_id": temp_version_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in start_datatype_conversion_temp: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

@dataset_bp.route('/get_datatype_conversion_preview', methods=['GET'])
def get_datatype_conversion_preview():
    """
    Get preview data for datatype conversion showing date, numeric, and currency columns
    with sample values and conversion status.
    
    Args:
        project_id (str): ID of the project (query parameter)
        
    Returns:
        JSON response with date columns, numeric columns, and currency columns arrays
    """
    try:
        import re
        import random
        
        project_id = request.args.get('project_id')
        
        if not project_id:
            return jsonify({"error": "Missing project_id parameter"}), 400
        
        # Step 1: Get project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
        
        # Step 2: Get the temp datatype conversion file
        if not project.get('temp_datatype_conversion'):
            return jsonify({"error": "Temporary datatype conversion not started. Please call start_datatype_conversion_temp first"}), 400
            
        version_model = VersionModel()
        version_id = project['temp_datatype_conversion']
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Step 3: Get all system columns
        from app.models.system_column_model import SystemColumnModel
        system_column_model = SystemColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        if not system_columns:
            return jsonify({"error": "No system columns found"}), 404
        
        # Create mappings
        system_column_mapping = {}
        currency_columns_set = set()
        
        # Note: For project datasets, we don't have is_currency field, so we'll need to 
        # identify currency columns by their names or descriptions
        for col in system_columns:
            column_name = col.get("column_name")
            datatype = col.get("datatype")
            
            if column_name and datatype:
                system_column_mapping[column_name] = datatype
                # Identify currency columns by name patterns
                if any(curr in column_name.lower() for curr in ['amount', 'price', 'cost', 'value', 'balance']):
                    currency_columns_set.add(column_name)
        
        # Step 4: Load the dataset with dtype=str to preserve original values
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500
        
        # Make a copy for conversion
        df_converted = df.copy()
        
        # Step 5: Separate columns by datatype
        date_columns = []
        numeric_columns = []
        currency_columns = []
        
        # Get random indices for sampling
        total_rows = len(df)
        sample_size = min(5, total_rows)
        random_indices = random.sample(range(total_rows), sample_size) if total_rows > 0 else []
        
        for col in df.columns:
            if col in system_column_mapping:
                datatype = system_column_mapping[col]
                
                if datatype.lower() == 'date':
                    # Process date columns - no conversion, just sample
                    date_col_data = {
                        "column_name": col,
                        "rows": []
                    }
                    
                    for idx in random_indices:
                        value = df.iloc[idx][col]
                        date_col_data["rows"].append({
                            "row_number": idx,
                            "value": str(value) if value else ""
                        })
                    
                    date_columns.append(date_col_data)
                    
                elif datatype.lower() in ['number', 'decimal']:
                    # Check if it's a currency column
                    if col in currency_columns_set:
                        # Process currency columns
                        currency_col_data = {
                            "column_name": col,
                            "error": False,
                            "is_floating": False,
                            "rows": []
                        }
                        
                        # Convert currency values in the actual dataframe
                        has_error = False
                        has_floating = False
                        has_empty_values = False
                        
                        for i in range(len(df_converted)):
                            value = df_converted.iloc[i][col]
                            
                            if value and str(value).strip():
                                try:
                                    # Clean currency value
                                    cleaned_value = re.sub(r'[^\d.-]', '', str(value))
                                    
                                    # Handle multiple decimal points
                                    if cleaned_value.count('.') > 1:
                                        parts = cleaned_value.split('.')
                                        cleaned_value = parts[0] + '.' + ''.join(parts[1:])
                                    
                                    if cleaned_value and cleaned_value not in ['.', '-', '-.']:
                                        float_value = float(cleaned_value)
                                        
                                        # Check if it's a floating point value
                                        if float_value != int(float_value):
                                            has_floating = True
                                            df_converted.at[i, col] = f"{float_value:.2f}"
                                        else:
                                            df_converted.at[i, col] = f"{float_value:.2f}"
                                    else:
                                        df_converted.at[i, col] = ""
                                        has_empty_values = True
                                except:
                                    has_error = True
                                    df_converted.at[i, col] = ""
                                    has_empty_values = True
                            else:
                                df_converted.at[i, col] = ""
                                has_empty_values = True
                        
                        currency_col_data["error"] = has_error or has_empty_values
                        currency_col_data["is_floating"] = has_floating
                        
                        # Add sample rows from original data
                        for idx in random_indices:
                            value = df.iloc[idx][col]
                            currency_col_data["rows"].append({
                                "row_number": idx,
                                "value": str(value) if value else ""
                            })
                        
                        currency_columns.append(currency_col_data)
                        
                    else:
                        # Process regular numeric columns
                        numeric_col_data = {
                            "column_name": col,
                            "error": False,
                            "is_floating": False,
                            "rows": []
                        }
                        
                        # Convert numeric values in the actual dataframe
                        has_error = False
                        has_floating = False
                        has_empty_values = False
                        
                        for i in range(len(df_converted)):
                            value = df_converted.iloc[i][col]
                            
                            if value and str(value).strip():
                                try:
                                    # Clean numeric value
                                    cleaned_value = re.sub(r'[^\d.-]', '', str(value))
                                    
                                    # Handle multiple decimal points
                                    if cleaned_value.count('.') > 1:
                                        parts = cleaned_value.split('.')
                                        cleaned_value = parts[0] + '.' + ''.join(parts[1:])
                                    
                                    if cleaned_value and cleaned_value not in ['.', '-', '-.']:
                                        float_value = float(cleaned_value)
                                        
                                        # Check if it's a floating point value
                                        if float_value != int(float_value):
                                            has_floating = True
                                            df_converted.at[i, col] = str(float_value)
                                        else:
                                            df_converted.at[i, col] = str(int(float_value))
                                    else:
                                        df_converted.at[i, col] = ""
                                        has_empty_values = True
                                except:
                                    has_error = True
                                    df_converted.at[i, col] = ""
                                    has_empty_values = True
                            else:
                                df_converted.at[i, col] = ""
                                has_empty_values = True
                        
                        numeric_col_data["error"] = has_error or has_empty_values
                        numeric_col_data["is_floating"] = has_floating
                        
                        # Add sample rows from original data
                        for idx in random_indices:
                            value = df.iloc[idx][col]
                            numeric_col_data["rows"].append({
                                "row_number": idx,
                                "value": str(value) if value else ""
                            })
                        
                        numeric_columns.append(numeric_col_data)
        
        # Step 6: Save the converted dataframe (overwrite temp file)
        if file_path.endswith(".xlsx"):
            df_converted.to_excel(file_path, index=False, engine="openpyxl")
        elif file_path.endswith(".csv"):
            df_converted.to_csv(file_path, index=False, encoding="utf-8")
        
        return jsonify({
            "status": "success",
            "version_id": version_id,
            "file_path": file_path,
            "date_columns": date_columns,
            "numeric_columns": numeric_columns,
            "currency_columns": currency_columns
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_datatype_conversion_preview: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": "An unexpected error occurred",
            "details": str(e)
        }), 500

@dataset_bp.route('/get_column_sample_rows', methods=['GET'])
def get_column_sample_rows():
    """
    Get 5 random sample rows from a specific column in a dataset version.
    Now uses temp_datatype_conversion if version_id matches.
    
    Query Parameters:
        version_id (str): ID of the version
        column_name (str): Name of the column to sample
        
    Returns:
        JSON response with 5 random rows from the specified column
    """
    try:
        import random
        
        # Get query parameters
        version_id = request.args.get('version_id')
        column_name = request.args.get('column_name')
        
        # Validate required parameters
        if not version_id:
            return jsonify({
                "status": "error",
                "message": "Missing required parameter: version_id"
            }), 400
            
        if not column_name:
            return jsonify({
                "status": "error",
                "message": "Missing required parameter: column_name"
            }), 400
        
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({
                "status": "error",
                "message": "Version not found"
            }), 404
            
        # Get file path
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({
                "status": "error",
                "message": "File not found"
            }), 404
        
        # Load the dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
            else:
                return jsonify({
                    "status": "error",
                    "message": "Unsupported file format"
                }), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({
                "status": "error",
                "message": "Error reading file",
                "details": str(e)
            }), 500
        
        # Check if column exists
        if column_name not in df.columns:
            return jsonify({
                "status": "error",
                "message": f"Column '{column_name}' not found in dataset"
            }), 404
        
        # Get random 5 rows (or less if dataset is smaller)
        total_rows = len(df)
        sample_size = min(5, total_rows)
        random_indices = random.sample(range(total_rows), sample_size) if total_rows > 0 else []
        
        # Build response with random rows
        sample_rows = []
        for idx in random_indices:
            value = df.iloc[idx][column_name]
            sample_rows.append({
                "row_number": idx,
                "row_value": str(value) if pd.notna(value) else ""
            })
        
        return jsonify({
            "status": "success",
            "column_name": column_name,
            "total_rows": total_rows,
            "sample_rows": sample_rows
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_column_sample_rows: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred",
            "details": str(e)
        }), 500

@dataset_bp.route('/update_date_format', methods=['POST'])
def update_date_format():
    """
    Update date format for a specific column in the dataset.
    
    Request Body:
    {
        "version_id": "xxx",
        "column_name": "dob",
        "current_date_format": "yyyy-mm-dd HH:MM:SS",
        "system_format": "dd/mm/yyyy"
    }
    
    Returns:
        JSON response with success message or error details.
    """
    try:
        from datetime import datetime
        
        data = request.get_json()
        
        # Validate required fields
        version_id = data.get("version_id")
        column_name = data.get("column_name")
        current_date_format = data.get("current_date_format")
        system_format = data.get("system_format", "dd/mm/yyyy")
        
        if not all([version_id, column_name, current_date_format]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Load the dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500
        
        # Check if column exists
        if column_name not in df.columns:
            return jsonify({"error": f"Column '{column_name}' not found"}), 404
        
        # Convert format strings to Python datetime format
        def convert_format_to_python(format_str):
            """Convert date format string to Python datetime format"""
            format_map = {
                'yyyy': '%Y',
                'yy': '%y',
                'mm': '%m',
                'dd': '%d',
                'HH': '%H',
                'MM': '%M',
                'SS': '%S',
                'hh': '%I',  # 12-hour format
                'ss': '%S',
                # Handle uppercase variations
                'YYYY': '%Y',
                'YY': '%y',
                'MM': '%m',
                'DD': '%d',
                # Handle month names
                'MMM': '%b',  # Jan, Feb, etc.
                'MMMM': '%B',  # January, February, etc.
            }
            
            python_format = format_str
            # Sort by length in descending order to avoid partial replacements
            for key in sorted(format_map.keys(), key=len, reverse=True):
                python_format = python_format.replace(key, format_map[key])
            
            return python_format
        
        current_python_format = convert_format_to_python(current_date_format)
        system_python_format = convert_format_to_python(system_format)
        
        # Update date format
        error_count = 0
        error_rows = []
        
        for i in range(len(df)):
            value = df.at[i, column_name]
            
            if value and str(value).strip():
                converted = False
                original_value = str(value).strip()
                
                # Try the provided format first
                try:
                    date_obj = datetime.strptime(original_value, current_python_format)
                    df.at[i, column_name] = date_obj.strftime(system_python_format)
                    converted = True
                except:
                    pass
                
                # If that didn't work, try common datetime formats
                if not converted:
                    common_formats = [
                        '%Y-%m-%d %H:%M:%S',      # 1989-01-02 00:00:00
                        '%Y-%m-%d %H:%M:%S.%f',    # 1989-01-02 00:00:00.000
                        '%Y/%m/%d %H:%M:%S',       # 1989/01/02 00:00:00
                        '%d/%m/%Y %H:%M:%S',       # 02/01/1989 00:00:00
                        '%m/%d/%Y %H:%M:%S',       # 01/02/1989 00:00:00
                        '%Y-%m-%d',                # 1989-01-02
                        '%Y/%m/%d',                # 1989/01/02
                        '%d/%m/%Y',                # 02/01/1989
                        '%m/%d/%Y',                # 01/02/1989
                        '%d-%m-%Y',                # 02-01-1989
                        '%m-%d-%Y',                # 01-02-1989
                        '%Y-%m-%dT%H:%M:%S',       # ISO format
                        '%Y-%m-%dT%H:%M:%S.%f',    # ISO format with microseconds
                    ]
                    
                    for fmt in common_formats:
                        try:
                            date_obj = datetime.strptime(original_value, fmt)
                            df.at[i, column_name] = date_obj.strftime(system_python_format)
                            converted = True
                            break
                        except:
                            continue
                
                if not converted:
                    error_count += 1
                    error_rows.append({"row": i, "value": original_value})
                    logger.warning(f"Error converting date at row {i}: {original_value}")
                    # Keep original value on error
        
        if error_count == len(df):
            return jsonify({
                "error": "Failed to convert any dates. Please check the date format.",
                "error_count": error_count,
                "sample_errors": error_rows[:5]  # Show first 5 error examples
            }), 400
        
        # Save the updated file (overwrite existing)
        try:
            _, ext = os.path.splitext(file_path)
            if ext == ".xlsx":
                df.to_excel(file_path, index=False, engine="openpyxl")
            elif ext == ".csv":
                df.to_csv(file_path, index=False, encoding="utf-8")
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            return jsonify({"error": "Error saving file", "details": str(e)}), 500
        
        response_data = {
            "status": "success",
            "message": f"Date format updated for column '{column_name}'",
            "version_id": version_id,
            "error_count": error_count,
            "success_count": len(df) - error_count
        }
        
        # Add sample errors if there were any
        if error_count > 0:
            response_data["sample_errors"] = error_rows[:5]
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Error in update_date_format: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

@dataset_bp.route('/update_numeric_column', methods=['POST'])
def update_numeric_column():
    """
    Update numeric column by converting to integer and/or applying rounding.
    """
    try:
        import math
        
        data = request.get_json()
        
        # Validate required fields
        version_id = data.get("version_id")
        column_name = data.get("column_name")
        convert_to_int = data.get("convert_to_int", False)
        round_off_using = data.get("round_off_using", None)
        
        if not all([version_id, column_name]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Load the dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500
        
        # Check if column exists
        if column_name not in df.columns:
            return jsonify({"error": f"Column '{column_name}' not found"}), 404
        
        # Update numeric values
        error_count = 0
        empty_count = 0  # Track empty values
        
        for i in range(len(df)):
            value = df.at[i, column_name]
            
            if value and str(value).strip():
                try:
                    # Convert to float first
                    float_value = float(str(value).strip())
                    
                    # Apply rounding if specified
                    if round_off_using:
                        if round_off_using.lower() == "up":
                            float_value = math.ceil(float_value)
                        elif round_off_using.lower() == "down":
                            float_value = math.floor(float_value)
                    
                    # Convert to int if specified
                    if convert_to_int:
                        df.at[i, column_name] = str(int(float_value))
                    else:
                        df.at[i, column_name] = str(float_value)
                        
                except Exception as e:
                    error_count += 1
                    empty_count += 1
                    df.at[i, column_name] = ""  # Set to empty string
                    logger.warning(f"Error converting numeric value at row {i}: {value} - {str(e)}")
            else:
                empty_count += 1
                df.at[i, column_name] = ""
        
        # Check if there are any empty values after conversion
        if empty_count > 0:
            return jsonify({
                "error": f"Error in the format of your dataset in the column {column_name}",
                "empty_count": empty_count,
                "total_rows": len(df)
            }), 400
        
        if error_count == len(df):
            return jsonify({
                "error": "Failed to convert any numeric values",
                "error_count": error_count
            }), 400
        
        # Save the updated file (overwrite existing)
        try:
            _, ext = os.path.splitext(file_path)
            if ext == ".xlsx":
                df.to_excel(file_path, index=False, engine="openpyxl")
            elif ext == ".csv":
                df.to_csv(file_path, index=False, encoding="utf-8")
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            return jsonify({"error": "Error saving file", "details": str(e)}), 500
        
        return jsonify({
            "status": "success",
            "message": f"Numeric column '{column_name}' updated successfully",
            "version_id": version_id,
            "error_count": error_count,
            "success_count": len(df) - error_count
        }), 200
        
    except Exception as e:
        logger.error(f"Error in update_numeric_column: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

@dataset_bp.route('/update_currency_column', methods=['POST'])
def update_currency_column():
    """
    Update currency column by converting to integer and/or applying rounding.
    If whole_number_multiplier is provided, multiply the column by that number and convert to integer.
    """
    try: 
        import math
        import re
        
        data = request.get_json()
        
        # Validate required fields
        version_id = data.get("version_id")
        column_name = data.get("column_name")
        convert_to_int = data.get("convert_to_int", False)
        round_off_using = data.get("round_off_using", None)
        whole_number_multiplier = data.get("whole_number_multiplier", None)  # NEW PARAMETER
        
        if not all([version_id, column_name]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Load the dataset
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({"error": "Error reading file", "details": str(e)}), 500
        
        # Check if column exists
        if column_name not in df.columns:
            return jsonify({"error": f"Column '{column_name}' not found"}), 404
        
        # Update currency values
        error_count = 0
        empty_count = 0  # Track empty values
        
        # NEW LOGIC: If whole_number_multiplier is provided
        if whole_number_multiplier is not None:
            try:
                # Convert multiplier to float to ensure proper multiplication
                multiplier = float(whole_number_multiplier)
                
                for i in range(len(df)):
                    value = df.at[i, column_name]
                    
                    if value and str(value).strip():
                        try:
                            # Clean currency value - remove $, commas, and other non-numeric chars
                            cleaned_value = re.sub(r'[^\d.-]', '', str(value))
                            
                            # Handle multiple decimal points
                            if cleaned_value.count('.') > 1:
                                parts = cleaned_value.split('.')
                                cleaned_value = parts[0] + '.' + ''.join(parts[1:])
                            
                            if cleaned_value and cleaned_value not in ['.', '-', '-.']:
                                # Convert to float
                                float_value = float(cleaned_value)
                                
                                # Multiply by the whole_number_multiplier
                                multiplied_value = float_value * multiplier
                                
                                # Convert to integer
                                df.at[i, column_name] = str(int(multiplied_value))
                            else:
                                error_count += 1
                                empty_count += 1
                                df.at[i, column_name] = ""
                                logger.warning(f"Invalid currency value at row {i}: {value}")
                                
                        except Exception as e:
                            error_count += 1
                            empty_count += 1
                            df.at[i, column_name] = ""
                            logger.warning(f"Error converting currency value at row {i}: {value} - {str(e)}")
                    else:
                        empty_count += 1
                        df.at[i, column_name] = ""
                        
            except (TypeError, ValueError) as e:
                return jsonify({
                    "error": "Invalid whole_number_multiplier value",
                    "details": str(e)
                }), 400
                
        else:
            # EXISTING LOGIC: If whole_number_multiplier is NOT provided
            for i in range(len(df)):
                value = df.at[i, column_name]
                
                if value and str(value).strip():
                    try:
                        # Clean currency value - remove $, commas, and other non-numeric chars
                        cleaned_value = re.sub(r'[^\d.-]', '', str(value))
                        
                        # Handle multiple decimal points
                        if cleaned_value.count('.') > 1:
                            parts = cleaned_value.split('.')
                            cleaned_value = parts[0] + '.' + ''.join(parts[1:])
                        
                        if cleaned_value and cleaned_value not in ['.', '-', '-.']:
                            # Convert to float
                            float_value = float(cleaned_value)
                            
                            # Apply rounding if specified
                            if round_off_using:
                                if round_off_using.lower() == "up":
                                    float_value = math.ceil(float_value)
                                elif round_off_using.lower() == "down":
                                    float_value = math.floor(float_value)
                            
                            # Convert to int if specified
                            if convert_to_int:
                                df.at[i, column_name] = str(int(float_value))
                            else:
                                # Keep as currency format with 2 decimal places
                                df.at[i, column_name] = f"{float_value:.2f}"
                        else:
                            error_count += 1
                            empty_count += 1
                            df.at[i, column_name] = ""
                            logger.warning(f"Invalid currency value at row {i}: {value}")
                            
                    except Exception as e:
                        error_count += 1
                        empty_count += 1
                        df.at[i, column_name] = ""
                        logger.warning(f"Error converting currency value at row {i}: {value} - {str(e)}")
                else:
                    empty_count += 1
                    df.at[i, column_name] = ""
        
        # Check if there are any empty values after conversion
        if empty_count > 0:
            return jsonify({
                "error": f"Error in the format of your dataset in the column {column_name}",
                "empty_count": empty_count,
                "total_rows": len(df)
            }), 400
        
        if error_count == len(df):
            return jsonify({
                "error": "Failed to convert any currency values",
                "error_count": error_count
            }), 400
        
        # Save the updated file (overwrite existing)
        try:
            _, ext = os.path.splitext(file_path)
            if ext == ".xlsx":
                df.to_excel(file_path, index=False, engine="openpyxl")
            elif ext == ".csv":
                df.to_csv(file_path, index=False, encoding="utf-8")
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            return jsonify({"error": "Error saving file", "details": str(e)}), 500
        
        # Prepare response message
        if whole_number_multiplier is not None:
            message = f"Currency column '{column_name}' multiplied by {whole_number_multiplier} and converted to integer successfully"
        else:
            message = f"Currency column '{column_name}' updated successfully"
        
        return jsonify({
            "status": "success",
            "message": message,
            "version_id": version_id,
            "error_count": error_count,
            "success_count": len(df) - error_count,
            "whole_number_multiplier": whole_number_multiplier
        }), 200
        
    except Exception as e:
        logger.error(f"Error in update_currency_column: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

@dataset_bp.route('/after_datatype_conversion_send_temp_to_main', methods=['POST'])
def after_datatype_conversion_send_temp_to_main():
    """
    Move the temporary datatype conversion file to the main changed datatype file.
    This renames the file and creates a new version for the final datatype converted file.
    
    Request Body:
    {
        "project_id": "xxx"
    }
    
    Returns:
        JSON response with new version ID
    """
    try:
        data = request.get_json()
        project_id = data.get("project_id")
        
        if not project_id:
            return jsonify({"error": "Missing required field: project_id"}), 400
        
        # Get project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
        
        # Get the temp file version
        if not project.get("temp_datatype_conversion"):
            return jsonify({"error": "No temporary datatype conversion file found"}), 400
        
        version_model = VersionModel()
        temp_version = version_model.collection.find_one({"_id": ObjectId(project["temp_datatype_conversion"])})
        if not temp_version:
            return jsonify({"error": "Temporary version not found"}), 404
        
        temp_file_path = temp_version.get("files_path")
        if not temp_file_path or not os.path.exists(temp_file_path):
            return jsonify({"error": "Temporary file not found"}), 404
        
        # Create new filename for final datatype converted file
        project_folder = os.path.dirname(temp_file_path)
        _, ext = os.path.splitext(temp_file_path)
        final_filename = f"{project['name'].replace(' ', '_')}_original_preprocessed_updated_column_names_datatype_converted{ext}"
        final_file_path = os.path.join(project_folder, final_filename)
        
        # Rename the file
        os.rename(temp_file_path, final_file_path)
        
        # Create version for final file
        final_version_id = version_model.create_version(
            project_id=project_id,
            description="Columns renamed and datatypes converted",
            files_path=final_file_path,
            version_number=2.8
        )
        
        if not final_version_id:
            # Rename back on error
            os.rename(final_file_path, temp_file_path)
            return jsonify({"error": "Failed to create final version"}), 500
        
        # Update project
        update_fields = {
            "file_with_both_renaming_and_datatype_conversion_done": final_version_id,
            "temp_datatype_conversion": None,  # Clear temp reference
            "version_number": 2.8
        }
        project_model.update_all_fields(project_id, update_fields)
        
        # NEW: Mark datatype conversion as complete
        project_model.update_step_status(project_id, "datatype_conversion_done", True)
        project_model.update_temp_step_status(project_id, "datatype_conversion_in_progress", False)
        project_model.update_step_status(project_id, "data_validation_done", True)  # Auto-mark validation
        project_model.update_current_step(project_id, "split_by_tags")
        
        return jsonify({
            "status": "success",
            "message": "Datatype conversion finalized",
            "final_version_id": final_version_id,
            "next_step": "split_by_tags"
        }), 200
        
    except Exception as e:
        logger.error(f"Error in after_datatype_conversion_send_temp_to_main: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500



@dataset_bp.route('/fetch_temp_file/<version_id>', methods=['GET'])
def fetch_temp_file(version_id):
    """
    Fetch temp file data after rules have been applied.
    
    Args:
        version_id (str): Version ID of the temp file
        
    Returns:
        JSON response with file data or file download
    """
    try:
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
            
        # Option 1: Return file metadata and sample data
        if request.args.get('preview') == 'true':
            # Read file
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
                
            # Calculate loan amount total if exists
            loan_amount_total = 0
            if DEBTSHEET_LOAN_AMOUNT in df.columns:
                loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                
            return jsonify({
                "status": "success",
                "version_id": str(version_id),
                "tag_name": version.get("tag_name"),
                "tag_type": version.get("tag_type_name"),
                "rows_count": len(df),
                "rows_added": version.get("rows_added", 0),
                "rows_removed": version.get("rows_removed", 0),
                "loan_amount_total": loan_amount_total,
                "columns": df.columns.tolist(),
                "sample_data": df.head(10).to_dict(orient="records")
            }), 200
        else:
            # Option 2: Download the file
            return send_file(file_path, as_attachment=True)
            
    except Exception as e:
        logger.error(f"Error in fetch_temp_file: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@dataset_bp.route('/fetch_rows_removed/<project_id>/<tag_name>', methods=['GET'])
def fetch_rows_removed(project_id, tag_name):
    """
    Fetch rows removed file for a specific tag.
    Returns complete dataset in JSON format.
    
    Args:
        project_id (str): Project ID
        tag_name (str): Tag name
        
    Returns:
        JSON response with complete removed rows data
    """
    try:
        # Get project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
            
        # Find the rows removed file for this tag
        rows_removed_files = project.get("rows_removed_files", [])
        version_id = None
        
        for file_entry in rows_removed_files:
            if tag_name in file_entry:
                version_id = file_entry[tag_name]
                break
                
        if not version_id:
            return jsonify({"error": f"No rows removed file found for tag: {tag_name}"}), 404
            
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
            
        # Read file
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        else:
            return jsonify({"error": "Unsupported file format"}), 400
            
        # Calculate loan amount total if exists
        loan_amount_total = 0
        if DEBTSHEET_LOAN_AMOUNT in df.columns:
            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
            loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
            
        # Replace NaN with empty strings
        df = df.fillna('')
            
        return jsonify({
            "status": "success",
            "project_id": project_id,
            "version_id": str(version_id),
            "tag_name": tag_name,
            "type": "rows_removed",
            "file_location": file_path,
            "rows_count": len(df),
            "loan_amount_total": loan_amount_total,
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records")
        }), 200
            
    except Exception as e:
        logger.error(f"Error in fetch_rows_removed: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@dataset_bp.route('/fetch_rows_added/<project_id>/<tag_name>', methods=['GET'])
def fetch_rows_added(project_id, tag_name):
    """
    Fetch rows added file for a specific tag.
    Returns complete dataset in JSON format.
    
    Args:
        project_id (str): Project ID
        tag_name (str): Tag name
        
    Returns:
        JSON response with complete added rows data
    """
    try:
        # Get project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
            
        # Find the rows added file for this tag
        rows_added_files = project.get("rows_added_files", [])
        version_id = None
        
        for file_entry in rows_added_files:
            if tag_name in file_entry:
                version_id = file_entry[tag_name]
                break
                
        if not version_id:
            return jsonify({"error": f"No rows added file found for tag: {tag_name}"}), 404
            
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
            
        # Read file
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        else:
            return jsonify({"error": "Unsupported file format"}), 400
            
        # Calculate loan amount total if exists
        loan_amount_total = 0
        if DEBTSHEET_LOAN_AMOUNT in df.columns:
            loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
            loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
            
        # Replace NaN with empty strings
        df = df.fillna('')
            
        return jsonify({
            "status": "success",
            "project_id": project_id,
            "version_id": str(version_id),
            "tag_name": tag_name,
            "type": "rows_added",
            "file_location": file_path,
            "rows_count": len(df),
            "loan_amount_total": loan_amount_total,
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records")
        }), 200
            
    except Exception as e:
        logger.error(f"Error in fetch_rows_added: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# Additional helper API to get all tracking files info
@dataset_bp.route('/get_rows_tracking_info/<project_id>', methods=['GET'])
def get_rows_tracking_info(project_id):
    """
    Get information about all rows tracking files for a project.
    
    Args:
        project_id (str): Project ID
        
    Returns:
        JSON response with tracking files information
    """
    try:
        # Get project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
            
        tracking_info = {
            "rows_added": [],
            "rows_removed": []
        }
        
        version_model = VersionModel()
        
        # Process rows added files
        for file_entry in project.get("rows_added_files", []):
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    tracking_info["rows_added"].append({
                        "tag_name": tag_name,
                        "version_id": str(version_id),
                        "rows_count": version.get("rows_count", 0),
                        "file_path": version.get("files_path", "")
                    })
                    
        # Process rows removed files
        for file_entry in project.get("rows_removed_files", []):
            for tag_name, version_id in file_entry.items():
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    tracking_info["rows_removed"].append({
                        "tag_name": tag_name,
                        "version_id": str(version_id),
                        "rows_count": version.get("rows_count", 0),
                        "file_path": version.get("files_path", "")
                    })
                    
        return jsonify({
            "status": "success",
            "project_id": project_id,
            "tracking_info": tracking_info
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_rows_tracking_info: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@dataset_bp.route('/get_temp_version_by_tag', methods=['GET'])
def get_temp_version_by_tag():
    """
    Fetch version data and info for a specific tag.
    Priority: Final version (from files_with_rules_applied) > Temp version (from temp_files)
    
    Query Parameters:
        - project_id: The ID of the project
        - tag_name: The tag name to fetch
        - include_data: Optional boolean to include actual data (default: false)
        
    Returns:
        JSON response with version info and optionally the data
    """
    try:
        # Get query parameters
        project_id = request.args.get('project_id')
        tag_name = request.args.get('tag_name')
        include_data = request.args.get('include_data', 'false').lower() == 'true'
        
        # Validate required parameters
        if not project_id:
            return jsonify({"error": "Missing required parameter: project_id"}), 400
            
        if not tag_name:
            return jsonify({"error": "Missing required parameter: tag_name"}), 400
        
        # Fetch project
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
        
        version_id = None
        version_source = None
        
        # 1. First check files_with_rules_applied (final versions)
        files_with_rules = project.get("files_with_rules_applied", [])
        if files_with_rules:
            for file_entry in files_with_rules:
                # Check if tag_name exists in this file entry
                if tag_name.lower() in [k.lower() for k in file_entry.keys()]:
                    # Get the actual key (case-sensitive)
                    actual_key = next(k for k in file_entry.keys() if k.lower() == tag_name.lower())
                    version_id = file_entry[actual_key]
                    version_source = "final"
                    break
        
        # 2. If not found in final, check temp_files
        if not version_id:
            temp_files = project.get("temp_files", [])
            if not temp_files:
                return jsonify({"error": f"No version found for tag: {tag_name}"}), 404
            
            # Find the temp version for the specified tag
            for temp_file in temp_files:
                # Check if tag_name exists in this temp_file entry
                if tag_name.lower() in [k.lower() for k in temp_file.keys()]:
                    # Get the actual key (case-sensitive)
                    actual_key = next(k for k in temp_file.keys() if k.lower() == tag_name.lower())
                    version_id = temp_file[actual_key]
                    version_source = "temp"
                    break
        
        if not version_id:
            return jsonify({"error": f"No version found for tag: {tag_name}"}), 404
        
        # Get version details
        version_model = VersionModel()
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        if not version:
            return jsonify({"error": "Version not found"}), 404
        
        # Get file path
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Prepare response data
        response_data = {
            "status": "success",
            "project_id": project_id,
            "project_name": project.get("name", ""),
            "version_source": version_source,  # Indicates if data is from "final" or "temp"
            "version_info": {
                "version_id": str(version_id),
                "tag_name": version.get("tag_name", ""),
                "tag_type": version.get("tag_type_name", ""),
                "description": version.get("description", ""),
                "file_path": file_path,
                "version_number": version.get("version_number", ""),
                "rows_count": version.get("rows_count", 0),
                "rows_added": version.get("rows_added", 0),
                "rows_removed": version.get("rows_removed", 0),
                "modified": version.get("modified", False),
                "sent_for_rule_addition": version.get("sent_for_rule_addition", False),
                "bdc_multiplier": version.get("bdc_multiplier", 1),
                "created_at": version.get("created_at"),
                "updated_at": version.get("updated_at"),
                "is_final": version_source == "final"  # Flag to indicate if this is final version
            }
        }
        
        # Load file data if requested
        if include_data:
            try:
                # Read the file
                if file_path.endswith(".xlsx"):
                    df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
                elif file_path.endswith(".csv"):
                    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
                else:
                    return jsonify({"error": "Unsupported file format"}), 400
                
                # Calculate loan amount total if column exists
                loan_amount_total = 0
                if DEBTSHEET_LOAN_AMOUNT in df.columns:
                    loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors="coerce").sum()
                    loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                
                # Replace NaN with empty strings
                df = df.fillna('')
                
                # Add data to response
                response_data["file_data"] = {
                    "columns": df.columns.tolist(),
                    "rows_count": len(df),
                    "loan_amount_total": loan_amount_total,
                    "sample_data": df.head(10).to_dict(orient="records"),  # First 10 rows as sample
                    "full_data": df.to_dict(orient="records") if len(df) <= 1000 else None  # Full data only if <= 1000 rows
                }
                
                if len(df) > 1000:
                    response_data["file_data"]["message"] = "Full data not included due to size. Use download endpoint for complete data."
                    
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {str(e)}")
                response_data["file_data"] = {
                    "error": "Failed to read file data",
                    "details": str(e)
                }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Error in get_temp_version_by_tag: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500
    
@dataset_bp.route('/revert_to_split_tags/<project_id>', methods=['POST'])
def revert_to_split_tags(project_id):
    """Revert project back to split tags step, clearing all subsequent data
    
    Args:
        project_id (str): ID of the project
        
    Returns:
        JSON response with status
    """
    try:
        # Clear temp files
        clear_existing_temp_files(project_id)
        
        # Clear rows tracking files
        project_model.clear_rows_tracking_files(project_id)
        
        # Reset steps from split_by_tags_done
        success = project_model.reset_steps_from(project_id, "split_by_tags_done")
        
        if success:
            # Also reset sent_for_rule_addition for all versions
            project = project_model.get_project(project_id)
            if project:
                version_model = VersionModel()
                split_with_tags = project.get("split_with_tags", {})
                
                for version_number, version_id in split_with_tags.items():
                    version_model.collection.update_one(
                        {"_id": ObjectId(version_id)},
                        {"$set": {"sent_for_rule_addition": False}}
                    )
            
            return jsonify({
                'status': 'success',
                'message': 'Reverted to split tags step',
                'next_step': 'select_tags'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to revert to split tags'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in revert_to_split_tags: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500