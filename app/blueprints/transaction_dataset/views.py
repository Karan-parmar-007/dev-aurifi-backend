# transaction_dataset_views.py
from app.blueprints.transaction_dataset import transaction_dataset_bp
from app.models.transaction_model import TransactionModel
from app.models.transaction_version_model import TransactionVersionModel
from app.utils.logger import logger
from flask import request, jsonify
import pandas as pd
from bson import ObjectId
import os
from app.utils.db import db
from datetime import datetime
from app.utils.column_names import (
    DEBTSHEET_LOAN_AMOUNT, 
    DEBTSHEET_TAG_NAME, 
    DEBTSHEET_TAG_TYPE,
    TRANSACTION_LOAN_AMOUNT,
    TRANSACTION_TRANSACTION_ID,
    TRANSACTION_LAST_EMI_DATE,
    TRANSACTION_FIRST_EMI_DATE,
    TRANSACTION_MATURITY_DATE,
    TRANSACTION_DPD,
    TRANSACTION_OVERDUE,
    TRANSACTION_RESTRUCTURED,
    TRANSACTION_RESCHEDULED
)
import json 

# Initialize models
transaction_model = TransactionModel()
transaction_version_model = TransactionVersionModel()

@transaction_dataset_bp.route('/get_column_names', methods=['GET'])
def get_column_names():
    """Get column names from the uploaded transaction dataset file
    
    Returns:
        JSON response with column names or error message
    """
    transaction_id = request.args.get('transaction_id')
    transaction = transaction_model.get_transaction(transaction_id)
    
    if not transaction:
        return jsonify({"error": "Transaction not found"}), 404
    
    # Get the appropriate version based on priority
    # Priority: changed_datatype > column_rename > preprocessed > base
    if transaction.get('changed_datatype_of_column'):
        version_id = transaction['changed_datatype_of_column']
    elif transaction.get('column_rename_file'):
        version_id = transaction['column_rename_file']
    elif transaction.get('preprocessed_file'):
        version_id = transaction['preprocessed_file']
    elif transaction.get('base_file'):
        version_id = transaction['base_file']
    else:
        return jsonify({"error": "No file associated with transaction"}), 404
        
    # Fetch version details
    version = transaction_version_model.get_version(version_id)
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

@transaction_dataset_bp.route('/update_column_names', methods=['POST'])
def update_column_names():
    """
    Update column names in the transaction dataset file based on the provided mapping.
    
    Form Data:
        - transaction_id: The ID of the transaction.
        - mapped_columns: A nested dictionary containing old column names as keys and new column names as values.
    
    Returns:
        JSON response with success message or error details.
    """
    try:
        # Parse form data
        transaction_id = request.form.get("transaction_id")
        mapped_columns = request.form.get("mapped_columns")

        if not transaction_id or not mapped_columns:
            return jsonify({"error": "Missing required fields: transaction_id or mapped_columns"}), 400

        # Convert mapped_columns from string to dictionary
        try:
            import json
            column_mapping = json.loads(mapped_columns)
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid format for mapped_columns"}), 400

        # Filter out mappings where the new column name is an empty string
        filtered_mapping = {old: new for old, new in column_mapping.items() if new.strip()}

        # Step 1: Fetch the transaction details
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404

        # Get the preprocessed_file version
        if not transaction.get("preprocessed_file"):
            return jsonify({"error": "No preprocessed dataset found"}), 404
            
        version = transaction_version_model.get_version(transaction["preprocessed_file"])
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
        transaction_folder = os.path.dirname(file_path)
        _, ext = os.path.splitext(file_path)
        rename_filename = f"{transaction['name'].replace(' ', '_')}_original_preprocessed_updated_column_names{ext}"
        rename_file_path = os.path.join(transaction_folder, rename_filename)

        if ext == ".xlsx":
            df.to_excel(rename_file_path, index=False, engine="openpyxl")
        elif ext == ".csv":
            df.to_csv(rename_file_path, index=False, encoding="utf-8")

        # Step 6: Create version for renamed file
        rename_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Columns renamed",
            files_path=rename_file_path,
            version_number=2
        )
        if not rename_version_id:
            os.remove(rename_file_path)
            return jsonify({"error": "Failed to create renamed version"}), 500

        # Update transaction with rename version
        update_fields = {
            "column_rename_file": rename_version_id,
            "version_number": 2
        }
        
        transaction_model.update_transaction(transaction_id, update_fields)
        
        # NEW: Mark column mapping as complete
        transaction_model.update_step_status(transaction_id, "column_mapping_done", True)
        transaction_model.update_temp_step_status(transaction_id, "column_mapping_in_progress", False)
        transaction_model.update_current_step(transaction_id, "datatype_conversion")


        return jsonify({
            "status": "success",
            "message": "Column names updated successfully",
            "version_id": rename_version_id
        }), 200

    except Exception as e:
        logger.error(f"Error in update_column_names: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

@transaction_dataset_bp.route('/get_datatype_mapping/<transaction_id>', methods=['GET'])
def get_datatype_mapping(transaction_id):
    """
    Fetch the datatype mapping from system transaction columns.
    
    Args:
        transaction_id (str): ID of the transaction
    Returns:
        JSON response with column names and their datatypes from system columns
    """
    try:
        # Validate that the transaction exists
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
        
        # Get all system transaction columns
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        if not system_columns:
            return jsonify({
                'status': 'error',
                'message': 'No system columns found'
            }), 404
        
        # Create the datatype mapping with currency info
        datatype_mapping = {}
        currency_columns = []
        
        for column in system_columns:
            column_name = column.get("column_name")
            datatype = column.get("datatype")
            is_currency = column.get("is_currency", False)
            
            if column_name and datatype:
                datatype_mapping[column_name] = datatype
                
                # Track currency columns separately
                if is_currency:
                    currency_columns.append(column_name)
        
        return jsonify({
            'status': 'success',
            'datatype_mapping': datatype_mapping,
            'currency_columns': currency_columns
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_datatype_mapping: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500
    

@transaction_dataset_bp.route('/start_datatype_conversion_temp', methods=['POST'])
def start_datatype_conversion_temp():
    """
    Create a temporary version for datatype conversion process.
    This copies the column_rename_file and creates a new version for temporary datatype changes.
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    
    Returns:
        JSON response with temporary version ID
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the column_rename_file version
        if not transaction.get("column_rename_file"):
            return jsonify({"error": "Column renaming not completed yet"}), 400
        
        source_version = transaction_version_model.get_version(transaction["column_rename_file"])
        if not source_version:
            return jsonify({"error": "Source version not found"}), 404
        
        source_file_path = source_version.get("files_path")
        if not source_file_path or not os.path.exists(source_file_path):
            return jsonify({"error": "Source file not found"}), 404
        
        # Create a copy of the file
        transaction_folder = os.path.dirname(source_file_path)
        _, ext = os.path.splitext(source_file_path)
        temp_filename = f"{transaction['name'].replace(' ', '_')}_temp_datatype_conversion{ext}"
        temp_file_path = os.path.join(transaction_folder, temp_filename)
        
        # Copy the file
        import shutil
        shutil.copy2(source_file_path, temp_file_path)
        
        # Create version for temp file
        temp_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Temporary file for datatype conversion",
            files_path=temp_file_path,
            version_number=3
        )
        
        if not temp_version_id:
            os.remove(temp_file_path)
            return jsonify({"error": "Failed to create temporary version"}), 500
        
        # Update transaction
        update_fields = {
            "temp_changing_datatype_of_column": temp_version_id
        }
        transaction_model.update_transaction(transaction_id, update_fields)

        transaction_model.update_temp_step_status(transaction_id, "datatype_conversion_in_progress", True)

        
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

@transaction_dataset_bp.route('/add_column_with_rules', methods=['POST'])
def add_column_with_rules():
    """
    Add a new column to the transaction dataset based on rules.
    Updated to handle new rule format with separate boolean value objects.
    """
    try:
        from datetime import datetime
        import re
        
        data = request.get_json()
        
        # Validate required fields
        transaction_id = data.get("transaction_id")
        new_column_name = data.get("newColumnName")
        rules = data.get("rules", [])
        
        if not all([transaction_id, new_column_name, rules]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the temp_new_column_adding version
        if not transaction.get("temp_new_column_adding"):
            return jsonify({"error": "Please start the process of creating new columns first"}), 400
        
        version_id = transaction["temp_new_column_adding"]
        version = transaction_version_model.get_version(version_id)
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
        
        # Get column datatypes from system transaction columns
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        # Create datatype mapping
        column_datatype_map = {}
        if system_columns:
            for col in system_columns:
                col_name = col.get("column_name")
                datatype = col.get("datatype")
                if col_name and datatype:
                    column_datatype_map[col_name] = datatype.lower()
        
        # Initialize the new column with None values
        df[new_column_name] = None
        
        # Variables to track rule application
        current_condition_mask = None
        new_column_datatype = "text"  # Default datatype
        final_boolean_value = None
        is_expecting_boolean = False
        has_calculation = False
        has_condition = False
        prev_connector = None
        
        # Parse rules to understand the expected outcome
        # Check if there's a boolean value specification
        for rule_array in rules:
            if rule_array and isinstance(rule_array, list) and len(rule_array) > 0:
                rule = rule_array[0]
                if rule.get("isBoolean") or rule.get("valueType") == "static" and "booleanValue" in rule:
                    is_expecting_boolean = True
                    final_boolean_value = rule.get("booleanValue", rule.get("boolean", True))
        
        # Process each rule
        processed_rules = []
        for rule_index, rule_array in enumerate(rules):
            if not rule_array or not isinstance(rule_array, list):
                continue
            
            rule = rule_array[0] if len(rule_array) > 0 else {}
            
            # Skip if this is just the boolean value specification
            if rule.get("isBoolean") or (rule.get("valueType") == "static" and "booleanValue" in rule and not rule.get("column_one")):
                continue
            
            # Extract rule components
            column_one = rule.get("column_one") or rule.get("column")
            operator = rule.get("operator", "").lower()
            value = rule.get("value")
            value_type = rule.get("valueType", "static")
            connector = rule.get("connector", "").strip().upper()
            
            # If connector is empty, this is the final rule
            if not connector:
                connector = "FINAL"
            
            # Handle "column being created" reference (case-insensitive)
            if column_one and column_one.lower() == "column being created":
                column_one = new_column_name
            
            # Validate column_one
            if column_one == new_column_name and len(processed_rules) == 0:
                return jsonify({"error": f"Cannot use column being created '{new_column_name}' in the first rule"}), 400
            
            if column_one not in df.columns:
                return jsonify({"error": f"Column '{column_one}' not found in dataset"}), 404
            
            # Get column_one values
            col1_values = df[column_one].copy()
            
            # Get value/column_two based on valueType
            if value_type == "column":
                # Handle "column being created" in value field
                if isinstance(value, str) and value.lower() == "column being created":
                    value = new_column_name
                
                if value == new_column_name and len(processed_rules) == 0:
                    return jsonify({"error": f"Cannot use column being created '{new_column_name}' as value in first rule"}), 400
                
                if value not in df.columns:
                    return jsonify({"error": f"Column '{value}' not found in dataset"}), 404
                col2_values = df[value].copy()
            else:
                # Static value
                col2_values = value
            
            # Determine if this is a calculation or condition based on operator
            is_calculation_operator = operator in ["add", "subtract", "multiply", "divide", "modulo", "power"]
            is_condition_operator = operator in ["equal", "equals", "not equal", "greater than", "less than", 
                                               "greater than or equal", "less than or equal", "contains", "not contains"]
            
            if is_calculation_operator:
                has_calculation = True
            if is_condition_operator:
                has_condition = True
            
            # Store processed rule
            processed_rules.append({
                "operator": operator,
                "is_calculation": is_calculation_operator,
                "is_condition": is_condition_operator,
                "connector": connector
            })
            
            # Apply the rule
            if connector == "THEN" and is_condition_operator:
                # This is a condition that should result in boolean
                new_column_datatype = "boolean"
                col_datatype = column_datatype_map.get(column_one, 'number')
                condition_mask = apply_boolean_condition(col1_values, operator, col2_values, col_datatype)
                
                # Combine with existing condition mask if any
                if current_condition_mask is not None:
                    # Use the previous connector to combine conditions
                    if prev_connector == "OR":
                        final_mask = current_condition_mask | condition_mask
                    else:  # AND
                        final_mask = current_condition_mask & condition_mask
                else:
                    final_mask = condition_mask
                
                # Apply boolean value based on combined condition
                df.loc[final_mask, new_column_name] = str(final_boolean_value if final_boolean_value is not None else True)
                df.loc[~final_mask, new_column_name] = str(not (final_boolean_value if final_boolean_value is not None else True))
                
                # Reset condition mask after THEN
                current_condition_mask = None
                prev_connector = None
                
            elif connector in ["AND", "OR"] and is_condition_operator:
                # Building up conditions
                col_datatype = column_datatype_map.get(column_one, 'number')
                condition_mask = apply_boolean_condition(col1_values, operator, col2_values, col_datatype)
                
                if current_condition_mask is None:
                    current_condition_mask = condition_mask
                else:
                    if prev_connector == "AND":
                        current_condition_mask = current_condition_mask & condition_mask
                    elif prev_connector == "OR":
                        current_condition_mask = current_condition_mask | condition_mask
                
                # Store the connector for the next iteration
                prev_connector = connector
                        
            else:
                # This is a calculation
                result = apply_calculation(col1_values, operator, col2_values)
                
                if current_condition_mask is not None:
                    # Apply only to rows that meet condition
                    df.loc[current_condition_mask, new_column_name] = result[current_condition_mask]
                else:
                    # Apply to all rows
                    df[new_column_name] = result
                
                # Set datatype to number for calculations
                new_column_datatype = "number"
                
                # If this is a calculation with AND/OR, don't reset condition mask
                if connector not in ["AND", "OR"]:
                    current_condition_mask = None
                    prev_connector = None
                else:
                    prev_connector = connector
        
        # Handle case where we have conditions but no THEN
        if current_condition_mask is not None and is_expecting_boolean:
            # Apply the boolean value to the accumulated condition mask
            new_column_datatype = "boolean"
            df.loc[current_condition_mask, new_column_name] = str(final_boolean_value if final_boolean_value is not None else True)
            df.loc[~current_condition_mask, new_column_name] = str(not (final_boolean_value if final_boolean_value is not None else True))
        
        # Validation: Check if we're expecting boolean but only have calculations without conditions
        if is_expecting_boolean and has_calculation and not has_condition:
            return jsonify({
                "error": "Invalid rule configuration: Mathematical operations result in numeric values but boolean output is expected. Please add a condition (e.g., greater than, equal to) to convert the numeric result to boolean."
            }), 400
        
        # If we're expecting boolean but didn't set the datatype, set it now
        if is_expecting_boolean and new_column_datatype != "boolean":
            new_column_datatype = "boolean"
        
        # Convert results to string for consistency
        df[new_column_name] = df[new_column_name].astype(str)
        
        # Replace 'None' with empty string
        df[new_column_name] = df[new_column_name].replace('None', '')
        
        # Save the updated file (overwrite the existing file)
        try:
            if file_path.endswith(".xlsx"):
                df.to_excel(file_path, index=False, engine="openpyxl")
            elif file_path.endswith(".csv"):
                df.to_csv(file_path, index=False, encoding="utf-8")
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            return jsonify({"error": "Error saving file", "details": str(e)}), 500
        
        # Save the new column's datatype to the transaction
        success = transaction_model.add_new_column_datatype(transaction_id, new_column_name, new_column_datatype)
        if not success:
            logger.warning(f"Failed to save datatype for new column '{new_column_name}'")
        
        # Log some debug info
        logger.info(f"Column '{new_column_name}' added with datatype '{new_column_datatype}' and {len(df)} rows")
        logger.info(f"Sample values: {df[new_column_name].head(10).tolist()}")
        
        return jsonify({
            "status": "success",
            "message": f"Column '{new_column_name}' added successfully",
            "version_id": version_id,
            "datatype": new_column_datatype,
            "sample_values": df[new_column_name].head(10).tolist(),
            "rules_processed": len(processed_rules)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in add_column_with_rules: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    

def apply_boolean_condition(col1_values, operator, col2_values, col1_datatype=None):
    """
    Apply a boolean condition and return a mask.
    
    Args:
        col1_values: Series or values from first column
        operator: Operation to perform
        col2_values: Series, static value, or values from second column
        col1_datatype: Datatype of the first column from system columns
        
    Returns:
        Boolean mask series
    """
    try:
        # Convert based on datatype
        if col1_datatype == 'date':
            # Parse dates with the system format dd/mm/yyyy
            col1_numeric = pd.to_datetime(col1_values, format='%d/%m/%Y', errors='coerce')
            
            if isinstance(col2_values, pd.Series):
                col2_numeric = pd.to_datetime(col2_values, format='%d/%m/%Y', errors='coerce')
            else:
                # Static date value
                try:
                    col2_numeric = pd.to_datetime(col2_values, format='%d/%m/%Y')
                except:
                    # Try other common formats if the standard format fails
                    col2_numeric = pd.to_datetime(col2_values, errors='coerce')
        elif col1_datatype in ['text', 'string', None]:  # Handle text/string datatypes
            # Keep as string - no conversion needed
            col1_numeric = col1_values
            col2_numeric = col2_values
        else:
            # Numeric conversion for number/decimal types
            if isinstance(col1_values, pd.Series):
                col1_numeric = pd.to_numeric(col1_values, errors='coerce')
            else:
                col1_numeric = col1_values
            
            if isinstance(col2_values, pd.Series):
                col2_numeric = pd.to_numeric(col2_values, errors='coerce')
            else:
                try:
                    col2_numeric = float(col2_values)
                except:
                    col2_numeric = col2_values
        
        # Apply operator
        if operator == "equal" or operator == "equals":
            if col1_datatype in ['text', 'string', None]:
                # String comparison - case insensitive
                return col1_values.astype(str).str.strip().str.lower() == str(col2_values).strip().lower()
            else:
                return col1_numeric == col2_numeric
        elif operator == "not equal":
            if col1_datatype in ['text', 'string', None]:
                return col1_values.astype(str).str.strip().str.lower() != str(col2_values).strip().lower()
            else:
                return col1_numeric != col2_numeric
        elif operator == "greater than":
            return col1_numeric > col2_numeric
        elif operator == "less than":
            return col1_numeric < col2_numeric
        elif operator == "greater than or equal":
            return col1_numeric >= col2_numeric
        elif operator == "less than or equal":
            return col1_numeric <= col2_numeric
        elif operator == "contains":
            # String operation
            return col1_values.astype(str).str.contains(str(col2_values), case=False, na=False)
        elif operator == "not contains":
            return ~col1_values.astype(str).str.contains(str(col2_values), case=False, na=False)
        else:
            # Unknown operator, return all True
            return pd.Series([True] * len(col1_values))
            
    except Exception as e:
        logger.error(f"Error in apply_boolean_condition: {str(e)}")
        return pd.Series([False] * len(col1_values))


def apply_calculation(col1_values, operator, col2_values):
    """
    Apply a calculation operation and return result.
    
    Args:
        col1_values: Series or values from first column
        operator: Operation to perform
        col2_values: Series, static value, or values from second column
        
    Returns:
        Series with calculated values
    """
    try:
        # Convert to numeric
        if isinstance(col1_values, pd.Series):
            col1_numeric = pd.to_numeric(col1_values, errors='coerce')
        else:
            col1_numeric = col1_values
        
        if isinstance(col2_values, pd.Series):
            col2_numeric = pd.to_numeric(col2_values, errors='coerce')
        else:
            # Static value
            try:
                col2_numeric = float(col2_values)
            except:
                col2_numeric = col2_values
        
        # Apply operator
        if operator == "add":
            return col1_numeric + col2_numeric
        elif operator == "subtract":
            return col1_numeric - col2_numeric
        elif operator == "multiply":
            return col1_numeric * col2_numeric
        elif operator == "divide":
            # Avoid division by zero
            if isinstance(col2_numeric, pd.Series):
                return col1_numeric / col2_numeric.replace(0, pd.NA)
            else:
                if col2_numeric == 0:
                    return pd.Series([pd.NA] * len(col1_numeric))
                return col1_numeric / col2_numeric
        elif operator == "modulo":
            return col1_numeric % col2_numeric
        elif operator == "power":
            return col1_numeric ** col2_numeric
        else:
            # For unrecognized operators, return col1 values
            return col1_numeric
            
    except Exception as e:
        logger.error(f"Error in apply_calculation: {str(e)}")
        return pd.Series([None] * len(col1_values))

def is_date_column(series):
    """Check if a series contains date values."""
    if len(series) == 0:
        return False
    
    # Sample a few non-null values
    sample = series.dropna().head(10)
    if len(sample) == 0:
        return False
    
    # Try to parse as dates
    try:
        pd.to_datetime(sample, errors='coerce')
        # If more than half parsed successfully, likely a date column
        parsed = pd.to_datetime(sample, errors='coerce')
        return parsed.notna().sum() > len(sample) / 2
    except:
        return False


def is_date_string(value):
    """Check if a string value is a date."""
    try:
        pd.to_datetime(value)
        return True
    except:
        return False
    
@transaction_dataset_bp.route('/get_datatype_conversion_preview', methods=['GET'])
def get_datatype_conversion_preview():
    """
    Get preview data for datatype conversion showing date, numeric, and currency columns
    with sample values and conversion status.
    
    Args:
        transaction_id (str): ID of the transaction (query parameter)
        
    Returns:
        JSON response with date columns, numeric columns, and currency columns arrays
    """
    try:
        import re
        import random
        
        transaction_id = request.args.get('transaction_id')
        
        if not transaction_id:
            return jsonify({"error": "Missing transaction_id parameter"}), 400
        
        # Step 1: Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Step 2: Get the temp datatype conversion file
        if not transaction.get('temp_changing_datatype_of_column'):
            return jsonify({"error": "Temporary datatype conversion not started. Please call start_datatype_conversion_temp first"}), 400
            
        version_id = transaction['temp_changing_datatype_of_column']
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Step 3: Get all system transaction columns
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        if not system_columns:
            return jsonify({"error": "No system columns found"}), 404
        
        # Create mappings
        system_column_mapping = {}
        currency_columns_set = set()
        
        for col in system_columns:
            column_name = col.get("column_name")
            datatype = col.get("datatype")
            is_currency = col.get("is_currency", False)
            
            if column_name and datatype:
                system_column_mapping[column_name] = datatype
                if is_currency:
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

@transaction_dataset_bp.route('/get_column_sample_rows', methods=['GET'])
def get_column_sample_rows():
    """
    Get 5 random sample rows from a specific column in a dataset version.
    Now uses temp_changing_datatype_of_column if version_id matches.
    
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
        version = transaction_version_model.get_version(version_id)
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
     
@transaction_dataset_bp.route('/update_date_format', methods=['POST'])
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
        version = transaction_version_model.get_version(version_id)
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


@transaction_dataset_bp.route('/update_numeric_column', methods=['POST'])
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
        version = transaction_version_model.get_version(version_id)
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


@transaction_dataset_bp.route('/update_currency_column', methods=['POST'])
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
        version = transaction_version_model.get_version(version_id)
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



@transaction_dataset_bp.route('/after_datatype_conversion_send_temp_to_main', methods=['POST'])
def after_datatype_conversion_send_temp_to_main():
    """
    Move the temporary datatype conversion file to the main changed datatype file.
    This renames the file and creates a new version for the final datatype converted file.
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    
    Returns:
        JSON response with new version ID
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the temp file version
        if not transaction.get("temp_changing_datatype_of_column"):
            return jsonify({"error": "No temporary datatype conversion file found"}), 400
        
        temp_version = transaction_version_model.get_version(transaction["temp_changing_datatype_of_column"])
        if not temp_version:
            return jsonify({"error": "Temporary version not found"}), 404
        
        temp_file_path = temp_version.get("files_path")
        if not temp_file_path or not os.path.exists(temp_file_path):
            return jsonify({"error": "Temporary file not found"}), 404
        
        # Create new filename for final datatype converted file
        transaction_folder = os.path.dirname(temp_file_path)
        _, ext = os.path.splitext(temp_file_path)
        final_filename = f"{transaction['name'].replace(' ', '_')}_original_preprocessed_updated_column_names_datatype_converted{ext}"
        final_file_path = os.path.join(transaction_folder, final_filename)
        
        # Rename the file
        os.rename(temp_file_path, final_file_path)
        
        # Create version for final file
        final_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Datatype conversion completed",
            files_path=final_file_path,
            version_number=4
        )
        
        if not final_version_id:
            # Rename back on error
            os.rename(final_file_path, temp_file_path)
            return jsonify({"error": "Failed to create final version"}), 500
        
        # Update transaction
        update_fields = {
            "changed_datatype_of_column": final_version_id,
            "temp_changing_datatype_of_column": None,  # Clear temp reference
            "version_number": 4,
            # REMOVED: "are_all_steps_complete": True  # Don't set this here
        }
        transaction_model.update_transaction(transaction_id, update_fields)

        transaction_model.update_step_status(transaction_id, "datatype_conversion_done", True)
        transaction_model.update_temp_step_status(transaction_id, "datatype_conversion_in_progress", False)
        transaction_model.update_current_step(transaction_id, "add_fields")
        
        return jsonify({
            "status": "success",
            "message": "Datatype conversion finalized",
            "final_version_id": final_version_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in after_datatype_conversion_send_temp_to_main: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    

@transaction_dataset_bp.route('/get_final_dataset_preview/<transaction_id>', methods=['GET'])
def get_final_dataset_preview(transaction_id):
    """
    Get first 10 rows of the final datatype converted file.
    
    Args:
        transaction_id (str): ID of the transaction
        
    Returns:
        JSON response with column names and first 10 rows
    """
    try:
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                "status": "error",
                "message": "Transaction not found"
            }), 404
        
        # Get the final datatype converted file
        if not transaction.get("changed_datatype_of_column"):
            return jsonify({
                "status": "error",
                "message": "Datatype conversion not completed yet"
            }), 400
        
        version = transaction_version_model.get_version(transaction["changed_datatype_of_column"])
        if not version:
            return jsonify({
                "status": "error",
                "message": "Version not found"
            }), 404
        
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({
                "status": "error",
                "message": "File not found"
            }), 404
        
        # Read the file
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, nrows=10)
                total_rows = len(pd.read_excel(file_path, dtype=str))
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, nrows=10)
                total_rows = len(pd.read_csv(file_path, dtype=str))
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
        
        # Convert dataframe to list of dictionaries
        df = df.where(pd.notnull(df), '')
        rows = df.to_dict(orient="records")
        
        return jsonify({
            "status": "success",
            "columns": list(df.columns),
            "rows": rows,
            "total_rows": total_rows,
            "version_id": transaction["changed_datatype_of_column"],
            "file_path": file_path
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_final_dataset_preview: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred",
            "details": str(e)
        }), 500
    
@transaction_dataset_bp.route('/start_process_of_creating_new_columns', methods=['POST'])
def start_process_of_creating_new_columns():
    """
    Start the process of creating new columns by creating a temporary copy of the changed_datatype_of_column file.
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    
    Returns:
        JSON response with temporary version ID
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the changed_datatype_of_column version
        if not transaction.get("changed_datatype_of_column"):
            return jsonify({"error": "Datatype conversion not completed yet. Please complete datatype conversion first."}), 400
        
        source_version = transaction_version_model.get_version(transaction["changed_datatype_of_column"])
        if not source_version:
            return jsonify({"error": "Source version not found"}), 404
        
        source_file_path = source_version.get("files_path")
        if not source_file_path or not os.path.exists(source_file_path):
            return jsonify({"error": "Source file not found"}), 404
        
        # Create a copy of the file
        transaction_folder = os.path.dirname(source_file_path)
        _, ext = os.path.splitext(source_file_path)
        temp_filename = f"{transaction['name'].replace(' ', '_')}_temp_new_columns{ext}"
        temp_file_path = os.path.join(transaction_folder, temp_filename)
        
        # Copy the file
        import shutil
        shutil.copy2(source_file_path, temp_file_path)
        
        # Create version for temp file
        temp_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Temporary file for adding new columns",
            files_path=temp_file_path,
            version_number=5
        )
        
        if not temp_version_id:
            os.remove(temp_file_path)
            return jsonify({"error": "Failed to create temporary version"}), 500
        
        # Update transaction
        update_fields = {
            "temp_new_column_adding": temp_version_id
        }
        transaction_model.update_transaction(transaction_id, update_fields)
        
        transaction_model.update_temp_step_status(transaction_id, "new_fields_in_progress", True)


        return jsonify({
            "status": "success",
            "message": "Temporary new column file created",
            "temp_version_id": temp_version_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in start_process_of_creating_new_columns: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    
@transaction_dataset_bp.route('/temp_to_final_adding_new_column', methods=['POST'])
def temp_to_final_adding_new_column():
    """
    Move the temporary new column file to the final added column file.
    This renames the file and creates a new version for the final file with new columns.
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    
    Returns:
        JSON response with new version ID
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the temp file version
        if not transaction.get("temp_new_column_adding"):
            return jsonify({"error": "No temporary new column file found"}), 400
        
        temp_version = transaction_version_model.get_version(transaction["temp_new_column_adding"])
        if not temp_version:
            return jsonify({"error": "Temporary version not found"}), 404
        
        temp_file_path = temp_version.get("files_path")
        if not temp_file_path or not os.path.exists(temp_file_path):
            return jsonify({"error": "Temporary file not found"}), 404
        
        # Create new filename for final file with new columns
        transaction_folder = os.path.dirname(temp_file_path)
        _, ext = os.path.splitext(temp_file_path)
        final_filename = f"{transaction['name'].replace(' ', '_')}_original_preprocessed_updated_column_names_datatype_converted_new_columns_added{ext}"
        final_file_path = os.path.join(transaction_folder, final_filename)
        
        # Rename the file
        os.rename(temp_file_path, final_file_path)
        
        # Create version for final file
        final_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="New columns added to dataset",
            files_path=final_file_path,
            version_number=6
        )
        
        if not final_version_id:
            # Rename back on error
            os.rename(final_file_path, temp_file_path)
            return jsonify({"error": "Failed to create final version"}), 500
        
        # Update transaction
        update_fields = {
            "added_new_column_final": final_version_id,
            "temp_new_column_adding": None,  # Clear temp reference
            "version_number": 6,
            # "are_all_steps_complete": True  # Update this based on your workflow
        }
        transaction_model.update_transaction(transaction_id, update_fields)

        transaction_model.update_step_status(transaction_id, "new_fields_added", True)
        transaction_model.update_temp_step_status(transaction_id, "new_fields_in_progress", False)
        transaction_model.update_current_step(transaction_id, "rbi_rules")
        
        return jsonify({
            "status": "success",
            "message": "New columns finalized",
            "final_version_id": final_version_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in temp_to_final_adding_new_column: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    
@transaction_dataset_bp.route('/get_datatype_mapping_with_new_column_added/<transaction_id>', methods=['GET'])
def get_datatype_mapping_with_new_column_added(transaction_id):
    """
    Fetch the datatype mapping from system transaction columns combined with newly added columns.
    
    Args:
        transaction_id (str): ID of the transaction
    Returns:
        JSON response with column names and their datatypes mapping
    """
    try:
        # Validate that the transaction exists
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
        
        # Get all system transaction columns
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        if not system_columns:
            system_columns = []
        
        # Create the datatype mapping
        datatype_mapping = {}
        
        # Add system columns
        for column in system_columns:
            column_name = column.get("column_name")
            datatype = column.get("datatype")
            
            if column_name and datatype:
                datatype_mapping[column_name] = datatype
        
        # Get newly added columns from the transaction
        new_columns_datatypes = transaction.get("new_added_columns_datatype", {})
        
        # Add newly added columns to the mapping
        for column_name, datatype in new_columns_datatypes.items():
            if column_name and datatype:
                datatype_mapping[column_name] = datatype
        
        return jsonify({
            'status': 'success',
            'datatype_mapping': datatype_mapping
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_datatype_mapping_with_new_column_added: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@transaction_dataset_bp.route('/start_applying_rbi_rules', methods=['POST'])
def start_applying_rbi_rules():
    """
    Start the process of applying RBI rules by creating a temporary copy of the added_new_column_final file.
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    
    Returns:
        JSON response with temporary version ID and initial statistics
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the added_new_column_final version
        if not transaction.get("added_new_column_final"):
            return jsonify({"error": "New columns not added yet. Please complete adding new columns first."}), 400
        
        source_version = transaction_version_model.get_version(transaction["added_new_column_final"])
        if not source_version:
            return jsonify({"error": "Source version not found"}), 404
        
        source_file_path = source_version.get("files_path")
        if not source_file_path or not os.path.exists(source_file_path):
            return jsonify({"error": "Source file not found"}), 404
        
        # Create a copy of the file
        transaction_folder = os.path.dirname(source_file_path)
        _, ext = os.path.splitext(source_file_path)
        temp_filename = f"{transaction['name'].replace(' ', '_')}_temp_rbi_rules{ext}"
        temp_file_path = os.path.join(transaction_folder, temp_filename)
        
        # Copy the file
        import shutil
        shutil.copy2(source_file_path, temp_file_path)
        
        # Initialize RBI rules metadata
        rbi_rules_metadata = {
            "rules_applied": [],
            "total_rows_before": 0,
            "total_rows_after": 0,
            "total_loan_amount_before": 0,
            "total_loan_amount_after": 0
        }
        
        # Read the file to get initial stats
        if temp_file_path.endswith(".xlsx"):
            df = pd.read_excel(temp_file_path, dtype=str, keep_default_na=False)
        elif temp_file_path.endswith(".csv"):
            df = pd.read_csv(temp_file_path, dtype=str, keep_default_na=False)
        
        rbi_rules_metadata["total_rows_before"] = len(df)
        
        # Calculate initial loan amount if column exists
        loan_col = None
        for col in df.columns:
            if col.lower() == TRANSACTION_LOAN_AMOUNT or col.lower() == "loan_amount":
                loan_col = col
                break
                
        if loan_col:
            try:
                df[loan_col] = pd.to_numeric(df[loan_col].str.replace(',', ''), errors='coerce').fillna(0)
                rbi_rules_metadata["total_loan_amount_before"] = float(df[loan_col].sum())
            except:
                rbi_rules_metadata["total_loan_amount_before"] = 0
        
        # Create version for temp file
        temp_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Temporary file for applying RBI rules",
            files_path=temp_file_path,
            version_number=7,
            rbi_rules_metadata=rbi_rules_metadata
        )
        
        if not temp_version_id:
            os.remove(temp_file_path)
            return jsonify({"error": "Failed to create temporary version"}), 500
        
        # Update transaction
        update_fields = {
            "temp_rbi_rules_applied": temp_version_id
        }
        transaction_model.update_transaction(transaction_id, update_fields)

        transaction_model.update_temp_step_status(transaction_id, "rbi_rules_in_progress", True)

        
        return jsonify({
            "status": "success",
            "message": "Temporary RBI rules file created",
            "temp_version_id": temp_version_id,
            "initial_stats": {
                "total_rows": rbi_rules_metadata["total_rows_before"],
                "total_loan_amount": rbi_rules_metadata["total_loan_amount_before"]
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in start_applying_rbi_rules: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500


@transaction_dataset_bp.route('/apply_rbi_rules', methods=['POST'])
def apply_rbi_rules():
    """
    Apply selected RBI rules based on boolean flags.
    
    Request Body:
    {
        "transaction_id": "xxx",
        "rule1": true,  // Remove duplicate transaction IDs
        "rule2": false, // Remove loans not meeting EMI criteria
        "rule3": false, // Remove loans with maturity within 365 days of cutoff
        "rule4": true,  // Remove rows with non-zero overdue/dpd
        "rule5": true,  // Remove restructured/rescheduled loans
        "cutoff_date": "26/03/2024"
    }
    
    Returns:
        JSON response with details of all rules applied
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        cutoff_date = data.get("cutoff_date")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get rule flags with defaults
        apply_rule1 = data.get("rule1", False)
        apply_rule2 = data.get("rule2", False)
        apply_rule3 = data.get("rule3", False)
        apply_rule4 = data.get("rule4", False)
        apply_rule5 = data.get("rule5", False)
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction or not transaction.get("temp_rbi_rules_applied"):
            return jsonify({"error": "RBI rules process not started"}), 400
        
        # Check if rules were already applied to temp file
        temp_version = transaction_version_model.get_version(transaction["temp_rbi_rules_applied"])
        if not temp_version:
            return jsonify({"error": "Temporary version not found"}), 404
        
        # If rules were already applied, restore the original file
        if temp_version.get("rbi_rules_metadata", {}).get("rules_applied"):
            logger.info("Rules already applied, restoring original file")
            
            # Get the source file (added_new_column_final)
            if not transaction.get("added_new_column_final"):
                return jsonify({"error": "Source file not found"}), 400
            
            source_version = transaction_version_model.get_version(transaction["added_new_column_final"])
            if not source_version:
                return jsonify({"error": "Source version not found"}), 404
            
            source_file_path = source_version.get("files_path")
            temp_file_path = temp_version.get("files_path")
            
            if not source_file_path or not os.path.exists(source_file_path):
                return jsonify({"error": "Source file not found"}), 404
            
            # Copy source file to temp file to restore original state
            import shutil
            shutil.copy2(source_file_path, temp_file_path)
            
            # Clear cutoff date from transaction
            transaction_model.update_transaction(transaction_id, {"cutoff_date": None})
            
            # Clear metadata from temp version
            from app.utils.db import db
            db["transaction_versions"].update_one(
                {"_id": ObjectId(temp_version["_id"])},
                {"$set": {"rbi_rules_metadata": {
                    "rules_applied": [],
                    "total_rows_before": 0,
                    "total_rows_after": 0,
                    "total_loan_amount_before": 0,
                    "total_loan_amount_after": 0
                }}}
            )
        
        # Update cutoff date if provided
        if cutoff_date:
            transaction_model.update_cutoff_date(transaction_id, cutoff_date)
        
        # Get fresh version data after potential restoration
        version = transaction_version_model.get_version(transaction["temp_rbi_rules_applied"])
        file_path = version.get("files_path")
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        
        # Load the dataset
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        
        # Reset index to ensure proper alignment
        df = df.reset_index(drop=True)
        
        # Find loan amount column once
        loan_col = None
        for col in df.columns:
            if col.lower() == TRANSACTION_LOAN_AMOUNT or col.lower() == "loan_amount":
                loan_col = col
                break
        
        # Helper function to calculate current loan amount
        def calculate_loan_amount(dataframe):
            if loan_col and len(dataframe) > 0:
                try:
                    return float(pd.to_numeric(
                        dataframe[loan_col].str.replace(',', ''), 
                        errors='coerce'
                    ).fillna(0).sum())
                except:
                    return 0
            return 0
        
        # Initialize metadata with initial stats
        initial_rows = len(df)
        initial_loan_amount = calculate_loan_amount(df)
        
        metadata = {
            "rules_applied": [],
            "total_rows_before": initial_rows,
            "total_rows_after": 0,
            "total_loan_amount_before": initial_loan_amount,
            "total_loan_amount_after": 0,
            "cutoff_date": cutoff_date
        }
        
        rules_results = []
        
        # Rule 1: Remove duplicate transaction IDs
        if apply_rule1:
            rows_before = len(df)
            loan_amount_before = calculate_loan_amount(df)
            
            # Find transaction_id column
            transaction_id_col = None
            for col in df.columns:
                if col.lower() == TRANSACTION_TRANSACTION_ID or col.lower() == "transaction id":
                    transaction_id_col = col
                    break
        
            if transaction_id_col:
                # Get duplicate rows before removing
                duplicate_mask = df.duplicated(subset=[transaction_id_col], keep='first')
                duplicates_df = df[duplicate_mask]
                
                # Calculate loan amount from removed rows
                loan_amount_removed = calculate_loan_amount(duplicates_df)
                
                # Remove duplicates
                df = df.drop_duplicates(subset=[transaction_id_col], keep='first')
                df = df.reset_index(drop=True)
                
                rows_after = len(df)
                loan_amount_after = calculate_loan_amount(df)
                rows_removed = rows_before - rows_after
                
                rules_results.append({
                    "rule_number": 1,
                    "rule_name": "Remove duplicate transaction IDs",
                    "rows_removed": rows_removed,
                    "rows_left": rows_after,
                    "loan_amount_removed": loan_amount_removed,
                    "loan_amount_left": loan_amount_after,
                    "applied": True
                })
            else:
                rules_results.append({
                    "rule_number": 1,
                    "rule_name": "Remove duplicate transaction IDs",
                    "rows_removed": 0,
                    "rows_left": len(df),
                    "loan_amount_removed": 0,
                    "loan_amount_left": calculate_loan_amount(df),
                    "applied": False,
                    "note": "Transaction ID column not found"
                })
                
        # Rule 2: Remove loans not meeting EMI criteria
        if apply_rule2 and cutoff_date:
            rows_before = len(df)
            loan_amount_before = calculate_loan_amount(df)
            
            # Find required columns
            first_emi_col = None
            last_emi_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if TRANSACTION_FIRST_EMI_DATE in col_lower:
                    first_emi_col = col
                elif TRANSACTION_LAST_EMI_DATE in col_lower:
                    last_emi_col = col
            
            if first_emi_col and last_emi_col:
                from datetime import datetime
                cutoff_dt = datetime.strptime(cutoff_date, '%d/%m/%Y')
                
                keep_mask = pd.Series([True] * len(df), index=df.index)
                
                for idx in df.index:
                    try:
                        # Parse first EMI date
                        first_emi_str = df.loc[idx, first_emi_col]
                        if not first_emi_str or str(first_emi_str).strip() == '':
                            keep_mask[idx] = False
                            continue
                            
                        first_emi_dt = pd.to_datetime(first_emi_str, format='%d/%m/%Y', errors='coerce')
                        if pd.isna(first_emi_dt):
                            keep_mask[idx] = False
                            continue
                        
                        # Parse last EMI date
                        last_emi_str = df.loc[idx, last_emi_col]
                        if not last_emi_str or str(last_emi_str).strip() == '':
                            keep_mask[idx] = False
                            continue
                            
                        last_emi_dt = pd.to_datetime(last_emi_str, format='%d/%m/%Y', errors='coerce')
                        if pd.isna(last_emi_dt):
                            keep_mask[idx] = False
                            continue
                        
                        # Calculate loan duration in months from first to last EMI
                        duration_months = (last_emi_dt.year - first_emi_dt.year) * 12 + (last_emi_dt.month - first_emi_dt.month)
                        
                        # Calculate months passed from first EMI to cutoff
                        months_passed = (cutoff_dt.year - first_emi_dt.year) * 12 + (cutoff_dt.month - first_emi_dt.month)
                        
                        # Apply formula: required_months = (duration_years) * 3
                        duration_years = duration_months / 12
                        required_months = duration_years * 3
                        
                        # Remove if required months have not passed
                        if months_passed < required_months:
                            keep_mask[idx] = False
                        
                    except Exception as e:
                        logger.warning(f"Error processing row {idx} for Rule 2: {str(e)}")
                        keep_mask[idx] = False
                
                # Get rows to be removed
                rows_to_remove = df[~keep_mask]
                loan_amount_removed = calculate_loan_amount(rows_to_remove)
                
                # Keep only rows that pass the criteria
                df = df[keep_mask]
                df = df.reset_index(drop=True)
                
                rows_after = len(df)
                loan_amount_after = calculate_loan_amount(df)
                rows_removed = rows_before - rows_after
                
                rules_results.append({
                    "rule_number": 2,
                    "rule_name": "Remove loans not meeting EMI criteria",
                    "rows_removed": rows_removed,
                    "rows_left": rows_after,
                    "loan_amount_removed": loan_amount_removed,
                    "loan_amount_left": loan_amount_after,
                    "applied": True
                })
            else:
                missing_cols = []
                if not first_emi_col:
                    missing_cols.append("First EMI Date")
                if not last_emi_col:  # Fixed: was loan_duration_col
                    missing_cols.append("Last EMI Date")
                    
                rules_results.append({
                    "rule_number": 2,
                    "rule_name": "Remove loans not meeting EMI criteria",
                    "rows_removed": 0,
                    "rows_left": len(df),
                    "loan_amount_removed": 0,
                    "loan_amount_left": calculate_loan_amount(df),
                    "applied": False,
                    "note": f"Required columns not found: {', '.join(missing_cols)}"
                })
        
        # Rule 3: Remove loans with maturity within 365 days of cutoff
        if apply_rule3 and cutoff_date:
            rows_before = len(df)
            loan_amount_before = calculate_loan_amount(df)
            
            # Find maturity date column
            maturity_col = None
            for col in df.columns:
                if TRANSACTION_MATURITY_DATE in col.lower():
                    maturity_col = col
                    break
            
            if maturity_col:
                from datetime import datetime
                cutoff_dt = datetime.strptime(cutoff_date, '%d/%m/%Y')
                
                # Create mask for rows to remove
                remove_mask = pd.Series([False] * len(df), index=df.index)
                
                for idx in df.index:
                    try:
                        maturity_str = df.loc[idx, maturity_col]
                        if not maturity_str or str(maturity_str).strip() == '':
                            continue
                            
                        maturity_dt = pd.to_datetime(maturity_str, format='%d/%m/%Y', errors='coerce')
                        if pd.isna(maturity_dt):
                            continue
                        
                        # Calculate days difference
                        days_diff = (maturity_dt - cutoff_dt).days
                        
                        # Remove if maturity is within 365 days of cutoff
                        if days_diff <= 365 and days_diff >= 0:
                            remove_mask[idx] = True
                            
                    except Exception as e:
                        logger.warning(f"Error processing row {idx} for Rule 3: {str(e)}")
                
                # Get rows to be removed
                rows_to_remove = df[remove_mask]
                loan_amount_removed = calculate_loan_amount(rows_to_remove)
                
                # Remove rows
                df = df[~remove_mask]
                df = df.reset_index(drop=True)
                
                rows_after = len(df)
                loan_amount_after = calculate_loan_amount(df)
                rows_removed = rows_before - rows_after
                
                rules_results.append({
                    "rule_number": 3,
                    "rule_name": "Remove loans with maturity within 365 days of cutoff",
                    "rows_removed": rows_removed,
                    "rows_left": rows_after,
                    "loan_amount_removed": loan_amount_removed,
                    "loan_amount_left": loan_amount_after,
                    "applied": True
                })
            else:
                rules_results.append({
                    "rule_number": 3,
                    "rule_name": "Remove loans with maturity within 365 days of cutoff",
                    "rows_removed": 0,
                    "rows_left": len(df),
                    "loan_amount_removed": 0,
                    "loan_amount_left": calculate_loan_amount(df),
                    "applied": False,
                    "note": "Maturity Date column not found"
                })
        
        # Rule 4: Remove rows with non-zero overdue/dpd
        if apply_rule4:
            rows_before = len(df)
            loan_amount_before = calculate_loan_amount(df)
            
            # Find overdue and dpd columns
            overdue_col = None
            dpd_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if col_lower == TRANSACTION_OVERDUE:
                    overdue_col = col
                elif col_lower == TRANSACTION_DPD:
                    dpd_col = col
            
            if overdue_col or dpd_col:
                # Create mask for rows to remove
                remove_mask = pd.Series([False] * len(df), index=df.index)
                
                if overdue_col:
                    overdue_numeric = pd.to_numeric(df[overdue_col], errors='coerce').fillna(0)
                    remove_mask = remove_mask | (overdue_numeric != 0)
                
                if dpd_col:
                    dpd_numeric = pd.to_numeric(df[dpd_col], errors='coerce').fillna(0)
                    remove_mask = remove_mask | (dpd_numeric != 0)
                
                # Get rows to be removed
                rows_to_remove = df[remove_mask]
                loan_amount_removed = calculate_loan_amount(rows_to_remove)
                
                # Remove rows
                df = df[~remove_mask]
                df = df.reset_index(drop=True)
                
                rows_after = len(df)
                loan_amount_after = calculate_loan_amount(df)
                rows_removed = rows_before - rows_after
                
                rules_results.append({
                    "rule_number": 4,
                    "rule_name": "Remove rows with non-zero overdue/dpd",
                    "rows_removed": rows_removed,
                    "rows_left": rows_after,
                    "loan_amount_removed": loan_amount_removed,
                    "loan_amount_left": loan_amount_after,
                    "applied": True
                })
            else:
                rules_results.append({
                    "rule_number": 4,
                    "rule_name": "Remove rows with non-zero overdue/dpd",
                    "rows_removed": 0,
                    "rows_left": len(df),
                    "loan_amount_removed": 0,
                    "loan_amount_left": calculate_loan_amount(df),
                    "applied": False,
                    "note": "Overdue/DPD columns not found"
                })
        
        # Rule 5: Remove restructured/rescheduled loans
        if apply_rule5:
            rows_before = len(df)
            loan_amount_before = calculate_loan_amount(df)
            
            # Find restructured and rescheduled loan columns
            restructured_col = None
            rescheduled_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if TRANSACTION_RESTRUCTURED in col_lower:
                    restructured_col = col
                elif TRANSACTION_RESCHEDULED in col_lower:
                    rescheduled_col = col
            
            if restructured_col or rescheduled_col:
                # Create mask for rows to remove
                remove_mask = pd.Series([False] * len(df), index=df.index)
                
                # Helper function to check if value is true
                def is_true_value(val):
                    val_str = str(val).strip().lower()
                    return val_str in ['true', '1', 'yes', 'y']
                
                if restructured_col:
                    restructured_true = df[restructured_col].apply(is_true_value)
                    remove_mask = remove_mask | restructured_true
                
                if rescheduled_col:
                    rescheduled_true = df[rescheduled_col].apply(is_true_value)
                    remove_mask = remove_mask | rescheduled_true
                
                # Get rows to be removed
                rows_to_remove = df[remove_mask]
                loan_amount_removed = calculate_loan_amount(rows_to_remove)
                
                # Remove rows
                df = df[~remove_mask]
                df = df.reset_index(drop=True)
                
                rows_after = len(df)
                loan_amount_after = calculate_loan_amount(df)
                rows_removed = rows_before - rows_after
                
                rules_results.append({
                    "rule_number": 5,
                    "rule_name": "Remove restructured/rescheduled loans",
                    "rows_removed": rows_removed,
                    "rows_left": rows_after,
                    "loan_amount_removed": loan_amount_removed,
                    "loan_amount_left": loan_amount_after,
                    "applied": True
                })
            else:
                rules_results.append({
                    "rule_number": 5,
                    "rule_name": "Remove restructured/rescheduled loans",
                    "rows_removed": 0,
                    "rows_left": len(df),
                    "loan_amount_removed": 0,
                    "loan_amount_left": calculate_loan_amount(df),
                    "applied": False,
                    "note": "Restructured/Rescheduled loan columns not found"
                })
        
        # Save the updated file
        if file_path.endswith(".xlsx"):
            df.to_excel(file_path, index=False, engine="openpyxl")
        elif file_path.endswith(".csv"):
            df.to_csv(file_path, index=False, encoding="utf-8")
        
        # Calculate final stats
        metadata["total_rows_after"] = len(df)
        metadata["total_loan_amount_after"] = calculate_loan_amount(df)
        
        # Update metadata with all rules applied
        metadata["rules_applied"] = rules_results
        
        # Update version with new metadata
        from app.utils.db import db
        db["transaction_versions"].update_one(
            {"_id": ObjectId(version["_id"])},
            {"$set": {"rbi_rules_metadata": metadata}}
        )
        
        # Calculate totals
        total_rows_removed = sum(rule.get("rows_removed", 0) for rule in rules_results)
        total_loan_amount_removed = sum(rule.get("loan_amount_removed", 0) for rule in rules_results)
        
        return jsonify({
            "status": "success",
            "message": f"RBI rules applied successfully",
            "cutoff_date": cutoff_date,
            "rules_applied": rules_results,
            "summary": {
                "total_rows_removed": total_rows_removed,
                "total_loan_amount_removed": total_loan_amount_removed,
                "final_row_count": len(df),
                "initial_row_count": metadata["total_rows_before"],
                "initial_loan_amount": metadata["total_loan_amount_before"],
                "final_loan_amount": metadata["total_loan_amount_after"]
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in apply_rbi_rules: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500


@transaction_dataset_bp.route('/save_rbi_rules_applied_temp_to_final', methods=['POST'])
def save_rbi_rules_applied_temp_to_final():
    """
    Move the temporary RBI rules file to the final RBI rules applied file.
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({"error": "Missing required field: transaction_id"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get the temp file version
        if not transaction.get("temp_rbi_rules_applied"):
            return jsonify({"error": "No temporary RBI rules file found"}), 400
        
        temp_version = transaction_version_model.get_version(transaction["temp_rbi_rules_applied"])
        if not temp_version:
            return jsonify({"error": "Temporary version not found"}), 404
        
        temp_file_path = temp_version.get("files_path")
        if not temp_file_path or not os.path.exists(temp_file_path):
            return jsonify({"error": "Temporary file not found"}), 404
        
        # Create new filename for final RBI rules applied file
        transaction_folder = os.path.dirname(temp_file_path)
        _, ext = os.path.splitext(temp_file_path)
        final_filename = f"{transaction['name'].replace(' ', '_')}_original_preprocessed_updated_column_names_datatype_converted_new_columns_added_rbi_rules_applied{ext}"
        final_file_path = os.path.join(transaction_folder, final_filename)
        
        # Read file to get final stats
        if temp_file_path.endswith(".xlsx"):
            df = pd.read_excel(temp_file_path, dtype=str, keep_default_na=False)
        elif temp_file_path.endswith(".csv"):
            df = pd.read_csv(temp_file_path, dtype=str, keep_default_na=False)
        
        # Update metadata with final stats
        metadata = temp_version.get("rbi_rules_metadata", {})
        metadata["total_rows_after"] = len(df)
        
        # Calculate final loan amount
        loan_col = None
        for col in df.columns:
            if col.lower() == TRANSACTION_LOAN_AMOUNT or col.lower() == "loan_amount":
                loan_col = col
                break
        
        if loan_col:
            try:
                df[loan_col] = pd.to_numeric(
                    df[loan_col].astype(str).str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0)
                metadata["total_loan_amount_after"] = float(df[loan_col].sum())
            except:
                metadata["total_loan_amount_after"] = 0
        else:
            metadata["total_loan_amount_after"] = 0
        
        # Rename the file
        os.rename(temp_file_path, final_file_path)
        
        # Create version for final file
        final_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="RBI rules applied",
            files_path=final_file_path,
            version_number=8,
            rbi_rules_metadata=metadata
        )
        
        if not final_version_id:
            # Rename back on error
            os.rename(final_file_path, temp_file_path)
            return jsonify({"error": "Failed to create final version"}), 500
        
        # Update transaction
        update_fields = {
            "final_rbi_rules_applied": final_version_id,
            "temp_rbi_rules_applied": None,  # Clear temp reference
            "version_number": 8
        }
        transaction_model.update_transaction(transaction_id, update_fields)
        
        transaction_model.update_step_status(transaction_id, "rbi_rules_applied", True)
        transaction_model.update_temp_step_status(transaction_id, "rbi_rules_in_progress", False)
        transaction_model.update_current_step(transaction_id, "rule_versions")

        return jsonify({
            "status": "success",
            "message": "RBI rules finalized",
            "final_version_id": final_version_id,
            "final_stats": {
                "total_rows": metadata["total_rows_after"],
                "total_loan_amount": metadata["total_loan_amount_after"]
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in save_rbi_rules_applied_temp_to_final: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500


@transaction_dataset_bp.route('/fetch_rbi_rules_applied_data/<transaction_id>', methods=['GET'])
def fetch_rbi_rules_applied_data(transaction_id):
    """
    Fetch RBI rules application statistics including rows removed per rule and loan amounts.
    
    Args:
        transaction_id (str): ID of the transaction
        
    Returns:
        JSON response with comprehensive RBI rules statistics and file information
    """
    try:
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Check which version to use (final or temp)
        is_finalized = transaction.get("final_rbi_rules_applied") is not None
        version_id = transaction.get("final_rbi_rules_applied") or transaction.get("temp_rbi_rules_applied")
        
        if not version_id:
            return jsonify({"error": "RBI rules not applied yet"}), 400
        
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({"error": "Version not found"}), 404
        
        metadata = version.get("rbi_rules_metadata", {})
        
        # Calculate summary statistics
        total_rows_removed = 0
        total_loan_amount_removed = 0
        rules_applied_count = 0
        
        for rule in metadata.get("rules_applied", []):
            if rule.get("applied", False):
                rules_applied_count += 1
                total_rows_removed += rule.get("rows_removed", 0)
                total_loan_amount_removed += rule.get("loan_amount_removed", 0)
        
        # Prepare response
        response_data = {
            "status": "success",
            "is_finalized": is_finalized,
            "file_info": {
                "version_id": version_id,
                "file_path": version.get("files_path", ""),
                "description": version.get("description", ""),
                "version_number": version.get("version_number", 0),
                "file_type": "final" if is_finalized else "temporary"
            },
            "cutoff_date": transaction.get("cutoff_date") or metadata.get("cutoff_date"),
            "initial_stats": {
                "total_rows": metadata.get("total_rows_before", 0),
                "total_loan_amount": metadata.get("total_loan_amount_before", 0)
            },
            "final_stats": {
                "total_rows": metadata.get("total_rows_after", 0),
                "total_loan_amount": metadata.get("total_loan_amount_after", 0)
            },
            "rules_data": metadata.get("rules_applied", []),
            "summary": {
                "total_rules_applied": rules_applied_count,
                "total_rows_removed": total_rows_removed,
                "total_loan_amount_removed": total_loan_amount_removed,
                "percentage_rows_removed": (total_rows_removed / metadata.get("total_rows_before", 1)) * 100 if metadata.get("total_rows_before", 0) > 0 else 0,
                "percentage_loan_amount_removed": (total_loan_amount_removed / metadata.get("total_loan_amount_before", 1)) * 100 if metadata.get("total_loan_amount_before", 0) > 0 else 0,
                "rows_reduction": f"{metadata.get('total_rows_before', 0)} → {metadata.get('total_rows_after', 0)}",
                "loan_amount_reduction": f"{metadata.get('total_loan_amount_before', 0):,.2f} → {metadata.get('total_loan_amount_after', 0):,.2f}"
            }
        }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Error in fetch_rbi_rules_applied_data: {str(e)}")
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500
    

@transaction_dataset_bp.route('/create_new_version_and_apply_rule', methods=['POST'])
def create_new_version_and_apply_rule():
    """
    Create a new root version from final RBI rules file and apply a rule
    This creates the first version in a new chain (branch_number = 0)
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        rules = data.get("rules", [])
        
        if not all([transaction_id, rules]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get system transaction columns for datatype mapping
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        # Create datatype mapping
        datatype_mapping = {}
        if system_columns:
            for col in system_columns:
                col_name = col.get("column_name")
                datatype = col.get("datatype")
                if col_name and datatype:
                    datatype_mapping[col_name] = datatype
        
        # Include newly added columns from transaction
        new_columns_datatypes = transaction.get("new_added_columns_datatype", {})
        datatype_mapping.update(new_columns_datatypes)
        
        # Get the final RBI rules file
        if not transaction.get("final_rbi_rules_applied"):
            return jsonify({"error": "RBI rules not finalized yet"}), 400
        
        source_version_id = transaction["final_rbi_rules_applied"]
        source_version = transaction_version_model.get_version(source_version_id)
        if not source_version:
            return jsonify({"error": "Source version not found"}), 404
        
        source_file_path = source_version.get("files_path")
        if not source_file_path or not os.path.exists(source_file_path):
            return jsonify({"error": "Source file not found"}), 404
        
        # Create a copy of the file
        transaction_folder = os.path.dirname(source_file_path)
        _, ext = os.path.splitext(source_file_path)
        
        # Get number of existing root versions
        root_versions = transaction.get("rule_application_root_versions", [])
        root_number = len(root_versions) + 1
        
        # Get the highest version number
        from app.utils.db import db
        all_versions = list(db["transaction_versions"].find({
            "transaction_id": ObjectId(transaction_id)
        }).sort("version_number", -1).limit(1))
        
        if all_versions:
            next_version_number = max(all_versions[0].get("version_number", 9) + 1, 9.0 + root_number)
        else:
            next_version_number = 9.0 + root_number
        
        new_filename = f"{transaction['name'].replace(' ', '_')}_rules_v{root_number}{ext}"
        new_file_path = os.path.join(transaction_folder, new_filename)
        
        # Copy file
        import shutil
        shutil.copy2(source_file_path, new_file_path)
        
        # Apply rules and get stats with datatype mapping
        stats_before, stats_after, rules_results = apply_complex_rules_to_file(
            new_file_path, rules, datatype_mapping
        )
        
        # Create version with branch_number = 0 (root)
        version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description=f"Root Version {root_number}",
            files_path=new_file_path,
            version_number=next_version_number,
            is_rule_application_version=True,
            parent_version_id=None,
            root_version_id=None,
            branch_level=0,
            branch_number=0,  # Root version always has branch_number = 0
            rule_applied={"rules": rules, "results": rules_results},
            stats_before_rule=stats_before,
            stats_after_rule=stats_after
        )
        
        if not version_id:
            os.remove(new_file_path)
            return jsonify({"error": "Failed to create version"}), 500
        
        # Update root_version_id to self
        db["transaction_versions"].update_one(
            {"_id": ObjectId(version_id)},
            {"$set": {"root_version_id": ObjectId(version_id)}}
        )
        
        # Add to transaction
        success = transaction_model.add_rule_application_root_version(transaction_id, version_id)
        
        if success:
            # NEW: Mark rule versions as started (not complete as more can be added)
            transaction_model.update_step_status(transaction_id, "rule_versions_created", True)
            transaction_model.update_temp_step_status(transaction_id, "rule_versions_in_progress", True)
            return jsonify({
                "status": "success",
                "message": "New root version created and rule applied",
                "version_id": version_id,
                "version_number": next_version_number,
                "branch_number": 0,
                "stats": {
                    "before": stats_before,
                    "after": stats_after,
                    "rules_results": rules_results
                }
            }), 200
        else:
            return jsonify({"error": "Failed to update transaction"}), 500
            
    except Exception as e:
        logger.error(f"Error in create_new_version_and_apply_rule: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@transaction_dataset_bp.route('/apply_rule_to_existing_version', methods=['POST'])
def apply_rule_to_existing_version():
    """
    Apply a rule to an existing version to create a sub-version
    Only allows one sub-version per parent (linear chain)
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        parent_version_id = data.get("parent_version_id")
        rules = data.get("rules", [])
        
        if not all([transaction_id, parent_version_id, rules]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        # Get system transaction columns for datatype mapping
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        # Create datatype mapping
        datatype_mapping = {}
        if system_columns:
            for col in system_columns:
                col_name = col.get("column_name")
                datatype = col.get("datatype")
                if col_name and datatype:
                    datatype_mapping[col_name] = datatype
        
        # Include newly added columns from transaction
        new_columns_datatypes = transaction.get("new_added_columns_datatype", {})
        datatype_mapping.update(new_columns_datatypes)
        
        # Get parent version
        parent_version = transaction_version_model.get_version(parent_version_id)
        if not parent_version:
            return jsonify({"error": "Parent version not found"}), 404
        
        # Check if parent already has a sub-version (enforce linear structure)
        from app.utils.db import db
        existing_child = db["transaction_versions"].find_one({
            "parent_version_id": ObjectId(parent_version_id)
        })
        
        if existing_child:
            return jsonify({
                "error": "This version already has a sub-version. Each version can only have one sub-version.",
                "existing_sub_version_id": str(existing_child["_id"])
            }), 400
        
        # Get parent file
        parent_file_path = parent_version.get("files_path")
        if not parent_file_path or not os.path.exists(parent_file_path):
            return jsonify({"error": "Parent file not found"}), 404
        
        # Create a copy of the file
        transaction_folder = os.path.dirname(parent_file_path)
        _, ext = os.path.splitext(parent_file_path)
        
        # Get branch level and root version
        branch_level = parent_version.get("branch_level", 0) + 1
        root_version_id = parent_version.get("root_version_id") or parent_version_id
        parent_branch_number = parent_version.get("branch_number", 0)
        branch_number = parent_branch_number + 1  # Increment branch number
        
        # Get the highest version number
        all_versions = list(db["transaction_versions"].find({
            "transaction_id": ObjectId(transaction_id)
        }).sort("version_number", -1).limit(1))
        
        if all_versions:
            next_version_number = all_versions[0].get("version_number", 9) + 0.01
        else:
            next_version_number = 9.1
        
        # Create filename showing the branch number
        new_filename = f"{transaction['name'].replace(' ', '_')}_rules_branch{branch_number}{ext}"
        new_file_path = os.path.join(transaction_folder, new_filename)
        
        # Copy file
        import shutil
        shutil.copy2(parent_file_path, new_file_path)
        
        # Apply rules and get stats with datatype mapping
        stats_before, stats_after, rules_results = apply_complex_rules_to_file(
            new_file_path, rules, datatype_mapping
        )
        
        # Create version
        version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description=f"Sub-version {branch_number}",
            files_path=new_file_path,
            version_number=next_version_number,
            is_rule_application_version=True,
            parent_version_id=parent_version_id,
            root_version_id=root_version_id,
            branch_level=branch_level,
            branch_number=branch_number,
            rule_applied={"rules": rules, "results": rules_results},
            stats_before_rule=stats_before,
            stats_after_rule=stats_after
        )
        
        if version_id:
            return jsonify({
                "status": "success",
                "message": "Rule applied successfully",
                "version_id": version_id,
                "parent_version_id": parent_version_id,
                "branch_level": branch_level,
                "branch_number": branch_number,
                "version_number": next_version_number,
                "stats": {
                    "before": stats_before,
                    "after": stats_after,
                    "rules_results": rules_results
                }
            }), 200
        else:
            os.remove(new_file_path)
            return jsonify({"error": "Failed to create version"}), 500
            
    except Exception as e:
        logger.error(f"Error in apply_rule_to_existing_version: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@transaction_dataset_bp.route('/fetch_all_rule_versions/<transaction_id>', methods=['GET'])
def fetch_all_rule_versions(transaction_id):
    """
    Fetch all rule application versions in a flat list with branch numbers
    """
    try:
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({"error": "Transaction not found"}), 404
        
        root_version_ids = transaction.get("rule_application_root_versions", [])
        
        # Build flat list of versions for each root
        all_versions = []
        
        for root_id in root_version_ids:
            # Get the linear chain starting from this root
            current_id = root_id
            
            while current_id:
                version = transaction_version_model.get_version(current_id)
                if not version:
                    break
                
                # Add version info to list
                version_info = {
                    "version_id": str(version["_id"]),
                    "parent_version": str(version.get("parent_version_id")) if version.get("parent_version_id") else None,
                    "root_version": str(version.get("root_version_id")) if version.get("root_version_id") else str(version["_id"]),
                    "branch_number": version.get("branch_number", 0),
                    "description": version.get("description", ""),
                    "rows_before": version.get("stats_before_rule", {}).get("rows", 0),
                    "rows_after": version.get("stats_after_rule", {}).get("rows", 0),
                    "rows_removed": version.get("stats_before_rule", {}).get("rows", 0) - version.get("stats_after_rule", {}).get("rows", 0),
                    "total_amount_before": version.get("stats_before_rule", {}).get("loan_amount", 0),
                    "total_amount_after": version.get("stats_after_rule", {}).get("loan_amount", 0),
                    "amount_removed": version.get("stats_before_rule", {}).get("loan_amount", 0) - version.get("stats_after_rule", {}).get("loan_amount", 0),
                    "rule_applied": version.get("rule_applied", {}).get("rules", []),
                    "file_path": version.get("files_path", ""),
                    "version_number": version.get("version_number", 0),
                    "created_at": version.get("created_at"),
                    "is_root": version.get("branch_number", 0) == 0
                }
                
                all_versions.append(version_info)
                
                # Find the child of current version (there should be only one)
                from app.utils.db import db
                child = db["transaction_versions"].find_one({
                    "parent_version_id": ObjectId(current_id)
                })
                
                current_id = str(child["_id"]) if child else None
        
        # Sort by root version and then by branch number
        all_versions.sort(key=lambda x: (x["root_version"], x["branch_number"]))
        
        # Group by root version for better organization
        grouped_versions = {}
        for version in all_versions:
            root = version["root_version"]
            if root not in grouped_versions:
                grouped_versions[root] = []
            grouped_versions[root].append(version)
        
        return jsonify({
            "status": "success",
            "versions": all_versions,
            "grouped_versions": grouped_versions,
            "total_chains": len(root_version_ids),
            "total_versions": len(all_versions)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in fetch_all_rule_versions: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@transaction_dataset_bp.route('/delete_rule_version', methods=['DELETE'])
def delete_rule_version():
    """
    Delete a version and all its descendants in the linear chain
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        version_id = data.get("version_id")
        
        if not all([transaction_id, version_id]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Function to delete version chain
        def delete_version_chain(vid):
            deleted_versions = []
            current_id = vid
            
            while current_id:
                # Find child before deleting
                child = db["transaction_versions"].find_one({
                    "parent_version_id": ObjectId(current_id)
                })
                
                # Get version details
                version = transaction_version_model.get_version(current_id)
                if version:
                    # Delete file
                    file_path = version.get("files_path")
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            logger.warning(f"Failed to delete file: {file_path}")
                    
                    deleted_versions.append(str(version["_id"]))
                    
                    # Delete version record
                    transaction_version_model.delete_version(current_id)
                
                # Move to child
                current_id = str(child["_id"]) if child else None
            
            return deleted_versions
        
        # Check if this is a root version
        transaction = transaction_model.get_transaction(transaction_id)
        if version_id in transaction.get("rule_application_root_versions", []):
            # Remove from root versions list
            transaction_model.remove_rule_application_root_version(transaction_id, version_id)
        
        # Delete the version chain
        deleted_versions = delete_version_chain(version_id)
        
        return jsonify({
            "status": "success",
            "message": f"Deleted {len(deleted_versions)} versions in the chain",
            "deleted_versions": deleted_versions
        }), 200
        
    except Exception as e:
        logger.error(f"Error in delete_rule_version: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@transaction_dataset_bp.route('/delete_sub_version', methods=['DELETE'])
def delete_sub_version():
    """
    Delete only a specific sub-version (not its children)
    
    Request Body:
    {
        "transaction_id": "xxx",
        "version_id": "xxx",
        "delete_children": false
    }
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        version_id = data.get("version_id")
        delete_children = data.get("delete_children", True)  # Default to true for safety
        
        if not all([transaction_id, version_id]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get version details
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({"error": "Version not found"}), 404
        
        # Check if this is a root version
        transaction = transaction_model.get_transaction(transaction_id)
        if version_id in transaction.get("rule_application_root_versions", []):
            return jsonify({"error": "Cannot delete root version using this endpoint. Use delete_rule_version instead."}), 400
        
        # Get parent information
        parent_id = version.get("parent_version_id")
        
        if delete_children:
            # Delete this version and all its descendants
            deleted_versions = []
            
            def delete_version_chain(vid):
                """Recursively delete version and all its children"""
                deleted = []
                current_id = vid
                
                while current_id:
                    # Find child before deleting
                    child = db["transaction_versions"].find_one({
                        "parent_version_id": ObjectId(current_id)
                    })
                    
                    # Get version details
                    v = transaction_version_model.get_version(current_id)
                    if v:
                        # Delete file
                        file_path = v.get("files_path")
                        if file_path and os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except Exception as e:
                                logger.warning(f"Failed to delete file: {file_path}")
                        
                        deleted.append(str(v["_id"]))
                        
                        # Delete version record
                        transaction_version_model.delete_version(current_id)
                    
                    # Move to child
                    current_id = str(child["_id"]) if child else None
                
                return deleted
            
            # If we have a parent, we need to update the parent's child reference
            if parent_id:
                # Find if parent has a child pointing to this version
                # Since it's a linear structure, we just need to find any child of this version
                child_of_deleted = db["transaction_versions"].find_one({
                    "parent_version_id": ObjectId(version_id)
                })
                
                # If the deleted version has children and parent exists,
                # we can't maintain the chain properly when deleting with children
                # So we just delete the entire sub-chain
                
            deleted_versions = delete_version_chain(version_id)
            
            return jsonify({
                "status": "success",
                "message": f"Sub-version and {len(deleted_versions)} descendants deleted successfully",
                "deleted_versions": deleted_versions
            }), 200
            
        else:
            # Only delete this specific version, reconnect parent to children
            
            # Find all children of this version
            children = list(db["transaction_versions"].find({
                "parent_version_id": ObjectId(version_id)
            }))
            
            if len(children) > 1:
                # This shouldn't happen in a linear structure
                return jsonify({
                    "error": "Version has multiple children. Cannot maintain linear structure."
                }), 400
            
            # Update child to point to grandparent
            if children and parent_id:
                child = children[0]
                # Update child's parent to be this version's parent
                db["transaction_versions"].update_one(
                    {"_id": child["_id"]},
                    {
                        "$set": {
                            "parent_version_id": parent_id,
                            "branch_level": version.get("branch_level", 1) - 1,
                            "updated_at": datetime.now()
                        }
                    }
                )
                
                # Update all descendants' branch numbers
                def update_descendant_branch_numbers(start_id, decrement):
                    """Recursively update branch numbers for all descendants"""
                    current_id = start_id
                    while current_id:
                        # Update branch number
                        db["transaction_versions"].update_one(
                            {"_id": ObjectId(current_id)},
                            {
                                "$inc": {"branch_number": -decrement},
                                "$set": {"updated_at": datetime.now()}
                            }
                        )
                        
                        # Find next child
                        next_child = db["transaction_versions"].find_one({
                            "parent_version_id": ObjectId(current_id)
                        })
                        
                        current_id = str(next_child["_id"]) if next_child else None
                
                # Update all descendants' branch numbers (decrement by 1)
                update_descendant_branch_numbers(str(child["_id"]), 1)
            
            elif children and not parent_id:
                # The version being deleted is directly under root
                # Make the child a new root
                child = children[0]
                child_id = str(child["_id"])
                
                # Update child to be a root
                db["transaction_versions"].update_one(
                    {"_id": child["_id"]},
                    {
                        "$set": {
                            "parent_version_id": None,
                            "root_version_id": child["_id"],
                            "branch_level": 0,
                            "branch_number": 0,
                            "updated_at": datetime.now()
                        }
                    }
                )
                
                # Add child as new root in transaction
                transaction_model.add_rule_application_root_version(transaction_id, child_id)
                
                # Update all descendants to have new root
                def update_descendants_root(start_id, new_root_id):
                    """Update root reference for all descendants"""
                    current_id = start_id
                    level = 0
                    while current_id:
                        # Find next child
                        next_child = db["transaction_versions"].find_one({
                            "parent_version_id": ObjectId(current_id)
                        })
                        
                        if next_child:
                            level += 1
                            db["transaction_versions"].update_one(
                                {"_id": next_child["_id"]},
                                {
                                    "$set": {
                                        "root_version_id": ObjectId(new_root_id),
                                        "branch_level": level,
                                        "branch_number": level,
                                        "updated_at": datetime.now()
                                    }
                                }
                            )
                        
                        current_id = str(next_child["_id"]) if next_child else None
                
                update_descendants_root(child_id, child_id)
            
            # Delete file
            file_path = version.get("files_path")
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.warning(f"Failed to delete file: {file_path}")
            
            # Delete version record
            success = transaction_version_model.delete_version(version_id)
            
            if success:
                return jsonify({
                    "status": "success",
                    "message": "Sub-version deleted successfully, chain maintained"
                }), 200
            else:
                return jsonify({"error": "Failed to delete version"}), 500
        
    except Exception as e:
        logger.error(f"Error in delete_sub_version: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@transaction_dataset_bp.route('/get_version_chain/<transaction_id>/<version_id>', methods=['GET'])
def get_version_chain(transaction_id, version_id):
    """
    Get the complete chain information for a specific version
    """
    try:
        # Find the root of this version
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({"error": "Version not found"}), 404
        
        root_id = version.get("root_version_id") or version_id
        
        # Build the complete chain from root
        chain = []
        current_id = root_id
        
        while current_id:
            v = transaction_version_model.get_version(current_id)
            if not v:
                break
                
            chain.append({
                "version_id": str(v["_id"]),
                "branch_number": v.get("branch_number", 0),
                "description": v.get("description", ""),
                "rows_count": v.get("stats_after_rule", {}).get("rows", 0),
                "is_current": str(v["_id"]) == version_id
            })
            
            # Find child
            from app.utils.db import db
            child = db["transaction_versions"].find_one({
                "parent_version_id": ObjectId(current_id)
            })
            
            current_id = str(child["_id"]) if child else None
        
        return jsonify({
            "status": "success",
            "chain": chain,
            "chain_length": len(chain),
            "current_branch_number": version.get("branch_number", 0)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_version_chain: {str(e)}")
        return jsonify({"error": str(e)}), 500


def apply_complex_rules_to_file(file_path, rules, datatype_mapping=None):
    """
    Apply complex rules with AND/OR/THEN logic to a file
    """
    try:
        # Load file
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        
        # Get initial stats
        initial_rows = len(df)
        
        # Find loan amount column
        loan_col = None
        for col in df.columns:
            if col.lower() in [TRANSACTION_LOAN_AMOUNT, "loan_amount"]:
                loan_col = col
                break
        
        initial_loan_amount = 0
        if loan_col:
            try:
                initial_loan_amount = float(pd.to_numeric(
                    df[loan_col].str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0).sum())
            except:
                initial_loan_amount = 0
        
        stats_before = {
            "rows": initial_rows,
            "loan_amount": initial_loan_amount
        }
        
        # Apply each rule group
        all_results = []
        total_rows_removed = 0
        total_amount_removed = 0
        
        for rule_group in rules:
            if not rule_group:
                continue
            
            # Clean up rule group - remove 'then' from all but last rule
            for i, rule in enumerate(rule_group[:-1]):  # All except last
                if 'then' in rule:
                    logger.warning(f"Removing 'then' from non-final rule at position {i}")
                    del rule['then']
            
            # Log the cleaned rule group for debugging
            logger.info(f"Processing rule group with {len(rule_group)} rules")
            for i, rule in enumerate(rule_group):
                logger.info(f"Rule {i}: {rule}")
            
            # Build condition mask for the rule group with datatype mapping
            mask = build_condition_mask(df, rule_group, datatype_mapping)
            
            # Get the final rule's "then" value (from the last rule only)
            rule_type = "reject"  # default
            if rule_group and len(rule_group) > 0:
                last_rule = rule_group[-1]
                rule_type = last_rule.get("then", "reject").lower()
            
            logger.info(f"Rule type: {rule_type}, Mask matches: {mask.sum()} rows out of {len(df)}")
            
            # Apply logic based on rule_type
            if rule_type == "accept":
                # Accept: Keep only matching rows, remove all others
                rows_to_remove = df[~mask]
                rows_removed = len(rows_to_remove)
                # Keep matching rows
                df = df[mask].reset_index(drop=True)
            else:  # reject (default)
                # Reject: Remove matching rows, keep all others
                rows_to_remove = df[mask]
                rows_removed = len(rows_to_remove)
                # Keep non-matching rows
                df = df[~mask].reset_index(drop=True)
            
            # Calculate amount removed
            amount_removed = 0
            if loan_col and rows_removed > 0:
                try:
                    amount_removed = float(pd.to_numeric(
                        rows_to_remove[loan_col].str.replace(',', ''), 
                        errors='coerce'
                    ).fillna(0).sum())
                except:
                    amount_removed = 0
            
            total_rows_removed += rows_removed
            total_amount_removed += amount_removed
            
            all_results.append({
                "rule_group": rule_group,
                "rule_type": rule_type,
                "rows_removed": rows_removed,
                "amount_removed": amount_removed,
                "rows_remaining": len(df)
            })
            
            logger.info(f"After applying rule group: {rows_removed} rows removed, {len(df)} rows remaining")
        
        # Save updated file
        if file_path.endswith(".xlsx"):
            df.to_excel(file_path, index=False, engine="openpyxl")
        else:
            df.to_csv(file_path, index=False, encoding="utf-8")
        
        # Calculate final stats
        final_rows = len(df)
        final_loan_amount = 0
        
        if loan_col:
            try:
                final_loan_amount = float(pd.to_numeric(
                    df[loan_col].str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0).sum())
            except:
                final_loan_amount = 0
        
        stats_after = {
            "rows": final_rows,
            "loan_amount": final_loan_amount
        }
        
        rules_results = {
            "total_rows_removed": total_rows_removed,
            "total_amount_removed": total_amount_removed,
            "rule_groups_applied": len(rules),
            "detailed_results": all_results
        }
        
        logger.info(f"Final results: {total_rows_removed} total rows removed, {final_rows} rows remaining")
        
        return stats_before, stats_after, rules_results
        
    except Exception as e:
        logger.error(f"Error applying complex rules to file: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def build_condition_mask(df, rule_group, datatype_mapping=None):
    """
    Build a combined mask for a group of rules with AND/OR/THEN connectors
    """
    if not rule_group:
        return pd.Series([False] * len(df))
    
    mask = None
    pending_connector = None  # Track the connector to use for the next condition
    
    for i, rule in enumerate(rule_group):
        condition = build_single_condition(df, rule, datatype_mapping)
        connector = rule.get("connector", "AND").strip().upper()
        
        if i == 0:
            mask = condition
            pending_connector = connector if connector != "THEN" else None
        else:
            # Apply the pending connector from the previous rule
            if pending_connector == "OR":
                mask |= condition
            else:  # Default to AND
                mask &= condition
            
            # Set pending connector for next iteration (unless it's THEN)
            pending_connector = connector if connector != "THEN" else None
    
    return mask

def build_single_condition(df, rule, datatype_mapping=None):
    """
    Build condition for a single rule
    """
    col = rule.get("column", "").strip()
    op = rule.get("operator", "").strip().lower()
    val = rule.get("value")
    
    if col not in df.columns:
        return pd.Series([False] * len(df))
    
    try:
        # Get column datatype from mapping
        col_dtype = datatype_mapping.get(col, "string").lower() if datatype_mapping else "string"
        
        # String operations
        if op in ["equal to", "equals"]:
            if col_dtype == "date":
                col_dates = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                val_date = pd.to_datetime(val)
                return col_dates == val_date
            else:
                # Try numeric comparison first
                try:
                    numeric_col = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                    numeric_val = float(val)
                    return numeric_col == numeric_val
                except:
                    # Fall back to string comparison
                    return df[col].str.lower() == str(val).lower()
                    
        elif op == "not equal to":
            if col_dtype == "date":
                col_dates = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                val_date = pd.to_datetime(val)
                return col_dates != val_date
            else:
                try:
                    numeric_col = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                    numeric_val = float(val)
                    return numeric_col != numeric_val
                except:
                    return df[col].str.lower() != str(val).lower()
                    
        elif op == "greater than":
            if col_dtype == "date":
                col_dates = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                val_date = pd.to_datetime(val)
                return col_dates > val_date
            else:
                numeric_col = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                return numeric_col > float(val)
            
        elif op == "less than":
            if col_dtype == "date":
                col_dates = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                val_date = pd.to_datetime(val)
                return col_dates < val_date
            else:
                numeric_col = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                return numeric_col < float(val)
            
        elif op == "greater than or equal":
            if col_dtype == "date":
                col_dates = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                val_date = pd.to_datetime(val)
                return col_dates >= val_date
            else:
                numeric_col = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                return numeric_col >= float(val)
            
        elif op == "less than or equal":
            if col_dtype == "date":
                col_dates = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                val_date = pd.to_datetime(val)
                return col_dates <= val_date
            else:
                numeric_col = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                return numeric_col <= float(val)
            
        elif op in ["contains", "includes"]:  # ADD "includes" here
            return df[col].str.contains(str(val), case=False, na=False)
            
        elif op == "not contains":
            return ~df[col].str.contains(str(val), case=False, na=False)
            
        else:
            # Unknown operator, return all False
            return pd.Series([False] * len(df))
            
    except Exception as e:
        logger.error(f"Error building condition for rule {rule}: {str(e)}")
        return pd.Series([False] * len(df))

@transaction_dataset_bp.route('/mark_processing_complete', methods=['POST'])
def mark_processing_complete():
    """
    Mark a transaction as processing complete by setting are_all_steps_complete to true
    
    Request Body:
    {
        "transaction_id": "xxx"
    }
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        
        if not transaction_id:
            return jsonify({
                "status": "error",
                "message": "Missing required field: transaction_id"
            }), 400
        
        # Get transaction to verify it exists
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                "status": "error",
                "message": "Transaction not found"
            }), 404
        
        # Update the are_all_steps_complete field to true
        update_fields = {
            "are_all_steps_complete": True
        }
        
        success = transaction_model.update_transaction(transaction_id, update_fields)
        
        if success:
            return jsonify({
                "status": "success",
                "message": "Transaction marked as processing complete",
                "transaction_id": transaction_id,
                "is_processing_done": True
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to update transaction"
            }), 500
            
    except Exception as e:
        logger.error(f"Error in mark_processing_complete: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred",
            "details": str(e)
        }), 500

@transaction_dataset_bp.route('/update_processing_status', methods=['POST'])
def update_processing_status():
    """
    Update the processing status of a transaction
    
    Request Body:
    {
        "transaction_id": "xxx",
        "is_processing_done": true/false
    }
    
    Returns:
        JSON response with success status
    """
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        is_processing_done = data.get("is_processing_done")
        
        if not transaction_id:
            return jsonify({
                "status": "error",
                "message": "Missing required field: transaction_id"
            }), 400
            
        if is_processing_done is None:
            return jsonify({
                "status": "error", 
                "message": "Missing required field: is_processing_done"
            }), 400
        
        # Get transaction to verify it exists
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                "status": "error",
                "message": "Transaction not found"
            }), 404
        
        # Update the are_all_steps_complete field
        update_fields = {
            "are_all_steps_complete": bool(is_processing_done)
        }
        
        success = transaction_model.update_transaction(transaction_id, update_fields)
        
        if success:
            return jsonify({
                "status": "success",
                "message": f"Transaction processing status updated to {bool(is_processing_done)}",
                "transaction_id": transaction_id,
                "is_processing_done": bool(is_processing_done)
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to update transaction"
            }), 500
            
    except Exception as e:
        logger.error(f"Error in update_processing_status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred",
            "details": str(e)
        }), 500


@transaction_dataset_bp.route('/fetch_dataset_columns_and_their_datatype/<transaction_id>', methods=['GET'])
def fetch_dataset_columns_and_their_datatype(transaction_id):
    """
    Fetch the dataset columns from the latest available file and map them with their datatypes.
    
    Args:
        transaction_id (str): ID of the transaction
        
    Returns:
        JSON response with column names mapped to their datatypes:
        {
            "status": "success",
            "column_datatypes": {
                "transaction_id": "text",
                "loan_amount": "decimal",
                "maturity_date": "date",
                "dpd": "number"
            }
        }
    """
    try:
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
        
        # Determine which version to use based on priority
        version_id = None
        
        # Priority order for finding the latest file
        if transaction.get('final_rbi_rules_applied'):
            version_id = transaction['final_rbi_rules_applied']
        elif transaction.get('temp_rbi_rules_applied'):
            version_id = transaction['temp_rbi_rules_applied']
        elif transaction.get('added_new_column_final'):
            version_id = transaction['added_new_column_final']
        elif transaction.get('temp_new_column_adding'):
            version_id = transaction['temp_new_column_adding']
        elif transaction.get('changed_datatype_of_column'):
            version_id = transaction['changed_datatype_of_column']
        elif transaction.get('temp_changing_datatype_of_column'):
            version_id = transaction['temp_changing_datatype_of_column']
        elif transaction.get('column_rename_file'):
            version_id = transaction['column_rename_file']
        elif transaction.get('preprocessed_file'):
            version_id = transaction['preprocessed_file']
        elif transaction.get('base_file'):
            version_id = transaction['base_file']
        else:
            return jsonify({
                'status': 'error',
                'message': 'No file associated with transaction'
            }), 404
        
        # Get version details
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({
                'status': 'error',
                'message': 'Version not found'
            }), 404
        
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({
                'status': 'error',
                'message': 'File not found'
            }), 404
        
        # Read the dataset to get column names (just headers)
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, nrows=1)
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, nrows=1)
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Unsupported file format'
                }), 400
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error reading file',
                'details': str(e)
            }), 500
        
        # Get column names from dataset
        dataset_columns = df.columns.tolist()
        
        # Get system columns and create mapping
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        
        # Create datatype mapping dictionary
        column_datatypes = {}
        
        if system_columns:
            # Create a mapping of system column names to datatypes
            system_column_mapping = {}
            for col in system_columns:
                column_name = col.get("column_name")
                datatype = col.get("datatype")
                if column_name and datatype:
                    system_column_mapping[column_name] = datatype
            
            # Map dataset columns to their datatypes
            for column in dataset_columns:
                if column in system_column_mapping:
                    column_datatypes[column] = system_column_mapping[column]
        
        # Include new columns added during processing
        new_columns_datatypes = transaction.get("new_added_columns_datatype", {})
        for column_name, datatype in new_columns_datatypes.items():
            if column_name in dataset_columns and column_name not in column_datatypes:
                column_datatypes[column_name] = datatype
        
        return jsonify({
            'status': 'success',
            'column_datatypes': column_datatypes
        }), 200
        
    except Exception as e:
        logger.error(f"Error in fetch_dataset_columns_and_their_datatype: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500
        
@transaction_dataset_bp.route('/get_final_rbi_data/<transaction_id>', methods=['GET'])
def get_final_rbi_data(transaction_id):
    """
    Get the final dataset after RBI rules have been applied and finalized.
    Returns data in the same format as get_transaction_data API with file location.
    
    Args:
        transaction_id (str): ID of the transaction
        
    Returns:
        JSON response with columns, rows (first 10), total rows, and file location
    """
    try:
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
        
        # Check if RBI rules have been finalized
        if not transaction.get('final_rbi_rules_applied'):
            return jsonify({
                'status': 'error',
                'message': 'RBI rules have not been finalized yet. Please complete the RBI rules application process.'
            }), 400
        
        # Get the final RBI rules applied version
        version_id = transaction['final_rbi_rules_applied']
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({
                'status': 'error',
                'message': 'Final RBI version not found'
            }), 404
        
        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({
                'status': 'error',
                'message': 'Final RBI file not found'
            }), 404
        
        # Read the file and get data
        try:
            if file_path.endswith(".xlsx"):
                # Read first 10 rows for preview
                df_preview = pd.read_excel(file_path, dtype=str, nrows=10)
                # Get total row count
                df_total = pd.read_excel(file_path, dtype=str)
                total_rows = len(df_total)
            elif file_path.endswith(".csv"):
                # Read first 10 rows for preview
                df_preview = pd.read_csv(file_path, dtype=str, nrows=10)
                # Get total row count
                df_total = pd.read_csv(file_path, dtype=str)
                total_rows = len(df_total)
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Unsupported file format'
                }), 400
                
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error reading the file',
                'details': str(e)
            }), 500
        
        # Convert dataframe to list of dictionaries
        df_preview = df_preview.where(pd.notnull(df_preview), '')
        rows = df_preview.to_dict(orient="records")
        
        # Get column names
        columns = list(df_preview.columns)
        
        # Get RBI metadata if available
        rbi_metadata = version.get('rbi_rules_metadata', {})
        
        # Calculate total loan amount if loan column exists
        loan_amount_total = 0
        loan_col = None
        for col in df_total.columns:
            if col.lower() in [TRANSACTION_LOAN_AMOUNT, "loan_amount"]:
                loan_col = col
                break
        
        if loan_col:
            try:
                df_total[loan_col] = pd.to_numeric(
                    df_total[loan_col].str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0)
                loan_amount_total = float(df_total[loan_col].sum())
            except:
                loan_amount_total = 0
        
        # Prepare response
        response_data = {
            'status': 'success',
            'columns': columns,
            'rows': rows,
            'total_rows': total_rows,
            'file_location': file_path,
            'file_name': os.path.basename(file_path),
            'version_info': {
                'version_id': version_id,
                'version_number': version.get('version_number', 8),
                'description': version.get('description', 'RBI rules applied'),
                'created_at': version.get('created_at')
            },
            'rbi_summary': {
                'cutoff_date': transaction.get('cutoff_date') or rbi_metadata.get('cutoff_date'),
                'total_rows_before_rbi': rbi_metadata.get('total_rows_before', 0),
                'total_rows_after_rbi': rbi_metadata.get('total_rows_after', total_rows),
                'total_loan_amount': loan_amount_total,
                'rules_applied_count': len(rbi_metadata.get('rules_applied', [])),
                'is_finalized': True
            }
        }
        
        # Add detailed rules information if needed
        if rbi_metadata.get('rules_applied'):
            response_data['rbi_rules_details'] = rbi_metadata['rules_applied']
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Error in get_final_rbi_data: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@transaction_dataset_bp.route('/get_gpt_column_mapping/<transaction_id>', methods=['GET'])
def get_gpt_column_mapping(transaction_id):
    """
    Fetch system columns and dataset columns from preprocessed dataset,
    send to GPT assistant, and return the mapping.
    """
    try:
        # Get transaction
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({'status': 'error', 'message': 'Transaction not found'}), 404

        # Get preprocessed version id
        version_id = transaction.get('preprocessed_file')
        if not version_id:
            return jsonify({'status': 'error', 'message': 'Preprocessed file not found'}), 404

        # Get version details
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({'status': 'error', 'message': 'Version not found'}), 404

        file_path = version.get('files_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({'status': 'error', 'message': 'File not found'}), 404

        # Read dataset columns and get sample values
        import pandas as pd
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, dtype=str)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path, dtype=str)
        else:
            return jsonify({'status': 'error', 'message': 'Unsupported file format'}), 400

        # Prepare uploaded columns with sample values
        uploaded_columns = []
        for col in df.columns:
            # Get up to 3 non-null sample values
            examples = df[col].dropna().unique().tolist()[:5]
            uploaded_columns.append({
                "name": col,
                "example_items": examples
            })

        # Get system columns with metadata
        from app.models.system_transaction_columns import SystemTransactionColumnModel
        system_column_model = SystemTransactionColumnModel()
        system_columns = system_column_model.get_all_columns()
        system_columns_structured = []
        for col in system_columns:
            system_columns_structured.append({
                "name": col.get("column_name"),
                "description": col.get("description", ""),
                "alternative_names": col.get("alt_names", [])
            })

        # Prepare input for GPT
        input_data = {
            "system_columns": system_columns_structured,
            "uploaded_columns": uploaded_columns
        }

        print(input_data)  # Debugging output to check the structure

        # Send to GPT assistant
        from app.utils.column_mapping import send_to_openai_assistant
        gpt_response = send_to_openai_assistant(input_data)

        # After you get gpt_response from send_to_openai_assistant
        if gpt_response.get("status") == "success" and "response" in gpt_response:
            try:
                # Clean up the response string before parsing
                response_str = gpt_response["response"]
                if isinstance(response_str, str):
                    response_str = response_str.strip()
                    # Remove leading/trailing quotes if present
                    if (response_str.startswith('"') and response_str.endswith('"')) or \
                    (response_str.startswith("'") and response_str.endswith("'")):
                        response_str = response_str[1:-1]
                    # Replace escaped newlines and tabs
                    response_str = response_str.replace('\\n', '').replace('\\t', '').replace('\n', '').replace('\t', '')
                    # Remove double backslashes
                    response_str = response_str.replace('\\\\', '\\')
                    # Remove extra spaces
                    response_str = response_str.strip()
                # Parse to JSON
                gpt_response["response"] = json.loads(response_str)
            except Exception as e:
                logger.error(f"Failed to parse GPT response: {e}")
                gpt_response["response"] = []

        return jsonify({
            "status": "success",
            "gpt_response": gpt_response["response"] if gpt_response.get("status") == "success" else gpt_response
        }), 200

    except Exception as e:
        logger.error(f"Error in get_gpt_column_mapping: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": "An unexpected error occurred",
            "details": str(e)
        }), 500