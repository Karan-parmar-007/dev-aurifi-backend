# transaction/views.py
from app.blueprints.transaction import transaction_bp
from app.models.transaction_model import TransactionModel
from app.models.transaction_version_model import TransactionVersionModel
from app.models.user_model import UserModel
from app.utils.logger import logger
import os
from werkzeug.utils import secure_filename
from flask import request, jsonify
import pandas as pd
from bson import ObjectId

# Initialize models
transaction_model = TransactionModel()
transaction_version_model = TransactionVersionModel()
user_model = UserModel()

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'datasets/transactions')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def save_file(file, filename, transaction_name):
    """Save uploaded file to a transaction-specific folder in the datasets directory
    
    Args:
        file: File object from request
        filename: Desired filename
        transaction_name: Name of the transaction (used as folder name)
        
    Returns:
        tuple: (bool, str, str) - (success status, file path, base folder path)
    """
    try:
        # Secure the transaction name
        secure_transaction_name = secure_filename(transaction_name)
        # Replace spaces with underscores
        secure_transaction_name = secure_transaction_name.replace(' ', '_')
        
        # Create transaction-specific folder path
        transaction_folder = os.path.join(UPLOAD_FOLDER, secure_transaction_name)
        
        # Check if transaction folder already exists
        if os.path.exists(transaction_folder):
            return False, "A transaction with this name already exists. Please choose a different transaction name.", None
        
        # Create transaction folder
        os.makedirs(transaction_folder)
        
        # Get file extension
        _, ext = os.path.splitext(filename)
        
        # Create filename with _original suffix
        secure_name = secure_filename(f"{secure_transaction_name}_original{ext}")
        # Ensure no spaces in filename
        secure_name = secure_name.replace(' ', '_')
        
        # Create file path within transaction folder
        file_path = os.path.join(transaction_folder, secure_name)
        
        # Save the file
        file.save(file_path)
        return True, file_path, transaction_folder
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        return False, "Error saving file", None

@transaction_bp.route('/upload_dataset', methods=['POST'])
def upload_dataset():
    """Upload a transaction dataset, create a transaction, process the dataset, and manage versions."""
    try:
        # Initialize models inside the function
        transaction_model = TransactionModel()
        transaction_version_model = TransactionVersionModel()
        user_model = UserModel()
        # Check if file is present in request
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'No file part in the request'
            }), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'No file selected'
            }), 400

        # Get other form data
        transaction_name = request.form.get('transaction_name')
        user_id = request.form.get('user_id')
        primary_asset_class = request.form.get('primary_asset_class', '')
        secondary_asset_class = request.form.get('secondary_asset_class', '')

        # Validate required fields
        if not all([transaction_name, user_id]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: transaction_name and user_id'
            }), 400

        # Save file and get paths
        success, result, base_folder_path = save_file(file, file.filename, transaction_name)
        if not success:
            return jsonify({
                'status': 'error',
                'message': result
            }), 400

        # Test read the file first to check for errors
        try:
            if result.endswith('.xlsx'):
                test_df = pd.read_excel(result, dtype=str)
            elif result.endswith('.csv'):
                test_df = pd.read_csv(result, dtype=str)
            else:
                # Clean up and return error
                os.remove(result)
                os.rmdir(base_folder_path)
                return jsonify({
                    'status': 'error',
                    'message': 'Unsupported file format'
                }), 400
        except Exception as e:
            # Clean up and return error
            os.remove(result)
            os.rmdir(base_folder_path)
            logger.error(f"Error reading file: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error reading the file',
                'details': str(e)
            }), 500

        # Step 1: Create the transaction in the database with base_file_path
        transaction_id = transaction_model.create_transaction(
            user_id=user_id,
            name=transaction_name,
            base_file_path=base_folder_path,
            primary_asset_class=primary_asset_class,
            secondary_asset_class=secondary_asset_class
        )
        if not transaction_id:
            # Clean up the uploaded file if transaction creation failed
            os.remove(result)
            os.rmdir(base_folder_path)
            return jsonify({
                'status': 'error',
                'message': f'The name "{transaction_name}" is already in use. Please choose a different name.'
            }), 400

        # Step 2: Create version for base file
        base_file_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Original uploaded file",
            files_path=result,
            version_number=0
        )

        if not base_file_version_id:
            os.remove(result)
            os.rmdir(base_folder_path)
            transaction_model.delete_transaction(transaction_id)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create base file version'
            }), 500

        # Update transaction with base_file version_id
        transaction_model.set_base_file(transaction_id, base_file_version_id)

        # Step 3: Process the dataset - read again for processing
        try:
            if result.endswith('.xlsx'):
                df = pd.read_excel(result, dtype=str)
            elif result.endswith('.csv'):
                df = pd.read_csv(result, dtype=str)
        except Exception as e:
            logger.error(f"Error reading file for processing: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error reading the file for processing',
                'details': str(e)
            }), 500

        # Step 4: Remove empty rows and columns
        df.dropna(how='all', inplace=True)  # Remove empty rows
        df.dropna(axis=1, how='all', inplace=True)  # Remove empty columns

        # Step 5: Save the preprocessed dataset
        # Get extension
        _, ext = os.path.splitext(result)
        # Create new filename with naming convention
        transaction_name_clean = transaction_name.replace(' ', '_')
        new_filename = f"{transaction_name_clean}_original_preprocessed{ext}"

        # Save the new file in the same transaction folder
        new_file_path = os.path.join(base_folder_path, new_filename)
        
        if ext == '.xlsx':
            df.to_excel(new_file_path, index=False, engine='openpyxl')
        elif ext == '.csv':
            df.to_csv(new_file_path, index=False, encoding='utf-8')

        # Step 6: Create version for preprocessed dataset
        preprocessed_version_id = transaction_version_model.create_version(
            transaction_id=transaction_id,
            description="Preprocessed dataset with cleaned data",
            files_path=new_file_path,
            version_number=1
        )
        
        if not preprocessed_version_id:
            os.remove(new_file_path)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create preprocessed version'
            }), 500

        # Step 7: Update transaction with preprocessed version info
        transaction_model.set_preprocessed_file(transaction_id, preprocessed_version_id)
        
        # Update version number
        transaction_model.update_transaction(
            transaction_id=transaction_id,
            update_fields={
                "version_number": 1
            }
        )

        # Add transaction to user's transactions array
        user_model.add_transaction(user_id, transaction_name, transaction_id)

        return jsonify({
            'status': 'success',
            'message': 'File uploaded, processed, and transaction created successfully',
            'transaction_id': transaction_id,
            'base_file': base_file_version_id,
            'preprocessed_file': preprocessed_version_id
        }), 201

    except Exception as e:
        logger.error(f"Error in upload_dataset: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@transaction_bp.route('/get_transaction_data/<transaction_id>', methods=['GET'])
def get_transaction_data(transaction_id):
    """
    Fetch transaction data, read the file, and return column names and first 10 rows
    
    Args:
        transaction_id (str): ID of the transaction to fetch data for
        
    Returns:
        JSON response with column names and first 10 rows of the file
    """
    try:
        # Fetch transaction details from the database
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404

        # Get the file based on priority order
        if transaction.get('added_new_column_final'):
            version_id = transaction['added_new_column_final']
        elif transaction.get('temp_new_column_adding'):
            version_id = transaction['temp_new_column_adding']
        elif transaction.get('changed_datatype_of_column'):
            version_id = transaction['changed_datatype_of_column']
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
            
        # Fetch version details
        version = transaction_version_model.get_version(version_id)
        if not version:
            return jsonify({
                'status': 'error',
                'message': 'Version not found'
            }), 404
            
        file_path = version.get('files_path')
        
        if not file_path or not os.path.exists(file_path):
            logger.error(f"File does not exist at path: {file_path}")
            return jsonify({
                'status': 'error',
                'message': 'File not found'
            }), 404

        def clean_and_preview(file_path, num_rows=10, is_excel=False):
            """Helper function to clean and preview file content."""
            if is_excel:
                if file_path.endswith('.xlsx'):
                    engine = "openpyxl"
                elif file_path.endswith('.xls'):
                    engine = "xlrd"
                else:
                    raise ValueError("Unsupported Excel file extension")
                df = pd.read_excel(file_path, engine=engine, dtype=str, nrows=num_rows)
            else:
                try:
                    df = pd.read_csv(file_path, dtype=str, nrows=num_rows, encoding="utf-8")
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, dtype=str, nrows=num_rows, encoding="ISO-8859-1")

            # Convert column names to strings to handle datetime objects
            df.columns = [str(col) for col in df.columns]

            # Replace NaN with None for JSON compatibility
            df = df.where(pd.notnull(df), '')

            # Convert all values to strings to ensure JSON serialization
            df = df.astype(str).replace("nan", '')

            # Get total row count
            if is_excel:
                total_rows = len(pd.read_excel(file_path, engine=engine, dtype=str))
            else:
                total_rows = len(pd.read_csv(file_path, dtype=str))

            # Return preview as list of dictionaries
            return df.head(num_rows).to_dict(orient="records"), total_rows

        # Read and preview the file
        try:
            if file_path.endswith(".xlsx"):
                try:
                    rows, total_rows = clean_and_preview(file_path, num_rows=10, is_excel=True)
                except Exception as e:
                    logger.warning(f"Excel read failed, trying CSV fallback: {e}")
                    rows, total_rows = clean_and_preview(file_path, num_rows=10, is_excel=False)
            elif file_path.endswith(".csv"):
                rows, total_rows = clean_and_preview(file_path, num_rows=10, is_excel=False)
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

        # Return the data to the frontend
        return jsonify({
            'status': 'success',
            'columns': list(rows[0].keys()) if rows else [],
            'rows': rows,
            'total_rows': total_rows
        }), 200

    except Exception as e:
        logger.error(f"Error in get_transaction_data: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@transaction_bp.route('/delete_transaction/<transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id):
    """Delete a transaction and its associated files
    
    Args:
        transaction_id (str): ID of the transaction to delete
        
    Returns:
        JSON response with status
    """
    try:
        # Get transaction details before deletion
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
            
        # Delete transaction folder and its contents
        try:
            transaction_folder = transaction['base_file_path']
            if os.path.exists(transaction_folder):
                # Remove all files in the transaction folder
                for file in os.listdir(transaction_folder):
                    file_path = os.path.join(transaction_folder, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                # Remove the transaction folder itself
                os.rmdir(transaction_folder)
        except Exception as e:
            logger.error(f"Error deleting transaction folder: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error deleting transaction folder'
            }), 500
            
        # Delete transaction from database
        success = transaction_model.delete_transaction(transaction_id)
        if success:
            # Remove transaction from user's transactions array
            user_model.remove_transaction(transaction['user_id'], transaction_id)
            
            return jsonify({
                'status': 'success',
                'message': 'Transaction deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete transaction'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_transaction: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@transaction_bp.route('/get_all_transactions/<user_id>', methods=['GET'])
def get_all_transactions(user_id):
    """Fetch all transactions for a given user ID
    
    Args:
        user_id (str): ID of the user whose transactions are to be fetched
        
    Returns:
        JSON response with transaction details including base file location,
        number of rows, total loan amount, and asset classes
    """
    try:
        # Fetch transactions from the database (this now includes base_file_location)
        transactions = transaction_model.get_transactions_by_user(user_id)
        if not transactions:
            return jsonify({
                'status': 'error',
                'message': 'No transactions found for the user'
            }), 404
        
        # Process each transaction
        processed_transactions = []
        for transaction in transactions:
            transaction_data = {
                '_id': transaction['_id'],
                'name': transaction['name'],
                'base_file_path': transaction.get('base_file_path', ''),
                'base_file_location': transaction.get('base_file_location', ''),
                'primary_asset_class': transaction.get('primary_asset_class', ''),
                'secondary_asset_class': transaction.get('secondary_asset_class', ''),
                'version_number': transaction.get('version_number', 0),
                'created_at': transaction.get('created_at'),
                'updated_at': transaction.get('updated_at'),
                'is_processing_done': transaction.get('are_all_steps_complete', False),
                'number_of_rows': 0,
                'total_loan_amount': 0
            }
            
            # Get the latest version by version_number
            from app.utils.db import db
            latest_version = db["transaction_versions"].find_one(
                {"transaction_id": ObjectId(transaction['_id'])},
                sort=[("version_number", -1)]  # Sort by version_number descending
            )
            
            # If we have a version, get row count and loan amount
            if latest_version:
                file_path = latest_version.get('files_path')
                
                if file_path and os.path.exists(file_path):
                    try:
                        # Read file to get row count and loan amount
                        if file_path.endswith(".xlsx"):
                            df = pd.read_excel(file_path, dtype=str)
                        elif file_path.endswith(".csv"):
                            df = pd.read_csv(file_path, dtype=str)
                        else:
                            df = pd.DataFrame()  # Empty dataframe for unsupported formats
                        
                        # Get row count
                        transaction_data['number_of_rows'] = len(df)
                        
                        # Calculate total loan amount if column exists
                        loan_col = None
                        for col in df.columns:
                            if col.lower() in ["loan amount", "loan_amount"]:
                                loan_col = col
                                break
                        
                        if loan_col:
                            try:
                                # Clean and convert loan amount values
                                df[loan_col] = pd.to_numeric(
                                    df[loan_col].str.replace(',', ''), 
                                    errors='coerce'
                                ).fillna(0)
                                transaction_data['total_loan_amount'] = float(df[loan_col].sum())
                            except Exception as e:
                                logger.warning(f"Error calculating loan amount for transaction {transaction['_id']}: {str(e)}")
                                transaction_data['total_loan_amount'] = 0
                                
                    except Exception as e:
                        logger.error(f"Error reading file for transaction {transaction['_id']}: {str(e)}")
                        # Keep default values of 0
                
                # Also update the version_number to reflect the latest
                transaction_data['version_number'] = latest_version.get('version_number', 0)
                
                # Add latest version info for debugging/reference
                transaction_data['latest_version_id'] = str(latest_version['_id'])
                transaction_data['latest_version_description'] = latest_version.get('description', '')
            
            processed_transactions.append(transaction_data)
        
        # Return transaction details
        return jsonify({
            'status': 'success',
            'transactions': processed_transactions
        }), 200
    except Exception as e:
        logger.error(f"Error in get_all_transactions: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@transaction_bp.route('/change-transaction-name', methods=['PUT'])
def change_transaction_name():
    """Change the name of a transaction
    
    Request body:
    {
        "transaction_id": "12345",
        "new_name": "New Transaction Name"
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'transaction_id' not in data or 'new_name' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: transaction_id and new_name'
            }), 400
        
        transaction_id = data['transaction_id']
        new_name = data['new_name']
        
        # Update transaction name
        success = transaction_model.change_transaction_name(transaction_id, new_name)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Transaction name updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update transaction name'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in change_transaction_name: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500
    
@transaction_bp.route('/get_all_data_for_one_transaction/<transaction_id>', methods=['GET'])
def get_all_data_for_one_transaction(transaction_id):
    """
    Fetch complete breakdown of all steps/events for a single transaction
    
    Args:
        transaction_id (str): ID of the transaction
        
    Returns:
        JSON response with chronological breakdown of all transaction steps
    """
    try:
        # Get transaction details
        transaction = transaction_model.get_transaction(transaction_id)
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
        
        # Get all versions for this transaction
        from app.utils.db import db
        all_versions = list(db["transaction_versions"].find(
            {"transaction_id": ObjectId(transaction_id)}
        ).sort("created_at", 1))  # Sort by creation date ascending
        
        breakdown = []
        
        # Process each version and create breakdown
        for version in all_versions:
            version_id = str(version["_id"])
            file_path = version.get("files_path", "")
            
            # Get file stats if file exists
            rows_count = 0
            loan_amount = 0
            if file_path and os.path.exists(file_path):
                try:
                    if file_path.endswith(".xlsx"):
                        df = pd.read_excel(file_path, dtype=str)
                    elif file_path.endswith(".csv"):
                        df = pd.read_csv(file_path, dtype=str)
                    else:
                        df = pd.DataFrame()
                    
                    rows_count = len(df)
                    
                    # Calculate loan amount
                    loan_col = None
                    for col in df.columns:
                        if col.lower() in ["loan amount", "loan_amount"]:
                            loan_col = col
                            break
                    
                    if loan_col:
                        try:
                            df[loan_col] = pd.to_numeric(
                                df[loan_col].str.replace(',', ''), 
                                errors='coerce'
                            ).fillna(0)
                            loan_amount = float(df[loan_col].sum())
                        except:
                            loan_amount = 0
                except Exception as e:
                    logger.warning(f"Error reading file for version {version_id}: {str(e)}")
            
            # Determine step type and create entry
            step_entry = {
                "date": version.get("created_at"),
                "version_id": version_id,
                "version_number": version.get("version_number", 0),
                "description": version.get("description", ""),
                "file_path": file_path,
                "file_name": os.path.basename(file_path) if file_path else "",
                "rows_count": rows_count,
                "loan_amount": loan_amount
            }
            
            # Add step-specific information based on version type
            if version_id == transaction.get("base_file"):
                step_entry["step_name"] = "Original File Upload"
                step_entry["step_type"] = "base_file"
                
            elif version_id == transaction.get("preprocessed_file"):
                step_entry["step_name"] = "Data Preprocessing"
                step_entry["step_type"] = "preprocessing"
                step_entry["details"] = "Removed empty rows and columns"
                
            elif version_id == transaction.get("column_rename_file"):
                step_entry["step_name"] = "Column Mapping"
                step_entry["step_type"] = "column_rename"
                
            elif version_id == transaction.get("temp_changing_datatype_of_column"):
                step_entry["step_name"] = "Datatype Conversion (Temporary)"
                step_entry["step_type"] = "temp_datatype"
                
            elif version_id == transaction.get("changed_datatype_of_column"):
                step_entry["step_name"] = "Datatype Conversion"
                step_entry["step_type"] = "datatype_conversion"
                
            elif version_id == transaction.get("temp_new_column_adding"):
                step_entry["step_name"] = "Add Fields (Temporary)"
                step_entry["step_type"] = "temp_new_columns"
                
            elif version_id == transaction.get("added_new_column_final"):
                step_entry["step_name"] = "Add Fields"
                step_entry["step_type"] = "new_columns_added"
                # Add info about new columns
                new_columns = list(transaction.get("new_added_columns_datatype", {}).keys())
                if new_columns:
                    step_entry["new_columns"] = new_columns
                    
            elif version_id == transaction.get("temp_rbi_rules_applied"):
                step_entry["step_name"] = "RBI Guidelines (Processing)"
                step_entry["step_type"] = "temp_rbi_rules"
                # Add RBI rules metadata
                metadata = version.get("rbi_rules_metadata", {})
                if metadata:
                    step_entry["rules_applied"] = len(metadata.get("rules_applied", []))
                    step_entry["rows_removed"] = metadata.get("total_rows_before", 0) - metadata.get("total_rows_after", 0)
                    
            elif version_id == transaction.get("final_rbi_rules_applied"):
                step_entry["step_name"] = "RBI Guidelines"
                step_entry["step_type"] = "rbi_rules"
                # Add RBI rules metadata
                metadata = version.get("rbi_rules_metadata", {})
                if metadata:
                    step_entry["rules_applied"] = len(metadata.get("rules_applied", []))
                    step_entry["rows_removed"] = metadata.get("total_rows_before", 0) - metadata.get("total_rows_after", 0)
                    step_entry["cutoff_date"] = metadata.get("cutoff_date", transaction.get("cutoff_date"))
                    
            elif version.get("is_rule_application_version"):
                # Rule application versions
                if version.get("branch_number", 0) == 0:
                    step_entry["step_name"] = f"Rule Application - Root Version"
                else:
                    step_entry["step_name"] = f"Rule Application - Branch {version.get('branch_number', 0)}"
                step_entry["step_type"] = "rule_application"
                
                # Add rule details
                rule_info = version.get("rule_applied", {})
                if rule_info:
                    step_entry["rules"] = rule_info.get("rules", [])
                    results = rule_info.get("results", {})
                    if results:
                        step_entry["total_rows_removed"] = results.get("total_rows_removed", 0)
                        step_entry["total_amount_removed"] = results.get("total_amount_removed", 0)
                
                # Add parent info
                if version.get("parent_version_id"):
                    step_entry["parent_version_id"] = str(version["parent_version_id"])
                    
            else:
                step_entry["step_name"] = "Other Processing"
                step_entry["step_type"] = "other"
            
            breakdown.append(step_entry)
        
        # Create summary
        summary = {
            "transaction_id": transaction_id,
            "transaction_name": transaction['name'],
            "primary_asset_class": transaction.get('primary_asset_class', ''),
            "secondary_asset_class": transaction.get('secondary_asset_class', ''),
            "created_at": transaction.get('created_at'),
            "updated_at": transaction.get('updated_at'),
            "total_steps": len(breakdown),
            "is_complete": transaction.get('are_all_steps_complete', False),
            "cutoff_date": transaction.get('cutoff_date'),
            "base_file_path": transaction.get('base_file_path', '')
        }
        
        # Get initial and final stats
        if breakdown:
            summary["initial_rows"] = breakdown[0].get("rows_count", 0)
            summary["final_rows"] = breakdown[-1].get("rows_count", 0)
            summary["initial_loan_amount"] = breakdown[0].get("loan_amount", 0)
            summary["final_loan_amount"] = breakdown[-1].get("loan_amount", 0)
            summary["total_rows_removed"] = summary["initial_rows"] - summary["final_rows"]
            summary["total_amount_removed"] = summary["initial_loan_amount"] - summary["final_loan_amount"]
        
        return jsonify({
            'status': 'success',
            'summary': summary,
            'breakdown': breakdown,
            'timeline_count': len(breakdown)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_all_data_for_one_transaction: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500