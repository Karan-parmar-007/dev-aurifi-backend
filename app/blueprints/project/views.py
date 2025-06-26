import os
import pandas as pd
from flask import request, jsonify, send_file
from app.blueprints.project import project_bp
from app.models.project_model import ProjectModel
from app.models.user_model import UserModel
from app.utils.logger import logger
from werkzeug.utils import secure_filename
from app.models.version_model import VersionModel
from bson import ObjectId
from utils.column_names import (
    DEBTSHEET_LOAN_AMOUNT, 
    DEBTSHEET_TAG_NAME, 
    DEBTSHEET_TAG_TYPE,
    TRANSACTION_LOAN_AMOUNT
)

# Initialize models
project_model = ProjectModel()
user_model = UserModel()

# Configure upload folder
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
        tuple: (bool, str, str) - (success status, file path, base folder path)
    """
    try:
        # Secure the project name
        secure_project_name = secure_filename(project_name)
        # Replace spaces with underscores
        secure_project_name = secure_project_name.replace(' ', '_')
        
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
        secure_name = secure_filename(f"{secure_project_name}_original{ext}")
        # Ensure no spaces in filename
        secure_name = secure_name.replace(' ', '_')
        
        # Create file path within project folder
        file_path = os.path.join(project_folder, secure_name)
        
        # Save the file
        file.save(file_path)
        return True, file_path, project_folder  # Return base folder path too
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        return False, "Error saving file", None

@project_bp.route('/upload_dataset', methods=['POST'])
def upload_dataset():
    """Upload a file, create a project, process the dataset, and manage versions."""
    try:
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
        name = request.form.get('name')
        user_id = request.form.get('user_id')
        remove_duplicates = request.form.get('remove_duplicates', 'false').lower() == 'true'

        # Validate required fields
        if not all([name, user_id]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: name and user_id'
            }), 400

        # Save file and get paths
        success, result, base_folder_path = save_file(file, file.filename, name)
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

        # Step 1: Create the project in the database with base_file_path
        project_id = project_model.create_project(
            user_id=user_id,
            name=name,
            base_file_path=base_folder_path,  # Store base folder path
            remove_duplicates=remove_duplicates
        )
        if not project_id:
            # Clean up the uploaded file if project creation failed
            os.remove(result)
            os.rmdir(base_folder_path)
            return jsonify({
                'status': 'error',
                'message': f'The name "{name}" is already in use. Please choose a different name.'
            }), 400

        # Step 2: Create version for base file
        version_model = VersionModel()
        base_file_version_id = version_model.create_version(
            project_id=project_id,
            description="Original uploaded file",
            files_path=result,
            version_number=0
        )

        if not base_file_version_id:
            os.remove(result)
            os.rmdir(base_folder_path)
            project_model.delete_project(project_id)
            return jsonify({
                'status': 'error',
                'message': 'Failed to create base file version'
            }), 500

        # Update project with base_file version_id
        project_model.set_base_file(project_id, base_file_version_id)

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

        # Step 4: Remove empty rows
        df.dropna(how='all', inplace=True)

        # Step 5: Remove duplicates if required
        if remove_duplicates:
            df.drop_duplicates(inplace=True)

        # Step 6: Save the preprocessed dataset
        # Get extension
        _, ext = os.path.splitext(result)
        # Create new filename with naming convention
        project_name_clean = name.replace(' ', '_')
        new_filename = f"{project_name_clean}_original_preprocessed{ext}"

        # Save the new file in the same project folder
        new_file_path = os.path.join(base_folder_path, new_filename)
        
        if ext == '.xlsx':
            df.to_excel(new_file_path, index=False, engine='openpyxl')
        elif ext == '.csv':
            df.to_csv(new_file_path, index=False, encoding='utf-8')

        # Step 7: Create version for preprocessed dataset
        preprocessed_version_id = version_model.create_version(
            project_id=project_id,
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

        # Step 8: Update project with preprocessed version info
        project_model.set_dataset_after_preprocessing(project_id, preprocessed_version_id)
        
        # Update version number
        project_model.update_all_fields(
            project_id=project_id,
            update_fields={
                "version_number": 1
            }
        )
        
        # NEW: Mark dataset upload step as complete
        project_model.update_step_status(project_id, "dataset_uploaded", True)
        project_model.update_current_step(project_id, "header_mapping")

        # Add project to user's projects array
        user_model.add_project(user_id, name, project_id)

        return jsonify({
            'status': 'success',
            'message': 'File uploaded, processed, and project created successfully',
            'project_id': project_id,
            'base_file': base_file_version_id,
            'dataset_after_preprocessing': preprocessed_version_id,
            'next_step': 'header_mapping'
        }), 201

    except Exception as e:
        logger.error(f"Error in upload_dataset: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/update_project/<project_id>', methods=['PUT'])
def update_project(project_id):
    """
    Update project datatype mapping
    NOTE: This endpoint is deprecated as datatype_mapping is no longer used
    
    Args:
        project_id (str): ID of the project to update
        
    Request body:
    {
        "datatype_mapping": [
            {"column_name": "col1", "datatype": "string"},
            {"column_name": "col2", "datatype": "number"}
        ]
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Log deprecation warning
        logger.warning("update_project endpoint called - datatype_mapping is deprecated")
        
        # Return success for backward compatibility
        return jsonify({
            'status': 'success',
            'message': 'Project updated successfully (datatype_mapping is deprecated)'
        }), 200
            
    except Exception as e:
        logger.error(f"Error in update_project: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@project_bp.route('/delete_project/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete a project and its associated files
    
    Args:
        project_id (str): ID of the project to delete
        
    Returns:
        JSON response with status
    """
    try:
        # Get project details before deletion
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404
            
        # Delete project folder and its contents
        try:
            project_folder = project['base_file_path']
            if os.path.exists(project_folder):
                # Remove all files in the project folder
                for file in os.listdir(project_folder):
                    file_path = os.path.join(project_folder, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                # Remove the project folder itself
                os.rmdir(project_folder)
        except Exception as e:
            logger.error(f"Error deleting project folder: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Error deleting project folder'
            }), 500
            
        # Delete project from database
        success = project_model.delete_project(project_id)
        if success:
            # Remove project from user's projects array
            user_model.remove_project(project['user_id'], project_id)
            
            return jsonify({
                'status': 'success',
                'message': 'Project deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete project'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_project: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@project_bp.route('/get_projects/<user_id>', methods=['GET'])
def get_projects(user_id):
    """Fetch all projects for a given user ID with file data if processing is complete
    
    Args:
        user_id (str): ID of the user whose projects are to be fetched
        
    Returns:
        JSON response with project details and file data if processing is complete
    """
    try:
        # Fetch projects from the database
        projects = project_model.get_projects_by_user(user_id)
        if not projects:
            return jsonify({
                'status': 'error',
                'message': 'No projects found for the user'
            }), 404
        
        # Initialize version model
        version_model = VersionModel()
        
        # Process each project
        processed_projects = []
        for project in projects:
            project_data = {
                '_id': project['_id'],
                'name': project['name'],
                'base_file_path': project.get('base_file_path', ''),
                'remove_duplicates': project.get('remove_duplicates', False),
                'version_number': project.get('version_number', 0),
                'created_at': project.get('created_at'),
                'updated_at': project.get('updated_at'),
                'is_processing_done': project.get('are_all_steps_complete', False)
            }
            
            # If processing is complete, fetch file data
            if project.get('are_all_steps_complete', False):
                file_data = {}
                
                # [existing code for fetching files_with_rules_applied data...]
                # Fetch data from files_with_rules_applied
                files_with_rules = project.get('files_with_rules_applied', [])
                for file_entry in files_with_rules:
                    for tag_name, version_id in file_entry.items():
                        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                        if version:
                            file_path = version.get('files_path', '')
                            if file_path and os.path.exists(file_path):
                                try:
                                    # Read file data
                                    if file_path.endswith('.xlsx'):
                                        df = pd.read_excel(file_path, dtype=str)
                                    elif file_path.endswith('.csv'):
                                        df = pd.read_csv(file_path, dtype=str)
                                    else:
                                        continue
                                    
                                    # Calculate loan amount total if column exists
                                    loan_amount_total = 0
                                    if DEBTSHEET_LOAN_AMOUNT in df.columns:
                                        loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors='coerce').sum()
                                        loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                                    
                                    # Store file data
                                    file_data[tag_name] = {
                                        'version_id': str(version_id),
                                        'file_path': file_path,
                                        'rows_count': len(df),
                                        'loan_amount_total': loan_amount_total,
                                        'tag_type': version.get('tag_type_name', ''),
                                        'description': version.get('description', ''),
                                        'rows_added': version.get('rows_added', 0),
                                        'rows_removed': version.get('rows_removed', 0),
                                        'modified': version.get('modified', False)
                                    }
                                except Exception as e:
                                    logger.error(f"Error reading file {file_path}: {str(e)}")
                
                # [existing code for fetching combined file data...]
                # Fetch combined file data
                if project.get('combined_file'):
                    combined_version = version_model.collection.find_one({"_id": ObjectId(project['combined_file'])})
                    if combined_version:
                        file_path = combined_version.get('files_path', '')
                        if file_path and os.path.exists(file_path):
                            try:
                                # Read combined file
                                if file_path.endswith('.xlsx'):
                                    df = pd.read_excel(file_path, dtype=str)
                                elif file_path.endswith('.csv'):
                                    df = pd.read_csv(file_path, dtype=str)
                                else:
                                    df = None
                                
                                if df is not None:
                                    # Calculate loan amount total
                                    loan_amount_total = 0
                                    if DEBTSHEET_LOAN_AMOUNT in df.columns:
                                        loan_amount_total = pd.to_numeric(df[DEBTSHEET_LOAN_AMOUNT], errors='coerce').sum()
                                        loan_amount_total = float(loan_amount_total) if not pd.isna(loan_amount_total) else 0
                                    
                                    file_data['combined_file'] = {
                                        'version_id': str(project['combined_file']),
                                        'file_path': file_path,
                                        'rows_count': len(df),
                                        'loan_amount_total': loan_amount_total,
                                        'description': combined_version.get('description', ''),
                                        'total_amount': combined_version.get('total_amount', 0)
                                    }
                            except Exception as e:
                                logger.error(f"Error reading combined file {file_path}: {str(e)}")
                
                # Add file data to project data
                project_data['file_data'] = file_data
            else:
                # If processing is NOT complete, fetch the original file path
                base_file_version_id = project.get('base_file')
                if base_file_version_id:
                    base_version = version_model.collection.find_one({"_id": ObjectId(base_file_version_id)})
                    if base_version:
                        original_file_path = base_version.get('files_path', '')
                        project_data['original_file_path'] = original_file_path
                    else:
                        project_data['original_file_path'] = None
                else:
                    project_data['original_file_path'] = None
            
            processed_projects.append(project_data)
        
        # Return project details
        return jsonify({
            'status': 'success',
            'projects': processed_projects
        }), 200
    except Exception as e:
        logger.error(f"Error in get_projects: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/get_project_data/<project_id>', methods=['GET'])
def get_project_data(project_id):
    """
    Fetch project data, read the file, and return column names and first 10 rows
    
    Args:
        project_id (str): ID of the project to fetch data for
        
    Returns:
        JSON response with column names and first 10 rows of the file
    """
    try:
        # Fetch project details from the database
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404

        # Get the preprocessed file if available, otherwise use base file
        version_model = VersionModel()
        
        # Priority order: both renaming and datatype done > only renaming done > preprocessed > base
        if project.get('file_with_both_renaming_and_datatype_conversion_done'):
            version_id = project['file_with_both_renaming_and_datatype_conversion_done']
        elif project.get('file_with_only_renaming_done'):
            version_id = project['file_with_only_renaming_done']
        elif project.get('dataset_after_preprocessing'):
            version_id = project['dataset_after_preprocessing']
        elif project.get('base_file'):
            version_id = project['base_file']
        else:
            return jsonify({
                'status': 'error',
                'message': 'No file associated with project'
            }), 404
            
        # Fetch version details
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
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

            # Return preview as list of dictionaries
            return df.head(num_rows).to_dict(orient="records")

        # Read and preview the file
        try:
            if file_path.endswith(".xlsx"):
                try:
                    rows = clean_and_preview(file_path, num_rows=10, is_excel=True)
                except Exception as e:
                    logger.warning(f"Excel read failed, trying CSV fallback: {e}")
                    rows = clean_and_preview(file_path, num_rows=10, is_excel=False)
            elif file_path.endswith(".csv"):
                rows = clean_and_preview(file_path, num_rows=10, is_excel=False)
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
            'rows': rows
        }), 200

    except Exception as e:
        logger.error(f"Error in get_project_data: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500


@project_bp.route('/download_file', methods=['GET'])
def download_file():
    """Download a file from the server using file_path from query parameter
    
    Returns:
        File response or error message
    """
    try:
        # Get file path from query parameter instead of URL path
        file_path = request.args.get('file_path')
        
        if not file_path:
            return jsonify({
                'status': 'error',
                'message': 'Missing file_path parameter'
            }), 400
        
        # If running in Docker, ensure we're using container paths
        if file_path.startswith('C:\\') or file_path.startswith('/Users/'):
            # This is a local development path, need to convert to container path
            # Extract just the dataset-relative path
            if 'datasets' in file_path:
                parts = file_path.split('datasets')[-1]
                parts = parts.replace('\\', '/').strip('/')
                file_path = f'/app/datasets/{parts}'
        
        # Normalize the file path
        normalized_path = os.path.normpath(file_path)
        
        # Security check - ensure the path is within the datasets directory
        datasets_dir = os.path.join('/app', 'datasets')
        if not normalized_path.startswith(datasets_dir):
            return jsonify({
                'status': 'error',
                'message': 'Invalid file path'
            }), 403

        # Ensure the file exists
        if not os.path.exists(normalized_path):
            logger.error(f"File not found at path: {normalized_path}")
            return jsonify({
                'status': 'error',
                'message': 'File not found',
                'path_tried': normalized_path
            }), 404
        
        # Get the filename for download
        filename = os.path.basename(normalized_path)
        
        # Send the file for download
        return send_file(
            normalized_path, 
            as_attachment=True,
            download_name=filename,
            mimetype='application/octet-stream'
        )
    except Exception as e:
        logger.error(f"Error in download_file: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/get_datatype_mapping/<project_id>', methods=['GET'])
def get_datatype_mapping(project_id):
    """
    Fetch the datatype mapping from system columns.
    
    Args:
        project_id (str): ID of the project
    Returns:
        JSON response with column names and their datatypes from system columns,
        plus a list of currency columns
    """
    try:
        # Validate that the project exists
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404
        
        # Get all system columns
        from app.models.system_column_model import SystemColumnModel
        system_column_model = SystemColumnModel()
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

@project_bp.route('/change-project-name', methods=['PUT'])
def change_project_name():
    """Change the name of a project
    
    Request body:
    {
        "project_id": "12345",
        "new_name": "New Project Name"
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'project_id' not in data or 'new_name' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: project_id and new_name'
            }), 400
        
        project_id = data['project_id']
        new_name = data['new_name']
        
        # Update project name
        success = project_model.change_project_name(project_id, new_name)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Project name updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update project name'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in change_project_name: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500


@project_bp.route('/get_project_navigation/<project_id>', methods=['GET'])
def get_project_navigation(project_id):
    """Get the next step for project navigation
    
    Args:
        project_id (str): ID of the project
        
    Returns:
        JSON response with navigation information
    """
    try:
        navigation_info = project_model.get_next_step(project_id)
        
        if navigation_info.get("error"):
            return jsonify({
                'status': 'error',
                'message': navigation_info["error"]
            }), 404
            
        return jsonify({
            'status': 'success',
            'navigation': navigation_info
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_project_navigation: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/reset_project_steps/<project_id>', methods=['POST'])
def reset_project_steps(project_id):
    """Reset project steps from a specific step
    
    Request body:
    {
        "from_step": "split_by_tags_done"
    }
    
    Args:
        project_id (str): ID of the project
        
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        from_step = data.get("from_step")
        
        if not from_step:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: from_step'
            }), 400
            
        success = project_model.reset_steps_from(project_id, from_step)
        
        if success:
            # Get next navigation info
            navigation_info = project_model.get_next_step(project_id)
            
            return jsonify({
                'status': 'success',
                'message': f'Steps reset from {from_step}',
                'navigation': navigation_info
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to reset steps'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in reset_project_steps: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@project_bp.route('/get_project_progress/<project_id>', methods=['GET'])
def get_project_progress(project_id):
    """Get detailed progress information for a project
    
    Args:
        project_id (str): ID of the project
        
    Returns:
        JSON response with detailed progress information
    """
    try:
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404
            
        steps_completed = project.get("steps_completed", {})
        temp_steps = project.get("temp_steps", {})
        current_step = project.get("current_step", "upload_dataset")
        
        # Calculate progress percentage
        total_steps = len(steps_completed)
        completed_steps = sum(1 for v in steps_completed.values() if v)
        progress_percentage = int((completed_steps / total_steps) * 100) if total_steps > 0 else 0
        
        return jsonify({
            'status': 'success',
            'progress': {
                'steps_completed': steps_completed,
                'temp_steps': temp_steps,
                'current_step': current_step,
                'progress_percentage': progress_percentage,
                'total_steps': total_steps,
                'completed_count': completed_steps,
                'is_complete': project.get("are_all_steps_complete", False)
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_project_progress: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500
