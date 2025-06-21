import os
import shutil
from flask import request, jsonify
from app.blueprints.archive_debt_sheet import archive_debt_sheet_bp
from app.models.archive_debt_sheet_model import ArchiveDebtSheetModel
from app.models.project_model import ProjectModel
from app.models.user_model import UserModel
from app.models.version_model import VersionModel
from app.utils.logger import logger
from bson import ObjectId

# Initialize models
archive_model = ArchiveDebtSheetModel()
project_model = ProjectModel()
user_model = UserModel()
version_model = VersionModel()

@archive_debt_sheet_bp.route('/send_project_to_archive', methods=['POST'])
def send_project_to_archive():
    """
    Send a project to archive
    
    Request body:
    {
        "user_id": "user_id",
        "project_id": "project_id"
    }
    
    Returns:
        JSON response with status and archive_id
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        user_id = data.get('user_id')
        project_id = data.get('project_id')
        
        if not user_id or not project_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: user_id and project_id'
            }), 400
        
        # Get project data directly from collection to preserve ObjectId types
        project = project_model.collection.find_one({"_id": ObjectId(project_id)})
        if not project:
            return jsonify({
                'status': 'error',
                'message': 'Project not found'
            }), 404
        
        # Verify the project belongs to the user
        if str(project['user_id']) != user_id:
            return jsonify({
                'status': 'error',
                'message': 'Unauthorized: Project does not belong to this user'
            }), 403
        
        # Create archive from project (user_id will remain as ObjectId)
        archive_id = archive_model.create_archive_from_project(project)
        if not archive_id:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create archive'
            }), 500
        
        # Remove project from user's projects array
        user_model.remove_project(user_id, project_id)
        
        # Delete project from projects collection
        project_model.delete_project(project_id)
        
        return jsonify({
            'status': 'success',
            'message': 'Project archived successfully',
            'archive_id': archive_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in send_project_to_archive: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@archive_debt_sheet_bp.route('/revert_project_back_from_archive', methods=['POST'])
def revert_project_back_from_archive():
    """
    Revert a project back from archive
    
    Request body:
    {
        "archive_id": "archive_id"
    }
    
    Returns:
        JSON response with status and project_id
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        archive_id = data.get('archive_id')
        
        if not archive_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: archive_id'
            }), 400
        
        # Get archive data
        archive = archive_model.get_archive(archive_id)
        if not archive:
            return jsonify({
                'status': 'error',
                'message': 'Archive not found'
            }), 404
        
        # Get project data from archive
        project_data = archive_model.restore_archive_to_project(archive_id)
        if not project_data:
            return jsonify({
                'status': 'error',
                'message': 'Failed to restore archive data'
            }), 500
        
        # Create new project from archive data
        project_id = project_model.collection.insert_one(project_data).inserted_id
        project_id = str(project_id)
        
        # Add project back to user's projects array
        user_id = str(project_data['user_id'])
        project_name = project_data.get('name', 'Restored Project')
        user_model.add_project(user_id, project_name, project_id)
        
        # Delete archive
        archive_model.delete_archive(archive_id)
        
        return jsonify({
            'status': 'success',
            'message': 'Project restored successfully',
            'project_id': project_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in revert_project_back_from_archive: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@archive_debt_sheet_bp.route('/get_archives/<user_id>', methods=['GET'])
def get_archives(user_id):
    """
    Get all archive projects for a single user
    
    Args:
        user_id (str): ID of the user
        
    Returns:
        JSON response with archives list
    """
    try:
        # Fetch archives for the user
        archives = archive_model.get_archives_by_user(user_id)
        
        # Process archives to include basic info
        processed_archives = []
        for archive in archives:
            processed_archive = {
                '_id': archive['_id'],
                'name': archive.get('name', 'Unnamed Project'),
                'original_project_id': archive.get('original_project_id', ''),
                'archived_at': archive.get('archived_at', ''),
                'created_at': archive.get('created_at', ''),
                'base_file_path': archive.get('base_file_path', ''),
                'version_number': archive.get('version_number', 0),
                'are_all_steps_complete': archive.get('are_all_steps_complete', False)
            }
            processed_archives.append(processed_archive)
        
        return jsonify({
            'status': 'success',
            'archives': processed_archives
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_archives: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@archive_debt_sheet_bp.route('/delete_permanently_from_archive', methods=['DELETE'])
def delete_permanently_from_archive():
    """
    Delete an archive permanently including all related data
    
    Request body:
    {
        "archive_id": "archive_id"
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        archive_id = data.get('archive_id')
        
        if not archive_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: archive_id'
            }), 400
        
        # Get archive data
        archive = archive_model.get_archive(archive_id)
        if not archive:
            return jsonify({
                'status': 'error',
                'message': 'Archive not found'
            }), 404
        
        # Get all version IDs from the archive
        version_ids = []
        
        # Collect version IDs from various fields
        if archive.get('base_file'):
            version_ids.append(archive['base_file'])
        if archive.get('dataset_after_preprocessing'):
            version_ids.append(archive['dataset_after_preprocessing'])
        if archive.get('file_with_only_renaming_done'):
            version_ids.append(archive['file_with_only_renaming_done'])
        if archive.get('file_with_both_renaming_and_datatype_conversion_done'):
            version_ids.append(archive['file_with_both_renaming_and_datatype_conversion_done'])
        if archive.get('combined_file'):
            version_ids.append(archive['combined_file'])
        if archive.get('temp_datatype_conversion'):
            version_ids.append(archive['temp_datatype_conversion'])
            
        # Collect from split_with_tags
        split_with_tags = archive.get('split_with_tags', {})
        for _, version_id in split_with_tags.items():
            version_ids.append(version_id)
            
        # Collect from temp_files
        temp_files = archive.get('temp_files', [])
        for temp_file in temp_files:
            for _, version_id in temp_file.items():
                version_ids.append(version_id)
                
        # Collect from files_with_rules_applied
        files_with_rules = archive.get('files_with_rules_applied', [])
        for file_entry in files_with_rules:
            for _, version_id in file_entry.items():
                version_ids.append(version_id)
        
        # NEW: Collect from rows_added_files
        rows_added_files = archive.get('rows_added_files', [])
        for file_entry in rows_added_files:
            for _, version_id in file_entry.items():
                version_ids.append(version_id)
        
        # NEW: Collect from rows_removed_files
        rows_removed_files = archive.get('rows_removed_files', [])
        for file_entry in rows_removed_files:
            for _, version_id in file_entry.items():
                version_ids.append(version_id)
        
        # Delete all versions and their files
        deleted_file_count = 0
        deleted_version_count = 0
        
        for version_id in version_ids:
            try:
                version = version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    # Delete the file if it exists
                    file_path = version.get('files_path', '')
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            deleted_file_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete file {file_path}: {str(e)}")
                    
                    # Delete version from database
                    if version_model.delete_version(version_id):
                        deleted_version_count += 1
            except Exception as e:
                logger.error(f"Error deleting version {version_id}: {str(e)}")
        
        # Delete the project folder if it exists
        base_file_path = archive.get('base_file_path', '')
        if base_file_path and os.path.exists(base_file_path):
            try:
                shutil.rmtree(base_file_path)
            except Exception as e:
                logger.warning(f"Failed to delete project folder {base_file_path}: {str(e)}")
        
        # Delete the archive
        success = archive_model.delete_archive(archive_id)
        
        if success:
            logger.info(f"Archive {archive_id} deleted permanently. Deleted {deleted_file_count} files and {deleted_version_count} version records.")
            return jsonify({
                'status': 'success',
                'message': 'Archive deleted permanently',
                'deleted_files': deleted_file_count,
                'deleted_versions': deleted_version_count
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete archive'
            }), 500
        
    except Exception as e:
        logger.error(f"Error in delete_permanently_from_archive: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500