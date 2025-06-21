# archive_transaction_views.py
import os
import shutil
from flask import request, jsonify
from app.blueprints.archive_transaction import archive_transaction_bp
from app.models.archive_transaction_model import ArchiveTransactionModel
from app.models.transaction_model import TransactionModel
from app.models.user_model import UserModel
from app.models.transaction_version_model import TransactionVersionModel
from app.utils.logger import logger
from bson import ObjectId

# Initialize models
archive_model = ArchiveTransactionModel()
transaction_model = TransactionModel()
user_model = UserModel()
transaction_version_model = TransactionVersionModel()

@archive_transaction_bp.route('/send_transaction_to_archive', methods=['POST'])
def send_transaction_to_archive():
    """
    Send a transaction to archive
    
    Request body:
    {
        "user_id": "user_id",
        "transaction_id": "transaction_id"
    }
    
    Returns:
        JSON response with status and archive_id
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        user_id = data.get('user_id')
        transaction_id = data.get('transaction_id')
        
        if not user_id or not transaction_id:
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: user_id and transaction_id'
            }), 400
        
        # Get transaction data directly from collection to preserve ObjectId types
        transaction = transaction_model.collection.find_one({"_id": ObjectId(transaction_id)})
        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404
        
        # Verify the transaction belongs to the user
        if str(transaction['user_id']) != user_id:
            return jsonify({
                'status': 'error',
                'message': 'Unauthorized: Transaction does not belong to this user'
            }), 403
        
        # Create archive from transaction (user_id will remain as ObjectId)
        archive_id = archive_model.create_archive_from_transaction(transaction)
        if not archive_id:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create archive'
            }), 500
        
        # Remove transaction from user's transactions array
        user_model.remove_transaction(user_id, transaction_id)
        
        # Delete transaction from transactions collection
        transaction_model.delete_transaction(transaction_id)
        
        return jsonify({
            'status': 'success',
            'message': 'Transaction archived successfully',
            'archive_id': archive_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in send_transaction_to_archive: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@archive_transaction_bp.route('/revert_transaction_back_from_archive', methods=['POST'])
def revert_transaction_back_from_archive():
    """
    Revert a transaction back from archive
    
    Request body:
    {
        "archive_id": "archive_id"
    }
    
    Returns:
        JSON response with status and transaction_id
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
        
        # Get transaction data from archive
        transaction_data = archive_model.restore_archive_to_transaction(archive_id)
        if not transaction_data:
            return jsonify({
                'status': 'error',
                'message': 'Failed to restore archive data'
            }), 500
        
        # Create new transaction from archive data
        transaction_id = transaction_model.collection.insert_one(transaction_data).inserted_id
        transaction_id = str(transaction_id)
        
        # Add transaction back to user's transactions array
        user_id = str(transaction_data['user_id'])
        transaction_name = transaction_data.get('name', 'Restored Transaction')
        user_model.add_transaction(user_id, transaction_name, transaction_id)
        
        # Delete archive
        archive_model.delete_archive(archive_id)
        
        return jsonify({
            'status': 'success',
            'message': 'Transaction restored successfully',
            'transaction_id': transaction_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error in revert_transaction_back_from_archive: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@archive_transaction_bp.route('/get_transaction_archives/<user_id>', methods=['GET'])
def get_transaction_archives(user_id):
    """
    Get all archive transactions for a single user
    
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
                'name': archive.get('name', 'Unnamed Transaction'),
                'original_transaction_id': archive.get('original_transaction_id', ''),
                'archived_at': archive.get('archived_at', ''),
                'created_at': archive.get('created_at', ''),
                'base_file_path': archive.get('base_file_path', ''),
                'version_number': archive.get('version_number', 0),
                'are_all_steps_complete': archive.get('are_all_steps_complete', False),
                'primary_asset_class': archive.get('primary_asset_class', ''),
                'secondary_asset_class': archive.get('secondary_asset_class', '')
            }
            processed_archives.append(processed_archive)
        
        return jsonify({
            'status': 'success',
            'archives': processed_archives
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_transaction_archives: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@archive_transaction_bp.route('/delete_permanently_from_archive', methods=['DELETE'])
def delete_permanently_from_archive():
    """
    Delete a transaction archive permanently including all related data
    
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
            
        # Collect from version_info
        version_info = archive.get('version_info', [])
        for version_entry in version_info:
            for _, version_id in version_entry.items():
                version_ids.append(version_id)
        
        # Delete all versions and their files
        for version_id in version_ids:
            try:
                version = transaction_version_model.collection.find_one({"_id": ObjectId(version_id)})
                if version:
                    # Delete the file if it exists
                    file_path = version.get('files_path', '')
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            logger.warning(f"Failed to delete file {file_path}: {str(e)}")
                    
                    # Delete version from database
                    transaction_version_model.delete_version(version_id)
            except Exception as e:
                logger.error(f"Error deleting version {version_id}: {str(e)}")
        
        # Delete the transaction folder if it exists
        base_file_path = archive.get('base_file_path', '')
        if base_file_path and os.path.exists(base_file_path):
            try:
                shutil.rmtree(base_file_path)
            except Exception as e:
                logger.warning(f"Failed to delete transaction folder {base_file_path}: {str(e)}")
        
        # Delete the archive
        success = archive_model.delete_archive(archive_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Archive deleted permanently'
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