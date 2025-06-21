from flask import request, jsonify
from app.blueprints.admin import admin_bp
from app.models.system_column_model import SystemColumnModel
from app.models.system_transaction_columns import SystemTransactionColumnModel 
from app.utils.logger import logger
from app.models.asset_class_model import AssetClassModel


system_column_model = SystemColumnModel()
system_transaction_column_model = SystemTransactionColumnModel()
asset_class_model = AssetClassModel()

@admin_bp.route('/get_system_columns', methods=['GET'])
def get_all_columns():
    """Get all system columns
    
    Returns:
        JSON response with list of all system columns including all their details
    """
    try:
        columns = system_column_model.get_all_columns()
        if columns is not None:
            return jsonify({
                'status': 'success',
                'data': columns
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch system columns'
            }), 500
    except Exception as e:
        logger.error(f"Error in get_all_columns: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/get_system_column/<column_id>', methods=['GET'])
def get_column(column_id):
    """Get a single system column by ID
    
    Args:
        column_id (str): ID of the column to retrieve
        
    Returns:
        JSON response with column data including is_currency field
    """
    try:
        column = system_column_model.get_column(column_id)
        
        if column:
            return jsonify({
                'status': 'success',
                'data': column
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Column not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error in get_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

# views.py - Updated add_column endpoint
@admin_bp.route('/add_system_column', methods=['POST'])
def add_column():
    """Add a new system column
    
    Request body:
    {
        "column_name": "Column Name",
        "description": "Column Description",
        "alt_names": ["alt1", "alt2"],
        "asset_class": "Asset Class",
        "datatype": "DataType",
        "general_mandatory": true/false,
        "is_currency": true/false
    }
    
    Returns:
        JSON response with status and column ID if successful
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['column_name', 'description', 'alt_names', 'asset_class', 'datatype']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Get general_mandatory with default value of False if not provided
        general_mandatory = data.get('general_mandatory', False)
        # Get is_currency with default value of False if not provided
        is_currency = data.get('is_currency', False)
        
        # Create new column
        column_id = system_column_model.create_column(
            column_name=data['column_name'],
            description=data['description'],
            alt_names=data['alt_names'],
            asset_class=data['asset_class'],
            datatype=data['datatype'],
            general_mandatory=general_mandatory,
            is_currency=is_currency
        )
        
        if column_id:
            return jsonify({
                'status': 'success',
                'message': 'Column created successfully',
                'column_id': column_id
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in add_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500


# views.py - Updated update_column endpoint
@admin_bp.route('/update_system_column/<column_id>', methods=['PUT'])
def update_column(column_id):
    """Update a system column
    
    Args:
        column_id (str): ID of the column to update
        
    Request body:
    {
        "column_name": "Updated Name",
        "description": "Updated Description",
        "alt_names": ["new_alt1", "new_alt2"],
        "asset_class": "Updated Asset Class",
        "datatype": "Updated DataType",
        "general_mandatory": true/false,
        "is_currency": true/false
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['column_name', 'description', 'alt_names', 'asset_class', 'datatype']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Include general_mandatory and is_currency in update data
        update_data = {
            'column_name': data['column_name'],
            'description': data['description'],
            'alt_names': data['alt_names'],
            'asset_class': data['asset_class'],
            'datatype': data['datatype'],
            'general_mandatory': data.get('general_mandatory', False),
            'is_currency': data.get('is_currency', False)
        }
        
        # Update column
        success = system_column_model.update_column(column_id, update_data)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Column updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in update_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500


@admin_bp.route('/delete_system_column/<column_id>', methods=['DELETE'])
def delete_column(column_id):
    """Delete a system column
    
    Args:
        column_id (str): ID of the column to delete
        
    Returns:
        JSON response with status
    """
    try:
        success = system_column_model.delete_column(column_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Column deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500 
    
@admin_bp.route('/get_system_column_names', methods=['GET'])
def get_column_names_only():
    """Return only the names of all system columns using the model method
    
    Returns:
        JSON response with list of column names
    """
    try:
        column_names = system_column_model.get_all_column_names()
        if column_names is not None:
            return jsonify({
                'status': 'success',
                'column_names': column_names
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch column names'
            }), 500
    except Exception as e:
        logger.error(f"Error in get_column_names_only: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500


@admin_bp.route('/get_system_transaction_columns', methods=['GET'])
def get_all_transaction_columns():
    """Get all system transaction columns
    
    Returns:
        JSON response with list of all system transaction columns
    """
    try:
        columns = system_transaction_column_model.get_all_columns()
        if columns is not None:
            return jsonify({
                'status': 'success',
                'data': columns
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch system transaction columns'
            }), 500
    except Exception as e:
        logger.error(f"Error in get_all_transaction_columns: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/add_system_transaction_column', methods=['POST'])
def add_transaction_column():
    """Add a new system transaction column
    
    Request body:
    {
        "column_name": "Column Name",
        "description": "Column Description",
        "alt_names": ["alt1", "alt2"],
        "asset_class": "Asset Class",
        "datatype": "DataType",
        "general_mandatory": true/false,
        "is_currency": true/false
    }
    
    Returns:
        JSON response with status and column ID if successful
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['column_name', 'description', 'alt_names', 'asset_class', 'datatype']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Get general_mandatory with default value of False if not provided
        general_mandatory = data.get('general_mandatory', False)
        # Get is_currency with default value of False if not provided
        is_currency = data.get('is_currency', False)
        
        # Create new transaction column
        column_id = system_transaction_column_model.create_column(
            column_name=data['column_name'],
            description=data['description'],
            alt_names=data['alt_names'],
            asset_class=data['asset_class'],
            datatype=data['datatype'],
            general_mandatory=general_mandatory,
            is_currency=is_currency
        )
        
        if column_id:
            return jsonify({
                'status': 'success',
                'message': 'Transaction column created successfully',
                'column_id': column_id
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create transaction column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in add_transaction_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/update_system_transaction_column/<column_id>', methods=['PUT'])
def update_transaction_column(column_id):
    """Update a system transaction column
    
    Args:
        column_id (str): ID of the column to update
        
    Request body:
    {
        "column_name": "Updated Name",
        "description": "Updated Description",
        "alt_names": ["new_alt1", "new_alt2"],
        "asset_class": "Updated Asset Class",
        "datatype": "Updated DataType",
        "general_mandatory": true/false,
        "is_currency": true/false
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['column_name', 'description', 'alt_names', 'asset_class', 'datatype']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Include general_mandatory and is_currency in update data
        update_data = {
            'column_name': data['column_name'],
            'description': data['description'],
            'alt_names': data['alt_names'],
            'asset_class': data['asset_class'],
            'datatype': data['datatype'],
            'general_mandatory': data.get('general_mandatory', False),
            'is_currency': data.get('is_currency', False)
        }
        
        # Update transaction column
        success = system_transaction_column_model.update_column(column_id, update_data)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Transaction column updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update transaction column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in update_transaction_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500


@admin_bp.route('/delete_system_transaction_column/<column_id>', methods=['DELETE'])
def delete_transaction_column(column_id):
    """Delete a system transaction column
    
    Args:
        column_id (str): ID of the column to delete
        
    Returns:
        JSON response with status
    """
    try:
        success = system_transaction_column_model.delete_column(column_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Transaction column deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete transaction column'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_transaction_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500 
    
@admin_bp.route('/get_system_transaction_column_names', methods=['GET'])
def get_transaction_column_names_only():
    """Return only the names of all system transaction columns using the model method
    
    Returns:
        JSON response with list of transaction column names
    """
    try:
        column_names = system_transaction_column_model.get_all_column_names()
        if column_names is not None:
            return jsonify({
                'status': 'success',
                'column_names': column_names
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch transaction column names'
            }), 500
    except Exception as e:
        logger.error(f"Error in get_transaction_column_names_only: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500
    
@admin_bp.route('/get_system_transaction_column/<column_id>', methods=['GET'])
def get_transaction_column(column_id):
    """Get a single system transaction column by ID
    
    Args:
        column_id (str): ID of the column to retrieve
        
    Returns:
        JSON response with column data
    """
    try:
        column = system_transaction_column_model.get_column(column_id)
        
        if column:
            return jsonify({
                'status': 'success',
                'data': column
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Transaction column not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error in get_transaction_column: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500



@admin_bp.route('/get_asset_classes', methods=['GET'])
def get_all_asset_classes():
    """Get all asset classes
    
    Returns:
        JSON response with list of all asset classes
    """
    try:
        asset_classes = asset_class_model.get_all_asset_classes()
        return jsonify({
            'status': 'success',
            'data': asset_classes
        }), 200
    except Exception as e:
        logger.error(f"Error in get_all_asset_classes: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/get_asset_class/<asset_class_id>', methods=['GET'])
def get_asset_class(asset_class_id):
    """Get a single asset class by ID
    
    Args:
        asset_class_id (str): ID of the asset class to retrieve
        
    Returns:
        JSON response with asset class data
    """
    try:
        asset_class = asset_class_model.get_asset_class(asset_class_id)
        
        if asset_class:
            return jsonify({
                'status': 'success',
                'data': asset_class
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Asset class not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error in get_asset_class: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/add_asset_class', methods=['POST'])
def add_asset_class():
    """Add a new asset class
    
    Request body:
    {
        "name": "Asset Class Name"
    }
    
    Returns:
        JSON response with status and asset class ID if successful
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'name' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: name'
            }), 400
        
        name = data['name'].strip()
        if not name:
            return jsonify({
                'status': 'error',
                'message': 'Asset class name cannot be empty'
            }), 400
        
        # Create new asset class
        asset_class_id = asset_class_model.create_asset_class(name)
        
        if asset_class_id:
            return jsonify({
                'status': 'success',
                'message': 'Asset class created successfully',
                'asset_class_id': asset_class_id
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create asset class. Name might already exist.'
            }), 400
            
    except Exception as e:
        logger.error(f"Error in add_asset_class: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/update_asset_class/<asset_class_id>', methods=['PUT'])
def update_asset_class(asset_class_id):
    """Update an asset class
    
    Args:
        asset_class_id (str): ID of the asset class to update
        
    Request body:
    {
        "name": "Updated Asset Class Name"
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'name' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: name'
            }), 400
        
        name = data['name'].strip()
        if not name:
            return jsonify({
                'status': 'error',
                'message': 'Asset class name cannot be empty'
            }), 400
        
        # Update asset class
        success = asset_class_model.update_asset_class(asset_class_id, name)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Asset class updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update asset class. Name might already exist or asset class not found.'
            }), 400
            
    except Exception as e:
        logger.error(f"Error in update_asset_class: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/delete_asset_class/<asset_class_id>', methods=['DELETE'])
def delete_asset_class(asset_class_id):
    """Delete an asset class
    
    Args:
        asset_class_id (str): ID of the asset class to delete
        
    Returns:
        JSON response with status
    """
    try:
        success = asset_class_model.delete_asset_class(asset_class_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Asset class deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete asset class'
            }), 404
            
    except Exception as e:
        logger.error(f"Error in delete_asset_class: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@admin_bp.route('/get_asset_class_names', methods=['GET'])
def get_asset_class_names():
    """Get only the names of all asset classes
    
    Returns:
        JSON response with list of asset class names
    """
    try:
        names = asset_class_model.get_all_asset_class_names()
        return jsonify({
            'status': 'success',
            'asset_class_names': names
        }), 200
    except Exception as e:
        logger.error(f"Error in get_asset_class_names: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500