from app.blueprints.user import user_bp
from flask import request, jsonify
from app.models.user_model import UserModel
from app.utils.logger import logger

user_model = UserModel()


@user_bp.route('/create_user', methods=['POST'])
def create_user():
    """Create a new user
    
    Request body:
    {
        "name": "User Name",
        "email": "user@example.com",
        "password": "password123"
    }
    
    Returns:
        JSON response with status and message
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['name', 'email', 'password']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Check if user already exists
        if user_model.check_user_exists(data['email']):
            return jsonify({
                'status': 'error',
                'message': 'User with this email already exists'
            }), 409
        
        # Create new user
        user_id = user_model.create_user(
            name=data['name'],
            email=data['email'],
            password=data['password']
        )
        
        if user_id:
            return jsonify({
                'status': 'success',
                'message': 'User created successfully',
                'user_id': user_id
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create user'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in create_user: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500