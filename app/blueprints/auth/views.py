from flask import request, jsonify
from app.blueprints.auth import auth_bp
from app.models.user_model import UserModel
from app.utils.handle_otp import send_otp_to_user, verify_user_otp, resend_otp_to_user
from app.utils.logger import logger

user_model = UserModel()

@auth_bp.route('/send_otp', methods=['POST'])
def send_otp():
    """Send OTP to user's email
    
    Request body:
    {
        "email": "user@example.com"
    }
    
    Returns:
        JSON response with status and message
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'email' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: email'
            }), 400
        
        email = data['email']
        
        # Validate email format (basic validation)
        if '@' not in email or '.' not in email:
            return jsonify({
                'status': 'error',
                'message': 'Invalid email format'
            }), 400
        
        # Call handle_otp function
        success, response_data, status_code = send_otp_to_user(email)
        
        return jsonify(response_data), status_code
            
    except Exception as e:
        logger.error(f"Error in send_otp: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@auth_bp.route('/verify_otp', methods=['POST'])
def verify_otp():
    """Verify OTP and return user_id
    
    Request body:
    {
        "email": "user@example.com",
        "otp": "123456"
    }
    
    Returns:
        JSON response with status, message, and user_id if successful
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['email', 'otp']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        email = data['email']
        otp = str(data['otp'])  # Ensure OTP is string
        
        # Validate OTP format (6 digits)
        if not otp.isdigit() or len(otp) != 6:
            return jsonify({
                'status': 'error',
                'message': 'OTP must be a 6-digit number'
            }), 400
        
        # Call handle_otp function
        success, response_data, status_code = verify_user_otp(email, otp)
        
        return jsonify(response_data), status_code
            
    except Exception as e:
        logger.error(f"Error in verify_otp: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500

@auth_bp.route('/resend_otp', methods=['POST'])
def resend_otp():
    """Resend OTP to user's email
    
    Request body:
    {
        "email": "user@example.com"
    }
    
    Returns:
        JSON response with status and message
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if 'email' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: email'
            }), 400
        
        email = data['email']
        
        # Validate email format (basic validation)
        if '@' not in email or '.' not in email:
            return jsonify({
                'status': 'error',
                'message': 'Invalid email format'
            }), 400
        
        # Call handle_otp function
        success, response_data, status_code = resend_otp_to_user(email)
        
        return jsonify(response_data), status_code
            
    except Exception as e:
        logger.error(f"Error in resend_otp: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred'
        }), 500