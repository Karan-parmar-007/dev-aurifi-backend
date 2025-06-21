from app.models.user_model import UserModel
from app.models.otp_model import OTPModel
from app.utils.logger import logger

user_model = UserModel()
otp_model = OTPModel()

def send_otp_to_user(email):
    """Handle sending OTP to user
    
    Args:
        email (str): User's email address
        
    Returns:
        tuple: (success, data, status_code)
    """
    try:
        # Check if user exists
        if not user_model.check_user_exists(email):
            return False, {
                'status': 'error',
                'message': 'User not found'
            }, 404
        
        # Generate and store OTP
        success, otp, message = otp_model.create_or_update_otp(email)
        
        if success:
            # TODO: Implement Brevo API for sending OTP via email
            # For now, print to console
            print(f"=== OTP for {email}: {otp} ===")
            logger.info(f"OTP generated for {email}")
            
            # Send OTP via email service (placeholder for Brevo API)
            email_sent = send_otp_via_email(email, otp)
            
            return True, {
                'status': 'success',
                'message': message,
                'otp': otp  # Remove this in production when email service is implemented
            }, 200
        else:
            status_code = 429 if "blocked" in message.lower() else 400
            return False, {
                'status': 'error',
                'message': message
            }, status_code
            
    except Exception as e:
        logger.error(f"Error in send_otp_to_user: {str(e)}")
        return False, {
            'status': 'error',
            'message': 'An unexpected error occurred while sending OTP'
        }, 500

def verify_user_otp(email, otp):
    """Handle OTP verification
    
    Args:
        email (str): User's email address
        otp (str): OTP to verify
        
    Returns:
        tuple: (success, data, status_code)
    """
    try:
        # Verify OTP
        success, message = otp_model.verify_otp(email, otp)
        
        if success:
            # Get user_id from user model
            user = user_model.collection.find_one({"email": email})
            if user:
                logger.info(f"OTP verified successfully for {email}")
                return True, {
                    'status': 'success',
                    'message': message,
                    'user_id': str(user['_id'])
                }, 200
            else:
                return False, {
                    'status': 'error',
                    'message': 'User not found'
                }, 404
        else:
            status_code = 429 if "blocked" in message.lower() else 400
            return False, {
                'status': 'error',
                'message': message
            }, status_code
            
    except Exception as e:
        logger.error(f"Error in verify_user_otp: {str(e)}")
        return False, {
            'status': 'error',
            'message': 'An unexpected error occurred while verifying OTP'
        }, 500

def send_otp_via_email(email, otp):
    """Send OTP via email service (Brevo API implementation placeholder)
    
    Args:
        email (str): Recipient email address
        otp (str): OTP to send
        
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        # TODO: Implement Brevo API integration here
        # Example structure for Brevo API call:
        
        # import requests
        # 
        # brevo_api_key = "your-brevo-api-key"
        # brevo_url = "https://api.brevo.com/v3/smtp/email"
        # 
        # headers = {
        #     "accept": "application/json",
        #     "content-type": "application/json",
        #     "api-key": brevo_api_key
        # }
        # 
        # payload = {
        #     "sender": {
        #         "email": "noreply@yourdomain.com",
        #         "name": "Your App Name"
        #     },
        #     "to": [
        #         {
        #             "email": email,
        #             "name": "User"
        #         }
        #     ],
        #     "subject": "Your OTP Code",
        #     "htmlContent": f"""
        #         <html>
        #             <body>
        #                 <h2>Your OTP Code</h2>
        #                 <p>Your verification code is: <strong>{otp}</strong></p>
        #                 <p>This code will expire in 15 minutes.</p>
        #                 <p>If you didn't request this code, please ignore this email.</p>
        #             </body>
        #         </html>
        #     """
        # }
        # 
        # response = requests.post(brevo_url, json=payload, headers=headers)
        # 
        # if response.status_code == 201:
        #     logger.info(f"OTP email sent successfully to {email}")
        #     return True
        # else:
        #     logger.error(f"Failed to send OTP email to {email}: {response.text}")
        #     return False
        
        # For now, just log that we would send an email
        logger.info(f"Email service placeholder: Would send OTP {otp} to {email}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending OTP email to {email}: {str(e)}")
        return False

def resend_otp_to_user(email):
    """Handle resending OTP to user (same as send_otp_to_user but with different logging)
    
    Args:
        email (str): User's email address
        
    Returns:
        tuple: (success, data, status_code)
    """
    try:
        logger.info(f"Resending OTP to {email}")
        return send_otp_to_user(email)
        
    except Exception as e:
        logger.error(f"Error in resend_otp_to_user: {str(e)}")
        return False, {
            'status': 'error',
            'message': 'An unexpected error occurred while resending OTP'
        }, 500