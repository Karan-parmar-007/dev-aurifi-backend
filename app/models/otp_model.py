from app.utils.db import db
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from datetime import datetime, timedelta
import hashlib
import random

class OTPModel:
    """MongoDB model class for handling OTP operations"""
    
    def __init__(self):
        self.collection = db["otps"]
        # Create TTL index for auto-deletion after 15 minutes
        try:
            self.collection.create_index("updated_at", expireAfterSeconds=900)  # 900 seconds = 15 minutes
        except Exception as e:
            logger.error(f"Error creating TTL index: {e}")
    
    def _hash_otp(self, otp):
        """Hash the OTP using SHA-256"""
        return hashlib.sha256(str(otp).encode()).hexdigest()
    
    def generate_otp(self):
        """Generate a 6-digit OTP"""
        return random.randint(100000, 999999)
    
    def create_or_update_otp(self, email):
        """Create new OTP or update existing one
        
        Args:
            email (str): User's email
            
        Returns:
            tuple: (success, otp, message)
        """
        try:
            current_time = datetime.now()
            
            # Check if user is blocked
            existing_otp = self.collection.find_one({"email": email})
            
            if existing_otp:
                # Check if user is blocked (more than 4 attempts and within block period)
                if (existing_otp.get("number_of_time_otp_sent", 0) >= 4 and 
                    existing_otp.get("blocked_until") and 
                    current_time < existing_otp["blocked_until"]):
                    remaining_time = existing_otp["blocked_until"] - current_time
                    minutes_remaining = int(remaining_time.total_seconds() / 60) + 1
                    return False, None, f"Too many OTP requests. User blocked for {minutes_remaining} more minutes"
                
                # If block period has passed, reset the counter
                if (existing_otp.get("blocked_until") and 
                    current_time >= existing_otp["blocked_until"]):
                    # Reset the OTP attempts
                    new_otp = self.generate_otp()
                    hashed_otp = self._hash_otp(new_otp)
                    
                    self.collection.update_one(
                        {"email": email},
                        {
                            "$set": {
                                "otp": hashed_otp,
                                "number_of_time_otp_sent": 1,
                                "exp_time": current_time + timedelta(minutes=15),
                                "updated_at": current_time,
                                "blocked_until": None
                            }
                        }
                    )
                    return True, new_otp, "OTP sent successfully"
            
            # Generate new OTP
            new_otp = self.generate_otp()
            hashed_otp = self._hash_otp(new_otp)
            
            if existing_otp:
                # Update existing record
                new_count = existing_otp.get("number_of_time_otp_sent", 0) + 1
                update_data = {
                    "otp": hashed_otp,
                    "number_of_time_otp_sent": new_count,
                    "exp_time": current_time + timedelta(minutes=15),
                    "updated_at": current_time
                }
                
                # Block user if they've requested OTP more than 4 times
                if new_count > 4:
                    update_data["blocked_until"] = current_time + timedelta(minutes=20)
                    self.collection.update_one({"email": email}, {"$set": update_data})
                    return False, None, "Too many OTP requests. User blocked for 20 minutes"
                
                self.collection.update_one({"email": email}, {"$set": update_data})
            else:
                # Create new record
                otp_data = {
                    "email": email,
                    "otp": hashed_otp,
                    "number_of_time_otp_sent": 1,
                    "exp_time": current_time + timedelta(minutes=15),
                    "inserted_at": current_time,
                    "updated_at": current_time,
                    "blocked_until": None
                }
                self.collection.insert_one(otp_data)
            
            return True, new_otp, "OTP sent successfully"
            
        except PyMongoError as e:
            logger.error(f"Database error while creating/updating OTP: {e}")
            return False, None, "Database error occurred"
    
    def verify_otp(self, email, otp):
        """Verify the OTP
        
        Args:
            email (str): User's email
            otp (str): OTP to verify
            
        Returns:
            tuple: (success, message)
        """
        try:
            current_time = datetime.now()
            hashed_otp = self._hash_otp(otp)
            
            # Find OTP record
            otp_record = self.collection.find_one({"email": email})
            
            if not otp_record:
                return False, "No OTP found for this email. Please request a new OTP"
            
            # Check if user is blocked
            if (otp_record.get("blocked_until") and 
                current_time < otp_record["blocked_until"]):
                remaining_time = otp_record["blocked_until"] - current_time
                minutes_remaining = int(remaining_time.total_seconds() / 60) + 1
                return False, f"User blocked due to too many attempts. Try again in {minutes_remaining} minutes"
            
            # Check if OTP has expired
            if current_time > otp_record["exp_time"]:
                return False, "OTP has expired. Please request a new one"
            
            # Verify OTP
            if otp_record["otp"] == hashed_otp:
                # Delete the OTP record after successful verification
                self.collection.delete_one({"email": email})
                return True, "OTP verified successfully"
            else:
                return False, "Invalid OTP. Please check and try again"
                
        except PyMongoError as e:
            logger.error(f"Database error while verifying OTP: {e}")
            return False, "Database error occurred"
    
    def cleanup_expired_otps(self):
        """Manual cleanup for expired OTPs (backup for TTL index)"""
        try:
            current_time = datetime.now()
            result = self.collection.delete_many({
                "exp_time": {"$lt": current_time},
                "$or": [
                    {"blocked_until": None},
                    {"blocked_until": {"$lt": current_time}}
                ]
            })
            logger.info(f"Cleaned up {result.deleted_count} expired OTP records")
            return result.deleted_count
        except PyMongoError as e:
            logger.error(f"Database error while cleaning up expired OTPs: {e}")
            return 0