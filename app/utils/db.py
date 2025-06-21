from pymongo import MongoClient
from dotenv import load_dotenv
import os
from app.utils.logger import logger

# Load environment variables from .env file for secure configuration
# This keeps credentials out of source code (especially important for production)
load_dotenv()

# Database configuration with fallback defaults for development
# Environment variables override defaults when set (recommended for production)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")  # Default to local MongoDB
DATABASE_NAME = os.getenv("DB_NAME", "mail_whatsapp")  # Default database name

# Debugging output (remove in production or use proper logging)
# Helps verify configuration during development setup
logger.info(f"Connecting to MongoDB with URI: {MONGO_URI}")
logger.info(f"Using database: {DATABASE_NAME}")

# Initialize MongoDB connection client
# Note: MongoClient manages connection pooling automatically
client = MongoClient(MONGO_URI)

# Get database reference
# Important: This is a lightweight operation, doesn't establish actual connection yet
db = client.get_database()  # Defaults to database from URI if none specified