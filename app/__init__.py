from flask import Flask
import os
from pymongo import MongoClient
from flask_cors import CORS
from app.utils.logger import logger
from flask import request, make_response


def create_app():
    app = Flask(__name__)
    
    # Load MongoDB configuration from environment variables
    app.config['MONGO_URI'] = os.getenv("MONGO_URI")
    app.config['MONGO_DBNAME'] = os.getenv("MONGO_DBNAME", "your_default_database")
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 

    # Initialize MongoDB client and attach to app
    if app.config['MONGO_URI']:
        app.mongo = MongoClient(app.config['MONGO_URI'])
    else:
        logger.error("MONGO_URI not set in environment variables.")
        raise ValueError("MONGO_URI must be set to connect to MongoDB.")
    
    app.secret_key = os.getenv("SECRET_KEY", "your_default_secret_key")

    # Enhanced CORS configuration for Docker deployment
    # Get allowed origins from environment variable or use defaults
    allowed_origins = os.getenv("CORS_ORIGINS", "http://139.59.26.29,http://139.59.26.29:3000,http://localhost:5173,http://localhost:5174").split(",")
    
    CORS(app, 
         origins=allowed_origins,
         supports_credentials=True,
         allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
         methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         expose_headers=["Content-Range", "X-Content-Range"])

    # Global OPTIONS handler for preflight requests
    @app.before_request
    def handle_preflight():
        if request.method == "OPTIONS":
            response = make_response()
            # Get the origin from environment or default
            origin = request.headers.get('Origin')
            if origin in allowed_origins:
                response.headers.add("Access-Control-Allow-Origin", origin)
            else:
                response.headers.add("Access-Control-Allow-Origin", allowed_origins[0])
            response.headers.add('Access-Control-Allow-Headers', "Content-Type,Authorization,X-Requested-With")
            response.headers.add('Access-Control-Allow-Methods', "GET,POST,PUT,DELETE,OPTIONS")
            response.headers.add('Access-Control-Allow-Credentials', 'true')
            return response

    # Register Blueprints
    from app.blueprints.user import user_bp
    from app.blueprints.auth import auth_bp
    from app.blueprints.admin import admin_bp
    from app.blueprints.project import project_bp
    from app.blueprints.dataset import dataset_bp
    from app.blueprints.transaction import transaction_bp
    from app.blueprints.archive_debt_sheet import archive_debt_sheet_bp
    from app.blueprints.rules_book_debt import rules_book_debt_bp
    from app.blueprints.transaction_dataset import transaction_dataset_bp
    from app.blueprints.archive_transaction import archive_transaction_bp

    app.register_blueprint(user_bp, url_prefix='/api/v1/user')
    app.register_blueprint(auth_bp, url_prefix='/api/v1/auth')
    app.register_blueprint(admin_bp, url_prefix='/api/v1/admin')
    app.register_blueprint(project_bp, url_prefix='/api/v1/project')
    app.register_blueprint(dataset_bp, url_prefix='/api/v1/dataset')
    app.register_blueprint(transaction_bp, url_prefix='/api/v1/transaction')
    app.register_blueprint(archive_debt_sheet_bp, url_prefix='/api/v1/archive_debt_sheet')
    app.register_blueprint(rules_book_debt_bp, url_prefix='/api/v1/rules_book_debt')
    app.register_blueprint(transaction_dataset_bp, url_prefix='/api/v1/transaction_dataset')
    app.register_blueprint(archive_transaction_bp, url_prefix='/api/v1/archive_transaction')


    # Log registered URLs for debugging
    logger.info("Registered URLs:")
    for rule in app.url_map.iter_rules():
        logger.info(rule)

    return app