from app import create_app
from dotenv import load_dotenv
import os
from app.utils.logger import logger

# Load environment variables from .backend_env file
load_dotenv()

# Create the Flask app
app = create_app()

def run_app():
    ENV_MODE = os.getenv("FLASK_ENV", "production")
    print(f"FLASK_ENV: {ENV_MODE}")
    debug = ENV_MODE == "development"
    logger.info(f"Running app in {ENV_MODE} mode")
    return app

# This block is for running the app directly (e.g., in development)
if __name__ == '__main__':
    ENV_MODE = os.getenv("FLASK_ENV", "production")
    debug = ENV_MODE == "development"
    # print("ci/cd pipeline is working properly for backend ")
    app.run(host='0.0.0.0', port=5000, debug=debug, use_reloader=True)