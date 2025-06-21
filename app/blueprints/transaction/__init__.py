# User management component initialization file
# Organizes user-related routes and functionality as a Flask Blueprint

from flask import Blueprint

# Create Blueprint instance for user management routes
# - Name: 'user' (used for URL routing and reverse URL lookups)
# - Package: Current Python package (__name__ provides package context)
transaction_bp = Blueprint('transaction', __name__)

# Import views after blueprint creation to:
# 1. Prevent circular dependency issues
# 2. Ensure blueprint object is available for route decorators
# 3. Maintain proper Flask application architecture patterns
from . import views  # Contains route definitions using @user_bp.route()

# The views module will decorate its routes with @user_bp.route()
# to register them under this blueprint's namespace