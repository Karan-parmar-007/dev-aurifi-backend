from flask import Blueprint, request, jsonify
from app.models.rules_book_debt_model import RulesBookDebtModel
from app.utils.logger import logger
from app.blueprints.rules_book_debt import rules_book_debt_bp


# Initialize model
rules_book_debt_model = RulesBookDebtModel()

@rules_book_debt_bp.route('/add_rule', methods=['POST'])
def add_rule():
    """
    Add a new rule or update existing rule if update parameter is true
    
    Request body:
    {
        "user_id": "user123",
        "rule_name": "sample_rule_1",
        "rules": [
            [
                {
                    "column": "State",
                    "operator": "equal to",
                    "value": "Gujarat",
                    "connector": "THEN",
                    "then": "accept"
                }
            ]
        ],
        "pin": true/false,
        "tag_name": "tag1",
        "type_of_rule": "insertion/ejection"
    }
    
    Query params:
        - update: true/false (optional)
    
    Returns:
        JSON response with status and rule ID
    """
    try:
        data = request.get_json()
        update = request.args.get('update', 'false').lower() == 'true'
        
        # Validate required fields
        required_fields = ['user_id', 'rule_name', 'rules', 'type_of_rule']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        user_id = data['user_id']
        rule_name = data['rule_name']
        rules = data['rules']
        pin = data.get('pin', False)
        tag_name = data.get('tag_name', '')
        type_of_rule = data['type_of_rule']
        
        # Validate type_of_rule
        if type_of_rule not in ['insertion', 'ejection']:
            return jsonify({
                'status': 'error',
                'message': 'type_of_rule must be either "insertion" or "ejection"'
            }), 400
        
        # Validate rules structure
        if not isinstance(rules, list) or len(rules) == 0:
            return jsonify({
                'status': 'error',
                'message': 'Rules must be a non-empty list'
            }), 400
        
        # Check if rule with same name exists
        existing_rule = rules_book_debt_model.get_rule_by_name(user_id, rule_name)
        
        if existing_rule:
            if update:
                # Update existing rule
                update_data = {
                    'rules': rules,
                    'pin': pin,
                    'tag_name': tag_name,
                    'type_of_rule': type_of_rule
                }
                
                success = rules_book_debt_model.update_rule_by_name(
                    user_id, rule_name, update_data
                )
                
                if success:
                    return jsonify({
                        'status': 'success',
                        'message': 'Rule updated successfully',
                        'rule_id': existing_rule['_id']
                    }), 200
                else:
                    return jsonify({
                        'status': 'error',
                        'message': 'Failed to update rule'
                    }), 500
            else:
                return jsonify({
                    'status': 'error',
                    'message': f'Rule with name "{rule_name}" already exists'
                }), 409
        
        # Create new rule
        rule_id = rules_book_debt_model.create_rule(
            user_id=user_id,
            rule_name=rule_name,
            rules=rules,
            pin=pin,
            tag_name=tag_name,
            type_of_rule=type_of_rule
        )
        
        if rule_id:
            return jsonify({
                'status': 'success',
                'message': 'Rule created successfully',
                'rule_id': rule_id
            }), 201
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create rule'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in add_rule: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@rules_book_debt_bp.route('/get_all_rules/<user_id>', methods=['GET'])
def get_all_rules(user_id):
    """
    Get all saved rules for a user
    
    Args:
        user_id (str): ID of the user
        
    Query params:
        - tag_name: Filter by tag name (optional)
        - pinned_only: true/false - Get only pinned rules (optional)
        
    Returns:
        JSON response with list of rules
    """
    try:
        tag_name = request.args.get('tag_name')
        pinned_only = request.args.get('pinned_only', 'false').lower() == 'true'
        
        if pinned_only:
            rules = rules_book_debt_model.get_pinned_rules(user_id)
        elif tag_name:
            rules = rules_book_debt_model.get_rules_by_tag(user_id, tag_name)
        else:
            rules = rules_book_debt_model.get_all_rules_by_user(user_id)
        
        return jsonify({
            'status': 'success',
            'rules': rules,
            'count': len(rules)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_all_rules: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@rules_book_debt_bp.route('/delete_rule/<rule_id>', methods=['DELETE'])
def delete_rule(rule_id):
    """
    Delete a saved rule by its ID
    
    Args:
        rule_id (str): ID of the rule to delete
        
    Returns:
        JSON response with status
    """
    try:
        # Check if rule exists
        rule = rules_book_debt_model.get_rule_by_id(rule_id)
        if not rule:
            return jsonify({
                'status': 'error',
                'message': 'Rule not found'
            }), 404
        
        # Delete the rule
        success = rules_book_debt_model.delete_rule(rule_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Rule deleted successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete rule'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in delete_rule: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@rules_book_debt_bp.route('/update_rule/<rule_id>', methods=['PUT'])
def update_rule(rule_id):
    """
    Update a rule by its ID
    
    Args:
        rule_id (str): ID of the rule to update
        
    Request body:
    {
        "rule_name": "updated_rule_name",
        "rules": [...],
        "pin": true/false,
        "tag_name": "updated_tag",
        "type_of_rule": "insertion/ejection"
    }
    
    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()
        
        # Check if rule exists
        rule = rules_book_debt_model.get_rule_by_id(rule_id)
        if not rule:
            return jsonify({
                'status': 'error',
                'message': 'Rule not found'
            }), 404
        
        # Prepare update data
        update_data = {}
        
        # Optional fields to update
        if 'rule_name' in data:
            # Check if new name already exists for this user
            if data['rule_name'] != rule['rule_name']:
                existing_rule = rules_book_debt_model.get_rule_by_name(
                    rule['user_id'], data['rule_name']
                )
                if existing_rule:
                    return jsonify({
                        'status': 'error',
                        'message': f'Rule with name "{data["rule_name"]}" already exists'
                    }), 409
            update_data['rule_name'] = data['rule_name']
        
        if 'rules' in data:
            if not isinstance(data['rules'], list) or len(data['rules']) == 0:
                return jsonify({
                    'status': 'error',
                    'message': 'Rules must be a non-empty list'
                }), 400
            update_data['rules'] = data['rules']
        
        if 'pin' in data:
            update_data['pin'] = bool(data['pin'])
        
        if 'tag_name' in data:
            update_data['tag_name'] = data['tag_name']
        
        if 'type_of_rule' in data:
            if data['type_of_rule'] not in ['insertion', 'ejection']:
                return jsonify({
                    'status': 'error',
                    'message': 'type_of_rule must be either "insertion" or "ejection"'
                }), 400
            update_data['type_of_rule'] = data['type_of_rule']
        
        # Update the rule
        success = rules_book_debt_model.update_rule(rule_id, update_data)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Rule updated successfully'
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to update rule'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in update_rule: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500

@rules_book_debt_bp.route('/get_rule/<rule_id>', methods=['GET'])
def get_rule(rule_id):
    """
    Get a specific rule by its ID
    
    Args:
        rule_id (str): ID of the rule
        
    Returns:
        JSON response with rule details
    """
    try:
        rule = rules_book_debt_model.get_rule_by_id(rule_id)
        
        if rule:
            return jsonify({
                'status': 'success',
                'rule': rule
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Rule not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error in get_rule: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred',
            'details': str(e)
        }), 500
    

# Add this to your rules_book_debt views.py file
@rules_book_debt_bp.route('/get_filtered_rules_for_project', methods=['GET'])
def get_filtered_rules_for_project():
    """
    Fetch all saved rules for a user and filter them based on column availability in a project's dataset file.
    
    Query Parameters:
        - user_id: The ID of the user
        - project_id: The ID of the project
        
    Returns:
        JSON response with filtered rules where all columns exist in the project's dataset file
    """
    try:
        # Get query parameters
        user_id = request.args.get('user_id')
        project_id = request.args.get('project_id')
        
        # Validate required parameters
        if not user_id:
            return jsonify({"error": "Missing required parameter: user_id"}), 400
            
        if not project_id:
            return jsonify({"error": "Missing required parameter: project_id"}), 400
        
        # Import required models
        from app.models.project_model import ProjectModel
        from app.models.version_model import VersionModel
        import pandas as pd
        import os
        from bson import ObjectId
        
        # Initialize models
        project_model = ProjectModel()
        version_model = VersionModel()
        
        # 1. Fetch project details
        project = project_model.get_project(project_id)
        if not project:
            return jsonify({"error": "Project not found"}), 404
        
        # 2. Get file path from project - use same priority as other project APIs
        file_path = None
        version_id = None
        
        # Priority order: both renaming and datatype done > only renaming done > preprocessed > base
        if project.get('file_with_both_renaming_and_datatype_conversion_done'):
            version_id = project['file_with_both_renaming_and_datatype_conversion_done']
        elif project.get('file_with_only_renaming_done'):
            version_id = project['file_with_only_renaming_done']
        elif project.get('dataset_after_preprocessing'):
            version_id = project['dataset_after_preprocessing']
        elif project.get('base_file'):
            version_id = project['base_file']
        else:
            return jsonify({"error": "No file associated with project"}), 404
        
        # Get version details
        version = version_model.collection.find_one({"_id": ObjectId(version_id)})
        
        if not version:
            return jsonify({"error": "Version not found"}), 404
            
        file_path = version.get('files_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "Project file not found"}), 404
        
        # 3. Read the file and get column names
        dataset_columns = set()
        try:
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(file_path, dtype=str, nrows=1)  # Read only first row for columns
            elif file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str, nrows=1)  # Read only first row for columns
            else:
                return jsonify({"error": "Unsupported file format"}), 400
                
            dataset_columns = set(df.columns.tolist())
            
        except Exception as e:
            logger.error(f"Error reading project file: {str(e)}")
            return jsonify({"error": "Error reading project file", "details": str(e)}), 500
        
        # 4. Fetch all saved rules for the user
        all_rules = rules_book_debt_model.get_all_rules_by_user(user_id)
        
        # 5. Filter rules based on column availability
        filtered_rules = []
        excluded_rules = []
        
        for rule in all_rules:
            # Check if all columns referenced in the rule exist in the dataset
            rule_valid = True
            columns_not_found = []
            
            for rule_group in rule.get("rules", []):
                for condition in rule_group:
                    column_name = condition.get("column", "").strip()
                    if column_name and column_name not in dataset_columns:
                        rule_valid = False
                        columns_not_found.append(column_name)
            
            # Prepare rule data
            rule_data = {
                "rule_id": rule.get("_id"),
                "rule_name": rule.get("rule_name"),
                "tag_name": rule.get("tag_name"),
                "type_of_rule": rule.get("type_of_rule"),
                "pin": rule.get("pin", False),
                "created_at": rule.get("created_at"),
                "updated_at": rule.get("updated_at")
            }
            
            if rule_valid:
                rule_data["rules"] = rule.get("rules", [])
                filtered_rules.append(rule_data)
            else:
                rule_data["excluded_reason"] = f"Columns not found: {', '.join(columns_not_found)}"
                excluded_rules.append(rule_data)
                logger.info(f"Rule '{rule.get('rule_name')}' excluded for project - columns not found: {columns_not_found}")
        
        # 6. Separate rules by type
        insertion_rules = [r for r in filtered_rules if r.get("type_of_rule") == "insertion"]
        ejection_rules = [r for r in filtered_rules if r.get("type_of_rule") == "ejection"]
        
        return jsonify({
            "status": "success",
            "project_id": project_id,
            "project_name": project.get("name", ""),
            "user_id": user_id,
            "total_rules": len(all_rules),
            "valid_rules_count": len(filtered_rules),
            "excluded_rules_count": len(excluded_rules),
            "dataset_columns": list(dataset_columns),
            "rules": {
                "all_valid": filtered_rules,
                "insertion": insertion_rules,
                "ejection": ejection_rules
            },
            "excluded_rules": excluded_rules  # Include this for debugging/transparency
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_filtered_rules_for_project: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500