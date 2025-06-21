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