import os
from openai import OpenAI
from flask import current_app

def send_to_openai_assistant(input_data):
    """
    Sends input data to an Open AI assistant using environment variables for assistant ID and API key.
    
    Args:
        input_data (dict): Input data containing system_columns and uploaded_columns
        
    Returns:
        dict: Response from the Open AI assistant or error message
    """
    try:
        # Retrieve assistant ID and API key from environment variables
        assistant_id = os.getenv('OPENAI_ASSISTANT_ID')
        api_key = os.getenv('OPENAI_API_KEY')
        
        if not assistant_id or not api_key:
            raise ValueError("OPENAI_ASSISTANT_ID or OPENAI_API_KEY not set in environment")
        
        # Initialize Open AI client
        client = OpenAI(api_key=api_key)
        
        # Create a new thread
        thread = client.beta.threads.create()
        
        # Send the input data as a message in the thread
        message = client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=str(input_data) 
        )
        
        # Run the assistant
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant_id
        )
        
        # Poll for the run completion
        while run.status != "completed":
            run = client.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )
        
        # Retrieve the assistant's response
        messages = client.beta.threads.messages.list(thread_id=thread.id)
        assistant_response = messages.data[0].content[0].text.value
        
        return {
            "status": "success",
            "response": assistant_response
        }
        
    except Exception as e:
        current_app.logger.error(f"Error communicating with Open AI assistant: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to get response from assistant: {str(e)}"
        }