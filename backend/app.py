
import mysql.connector
from flask import Flask, jsonify, request
from flask_cors import CORS

from backend.utils import generate_basic_statistics
from utils import process_instagram_data, process_discord_data, create_tables

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend-backend communication

@app.route('/api/hello', methods=['GET'])
def hello():
    return jsonify({"message": "Hello from Flask!"})

MYSQL_CREDS = {
}


@app.route('/upload', methods=['POST'])
def upload_files():
    # Validate `user_id` (uuid) TODO make python generate uuid
    if (user_id := request.form.get('user_id')) is None:
        return jsonify({"message": "No user_id found in the request"}), 400

    # Ensure `timezone` is present in the request TODO do something with timezone
    if (timezone := request.form.get('timezone')) is None:
        return jsonify({"message": "No timezone found in the request"}), 400

    # Ensure required files are present
    if 'discord_file' not in request.files or 'instagram_file_0' not in request.files:
        return jsonify({"message": "No files found in the request"}), 400

    # users[user1/2][platform]
    users = {
        'user1': {'discord': '', 'instagram': ''},
        'user2': {'discord': '', 'instagram': ''}
    }

    # Check if user1 and user2's discord and instagram names have been passed
    for user in ('user1', 'user2'):
        for platform in ('discord', 'instagram'):

            form_data = request.form.get(f'{user}_{platform}')
            if form_data is None:
                return jsonify({"message": f"No {user}_{platform} found in the request"}), 400
            users[user][platform] = form_data

    # Initialize data storage
    data = []

    # Process all files in `request.files`
    for key, file in request.files.items():

        # Handle Instagram JSON files
        if key.startswith('instagram_file_'):
            # Validate file type
            if not file.filename.endswith('.json'):
                return jsonify({"message": f"Invalid file type for {file.filename}. Only .json allowed"}), 400

            try:
                data.extend(process_instagram_data(file))
            except Exception as e:
                return jsonify({"message": f"Error processing Instagram file {file.filename}: {str(e)}"}), 400

        # Handle Discord CSV file
        elif key == 'discord_file':
            # Validate file type
            if not file.filename.endswith('.csv'):
                return jsonify({"message": f"Invalid file type for {file.filename}. Only .csv allowed"}), 400

            try:
                data.extend(process_discord_data(file))
            except Exception as e:
                return jsonify({"message": f"Error processing Discord file {file.filename}: {str(e)}"}), 400

        # Handle unknown files
        else:
            return jsonify({"message": f"Unknown key passed {key}"}), 400

    create_tables()

    generate_basic_statistics(user_id, users, data)

    # Combine Instagram and Discord data into a response
    return jsonify({
        "message": "Files processed successfully",
        "user_id": user_id,
    })


if __name__ == '__main__':
    app.run(debug=True)
