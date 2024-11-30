import time
from datetime import datetime
import re
import mysql.connector
from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import csv


app = Flask(__name__)
CORS(app)  # Enable CORS for frontend-backend communication

@app.route('/api/hello', methods=['GET'])
def hello():
    return jsonify({"message": "Hello from Flask!"})

MYSQL_CREDS = {
}

DISCORD_TIMESTAMP_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})[\d+]*(\+.*)")


def parse_discord_timestamp(timestamp_str):
    match = DISCORD_TIMESTAMP_REGEX.match(timestamp_str)
    if not match:
        raise ValueError(f"Invalid timestamp format: {timestamp_str}")
    normalized_timestamp = match.group(1) + match.group(2)  # Combine datetime and timezone
    return int(datetime.strptime(normalized_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp())

def process_instagram_data(instagram_file):
    """
    Process Instagram JSON files and extract relevant data.
    """
    data = json.load(instagram_file).get('messages', [])

    instagram_data = tuple([
        (
            message['sender_name'],
            message['timestamp_ms'],
            message['content']
        )
        for message in data
        if (content := message.get('content')) and not str(content).endswith(
            "wasn't notified about this message because they're in quiet mode."
        )
    ])

    return instagram_data

def process_discord_data(discord_file):
    """
    Process Discord CSV file and extract relevant data.
    """
    discord_data = []

    csv_reader = csv.reader(discord_file.stream.read().decode('utf-8').splitlines())
    header = next(csv_reader, None)  # Skip header row
    if not header:
        return jsonify({"message": "Discord CSV file is empty or missing a header."}), 400

    for row in csv_reader:
        try:
            author = row[1]
            timestamp = row[2] # parse_discord_timestamp(row[2])
            content = row[3]
            discord_data.append((author, timestamp, content))
        except (IndexError, ValueError) as e:
            return jsonify({"message": f"Error processing row in Discord file: {row}. Error: {e}"}), 400

    return discord_data



@app.route('/upload', methods=['POST'])
def upload_files():
    # Validate `user_id` (uuid)
    if (user_id := request.form.get('user_id')) is None:
        return jsonify({"message": "No user_id found in the request"}), 400

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
    discord_data = []
    instagram_data = []

    # Process all files in `request.files`
    for key, file in request.files.items():
        print(f"Processing Key: {key}, File: {file.filename}")

        # Handle Instagram JSON files
        if key.startswith('instagram_file_'):
            # Validate file type
            if not file.filename.endswith('.json'):
                return jsonify({"message": f"Invalid file type for {file.filename}. Only .json allowed"}), 400

            try:
                instagram_data.extend(process_instagram_data(file))
            except Exception as e:
                return jsonify({"message": f"Error processing Instagram file {file.filename}: {str(e)}"}), 400


        # Handle Discord CSV file
        elif key == 'discord_file':
            # Validate file type
            if not file.filename.endswith('.csv'):
                return jsonify({"message": f"Invalid file type for {file.filename}. Only .csv allowed"}), 400

            try:
                discord_data = process_discord_data(file)
            except Exception as e:
                return jsonify({"message": f"Error processing Discord file {file.filename}: {str(e)}"}), 400

        # Handle unknown files
        else:
            print(f"Unknown key: {key}")

    # Add data to the database
    con = mysql.connector.connect(**MYSQL_CREDS)
    cur = con.cursor(prepared=True)

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{user_id}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            platform ENUM('discord', 'instagram') NOT NULL,
            user ENUM('1', '2') NOT NULL,
            platform_username VARCHAR(255) NOT NULL,
            timestamp DATETIME NOT NULL,
            message_content TEXT,
            INDEX idx_user (user),
            INDEX idx_platform (platform),
            INDEX idx_timestamp (timestamp)
        );
        """
    )

    # Insert discord data
    discord_data_to_insert = [
        ('1' if users['user1']['discord'] == author else '2', author, timestamp, content)
        for author, timestamp, content in discord_data
    ]

    instagram_data_to_insert = [
        ('1' if users['user1']['instagram'] == author else '2', author, timestamp, content)
        for author, timestamp, content in instagram_data
    ]

    start = time.time()

    cur.executemany(
        f"""
        INSERT INTO `{user_id}` (platform, user, platform_username, timestamp, message_content)
        VALUES 
        ('discord', ?, ?, 
        STR_TO_DATE(
            LEFT(?, 26),  -- Trim extra fractional digits
            '%Y-%m-%dT%H:%i:%s.%f'
        ), 
        ?)
        """,
        discord_data_to_insert
    )

    cur.executemany(
        f"""
            INSERT INTO `{user_id}` (platform, user, platform_username, timestamp, message_content)
            VALUES ('instagram', ?, ?, FROM_UNIXTIME(LEFT(? / 1000, 10)), ?)
            """,
        instagram_data_to_insert
    )
    print(time.time() - start)

    con.commit()
    cur.close()


    # Combine Instagram and Discord data into a response
    return jsonify({
        "message": "Files processed successfully",
        "user_id": user_id,
        "instagram_messages_count": len(instagram_data),
        "discord_messages_count": len(discord_data)
    })


if __name__ == '__main__':
    app.run(debug=True)
