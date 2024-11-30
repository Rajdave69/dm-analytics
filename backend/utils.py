import re
from datetime import datetime
import csv

import mysql.connector
from flask import jsonify
import json

DISCORD_TIMESTAMP_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})[\d+]*(\+.*)")


def parse_discord_timestamp(timestamp_str):
    match = DISCORD_TIMESTAMP_REGEX.match(timestamp_str)
    if not match:
        raise ValueError(f"Invalid timestamp format: {timestamp_str}")
    normalized_timestamp = match.group(1) + match.group(2)  # Combine datetime and timezone
    return int(datetime.strptime(normalized_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp())

def process_instagram_data(instagram_file) -> tuple[tuple[str, str, int, str]]:
    """
    Process Instagram JSON files and extract relevant data.
    """
    data = json.load(instagram_file).get('messages', [])

    instagram_data = tuple([
        (
            'instagram',
            message['sender_name'],
            int(message['timestamp_ms'] / 1000),
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
            timestamp = parse_discord_timestamp(row[2])
            content = row[3]
            discord_data.append(('discord', author, timestamp, content))
        except (IndexError, ValueError) as e:
            return jsonify({"message": f"Error processing row in Discord file: {row}. Error: {e}"}), 400

    return discord_data


def create_tables():
    con = mysql.connector.connect(**MYSQL_CREDS)
    cur = con.cursor(prepared=True)

    cur.execute(
        f"""
            CREATE TABLE IF NOT EXISTS `basic_stats` (
                uuid TEXT NOT NULL,
                platform TEXT NOT NULL,
                user TEXT NOT NULL,
                message_count BIGINT NOT NULL,
                character_count BIGINT NOT NULL,
                word_count BIGINT NOT NULL,
                average_message_length FLOAT NOT NULL,
                avg_words_per_message FLOAT NOT NULL,
                avg_word_length FLOAT NOT NULL
            );
            """
    )



    #                 INDEX idx_user (user),
    #                 INDEX idx_platform (platform),
    #                 INDEX idx_stat (stat)

    con.commit()
    cur.close()

def generate_basic_statistics(user_id, users, data):
    con = mysql.connector.connect(**MYSQL_CREDS)
    cur = con.cursor(prepared=True)


    # Calculate message count, char count, word count, avg message length for all 9 combinations
    combinations = [  # None = both
        ('user1', 'discord'), ('user1', 'instagram'), ('user1', None),
        ('user2', 'discord'), ('user2', 'instagram'), ('user2', None),
        (None, 'discord'), (None, 'instagram'), (None, None)
    ]

    for user, platform in combinations:
        # Filter messages based on the user and platform combination
        filtered_data = [
            msg for msg in data
            # `msg` format: (platform, user, timestamp, content)
            if ((user is None or msg[1] == users[user][msg[0]]) and
                (platform is None or msg[0] == platform))
        ]

        # Calculate statistics
        message_count = len(filtered_data)
        char_count = sum(len(msg[3]) for msg in filtered_data)
        word_count = sum(len(msg[3].split()) for msg in filtered_data)
        avg_message_length = char_count / message_count if message_count > 0 else 0
        avg_word_length = char_count / word_count if message_count > 0 else 0
        avg_words_per_message = word_count / message_count if message_count > 0 else 0

        # Define stats key for identification
        stats_key_user = user if user else 'both_users'
        stats_key_platform = platform if platform else 'both_platforms'

        # Insert statistics into the database
        cur.execute(
            f"""
                INSERT INTO `basic_stats` (uuid, platform, user, message_count, character_count, word_count, average_message_length, avg_word_length, avg_words_per_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
            (user_id, stats_key_platform, stats_key_user, message_count, char_count, word_count, avg_message_length,
             avg_word_length, avg_words_per_message)
        )

