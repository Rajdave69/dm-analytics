import re
from datetime import datetime
import csv
from flask import jsonify
import json

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
            discord_data.append((author, timestamp, content))
        except (IndexError, ValueError) as e:
            return jsonify({"message": f"Error processing row in Discord file: {row}. Error: {e}"}), 400

    return discord_data
