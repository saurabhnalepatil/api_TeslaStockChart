from flask import Flask, jsonify, request
from flask_cors import CORS
from azure.data.tables import TableClient
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient
import csv

app = Flask(__name__)
CORS(app)


connection_string = "my_connection-string"
table_name = "Tesla"


def read_csv_data():
    data = []
    with open("TSLA.csv", "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data


@app.route("/api/insert_data", methods=["POST"])
def insert_data():
    data = read_csv_data()

    table_service_client = TableServiceClient.from_connection_string(
        conn_str=connection_string
    )
    table_client = table_service_client.get_table_client(table_name)

    for item in data:
        my_entity = {
            "PartitionKey": "TSLA",
            "RowKey": item["Date"],
            "Open": item["Open"],
            "Close": item["Close"],
            "High": item["High"],
            "Low": item["Low"],
            "Volume": item["Volume"],
            "Adj_Close": item["Adj_Close"],
        }
        table_client.create_entity(entity=my_entity)

    return (
        jsonify(
            {
                "message": "successfully posted",
            }
        ),
        200,
    )


@app.route("/api/insert_single_data", methods=["POST"])
def insert_single_data():
    data = request.json

    table_service_client = TableServiceClient.from_connection_string(
        conn_str=connection_string
    )
    table_client = table_service_client.get_table_client(table_name)

    payload = {
        "PartitionKey": data["PartitionKey"],
        "RowKey": data["RowKey"],
        "Open": data["Open"],
        "Close": data["Close"],
        "High": data["High"],
        "Low": data["Low"],
        "Volume": data["Volume"],
        "Adj_Close": data["Adj_Close"],
    }

    table_client.create_entity(entity=payload)

    return (
        jsonify(
            {
                "message": "Data Insert SuccessFully.",
            }
        ),
        200,
    )


@app.route("/api/get_data", methods=["GET"])
def get_data():
    table_client = TableClient.from_connection_string(
        conn_str=connection_string,
        table_name=table_name,
    )
    my_filter = "PartitionKey eq 'TSLA'"
    entities = table_client.query_entities(my_filter)

    Mydata = []
    for entity in entities:
        Mydata.append(entity)

    return (
        jsonify(Mydata),
        200,
    )


@app.route("/api/update_data", methods=["PUT"])
def update_data():
    try:
        data = request.json
        row_key = data.get("RowKey")

        table_service_client = TableServiceClient.from_connection_string(
            conn_str=connection_string, table_name=table_name
        )

        table_client = table_service_client.get_table_client(table_name=table_name)

        entity = table_client.get_entity(partition_key="TSLA", row_key=row_key)

        entity["Open"] = data.get("Open")
        entity["Close"] = data.get("Close")
        entity["High"] = data.get("High")
        entity["Low"] = data.get("Low")
        entity["Volume"] = data.get("Volume")
        entity["Adj_Close"] = data.get("Adj_Close")

        table_client.update_entity(mode=UpdateMode.REPLACE, entity=entity)

        return (
            jsonify("Data updated successfully"),
            200,
        )

    except ResourceNotFoundError:
        return "Entity not found"


@app.route("/api/insert-message", methods=["POST"])
def insert_message():
    try:
        message = request.json

        if not message:
            return "Invalid request. Message is missing.", 400
    except Exception as e:
        return "Invalid request. Could not parse JSON data.", 400

    queue_client = QueueClient.from_connection_string(connection_string, "tesla-queue")
    queue_client.send_message(message)

    return "Message inserted into the queue successfully"


@app.route("/api/reseed-data", methods=["PATCH"])
def reseed_data():
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client("tesladata", "TSLA.csv")

    blob_data = blob_client.download_blob().content_as_text()

    table_client = TableClient.from_connection_string(connection_string, table_name)

    entities = list(table_client.list_entities())

    for entity in entities:
        try:
            table_client.delete_entity(entity["PartitionKey"], entity["RowKey"])
        except ResourceNotFoundError:
            pass

    rows = blob_data.split("\n")[1:]  # Skip header row
    for row in rows:
        if row:
            data = row.split(",")
            my_entity = {
                "PartitionKey": "TSLA",
                "RowKey": data[0],
                "Open": data[1],
                "High": data[2],
                "Low": data[3],
                "Close": data[4],
                "Adj_Close": data[5],
                "Volume": data[6],
            }
            table_client.create_entity(entity=my_entity)

    response = {"message": "Table reseeded successfully"}
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True)
