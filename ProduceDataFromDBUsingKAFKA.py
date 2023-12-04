import pyodbc
from confluent_kafka import Producer
import json

# Define your SQL Server connection string
conn_str = 'DRIVER={SQL Server};SERVER=192.168.1.20;DATABASE=Demo;UID=loai;PWD=P@ssw0rd'

# Create a connection to SQL Server
connection = pyodbc.connect(conn_str)

# Create Kafka Producer
producer = Producer({'bootstrap.servers': 'worker2:9092'})

try:
    with connection.cursor() as cursor:
        # Define your SQL query
        sql_query = "SELECT * FROM emp"

        while True:
            cursor.execute(sql_query)
            result = cursor.fetchall()

            # Produce records to Kafka
            for row in result:
                # Convert row to JSON format
                json_row = json.dumps(row, default=str)
                print("json_row")
                producer.produce('StreamDB', key=row.unique_key, value=json_row)
                producer.flush()

finally:
    connection.close()

