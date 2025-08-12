
This project demonstrates a real-time streaming data pipeline that ingests streaming data from a Kafka topic, processes it using an Apache Spark Structured Streaming job, and persists the results into a Cassandra database.

Setup and Execution
  To run this project, you must have Docker and Docker Compose installed. 
  Place the docker-compose.yml, spark-kafka-stream-docker.py, and kafka-producer-docker.py files in the same directory.
  Open your terminal in that directory.
  Execute the following command to build the Docker images and start all services:

      docker-compose up --build

The Spark job will automatically start and connect to Kafka and Cassandra.
The producer will begin sending streaming data to the Kafka topic.
The pipeline will process the data and write it to the Cassandra tables.
To stop all services, press Ctrl+C in your terminal.

Data Pipeline

The data flows through the following stages:

  Data Generation (producer): A Python script continuously generates JSON messages and publishes them to a Kafka topic named test_topic.
  Data Ingestion and Processing (spark-submit):
  A Spark Structured Streaming application connects to the test_topic and reads the data stream.
  The raw JSON data is parsed and a timestamp is added.

  Feature 1: Records with an age greater than 30 are filtered, and a city is joined from a static DataFrame based on the country field. These results are written to the people table in Cassandra.
  Feature 2: All records are aggregated using a tumbling window of 10 seconds to calculate the average age. The results are written to the age_stats table in Cassandra.

Data Persistence (cassandra): Processed and aggregated data is stored in two distinct tables (people and age_stats) within the testks keyspace.
