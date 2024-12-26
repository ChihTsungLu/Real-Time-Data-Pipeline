Real-Time Data Pipeline
=======================

The **Real-Time Data Pipeline** leverages cutting-edge technologies like **Kafka**, **Zookeeper**, **Spark**, and **AWS** (S3, Glue, Athena, RedShift) to simulate real-time IoT data processing. This project models a taxi driving from Birmingham to London, streaming real-time data throughout the journey. The data is ingested, processed, and stored for further analysis, showcasing an end-to-end data pipeline.

Key Features
------------

*   **Real-Time Data Streaming**: Simulates IoT data generation from a moving taxi.
    
*   **Data Ingestion**: Utilizes **Kafka** and **Zookeeper** for reliable data transmission.
    
*   **Data Storage and Processing**:
    
    *   **S3**: Stores raw and processed data.
        
    *   **AWS Glue**: Extracts and transforms data for analysis.
        
    *   **RedShift**: Supports querying and visualizing the data.
        
*   **Scalability**: Modular and scalable architecture orchestrated with **Docker Compose**.
    

Prerequisites
-------------

Before you begin, ensure you have the following:

1.  **AWS Credentials**:
    
    *   An AWS account with access keys for S3, Glue, and RedShift.
        
2.  **Docker**: Installed and configured on your system.
    
3.  **Python**: Required to run the real-time data simulation.
    

Installation
------------

1.  ```git clone [https://github.com/yourusername/real-time-data-pipeline.gitcd real-time-data-pipeline](https://github.com/ChihTsungLu/Real-Time-Data-Pipeline.git)```
    
2.  **Set Up Docker Compose**:
    
    *   Ensure Docker is running on your machine.
        
    *   Configure docker-compose.yml as needed.
        

Configuration
-------------

1.  ```AWS\_ACCESS\_KEY = "YOUR\_AWS\_ACCESS\_KEY"AWS\_SECRET\_KEY = "YOUR\_AWS\_SECRET\_KEY"```
    
2.  Ensure your AWS IAM role has appropriate permissions for S3, Glue, and RedShift.
    

Running the Pipeline
--------------------

1.  ```docker-compose up```
    
2. ```python main.py```
    
3.  Monitor the pipeline:
    
    *   Check **Kafka** topics for incoming messages.
        
    *   Verify data in S3 and RedShift.
        

Technologies Used
-----------------

*   **Kafka**: Real-time data streaming.
    
*   **Zookeeper**: Manages Kafka clusters.
    
*   **Spark**: Processes streamed data.
    
*   **AWS S3**: Stores raw and processed data.
    
*   **AWS Glue**: Data transformation and ETL.
    
*   **AWS RedShift**: Data warehousing and analytics.
    
*   **Docker Compose**: Orchestrates the environment.
    

Future Enhancements
-------------------

*   Add support for multiple IoT devices.
    
*   Integrate monitoring tools like **Prometheus** and **Grafana**.
    
*   Implement data quality checks during ETL.
