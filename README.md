# ODD Processing Pipeline with Kafka, Redis, and Cassandra

This project demonstrates a data processing pipeline designed to ingest, prioritize, and store Operational Design Domain (ODD) data from YAML files. It leverages a modern, distributed architecture based on the principles of the CAP theorem, using Kafka, Redis, and Cassandra.

## Architecture Overview

The pipeline follows a clear, decoupled data flow, making it scalable and resilient.

```
[YAML Files] -> [1. Python Producer] -> [2. Kafka Topic] -> [3. Python Consumer] --+
                                                                                   |
                                                          +------------------------+
                                                          |                        |
                                                 (If Category is 'Corner Case')    (All ODDs)
                                                          |                        |
                                                          V                        V
                                                 [4. Redis List]          [5. Cassandra Table]
                                            (For High-Priority/Fast Access) (For Long-Term Storage)
```

### Technology Choices & CAP Theorem

The selection of technologies aligns with the CAP theorem (Consistency, Availability, Partition Tolerance) to handle different data needs:

*   **Kafka (AP - Availability & Partition Tolerance):** Acts as the central, durable message bus. It ingests all incoming ODD data, providing high availability and ensuring no data is lost even if downstream services are temporarily unavailable.
*   **Cassandra (AP - Availability & Partition Tolerance):** Serves as the long-term, scalable data store for *all* ODD records. Its masterless, distributed nature provides excellent availability and partition tolerance, making it ideal for archival and large-scale analytics.
*   **Redis (CP/AP - Configurable):** Used as a high-speed, in-memory cache for *high-priority* ODDs (identified as `Category: Corner Case`). This allows other services to immediately access and react to critical scenarios, prioritizing low-latency access.

## Technologies Used

*   **Backend:** Python 3
*   **Containerization:** Docker, Docker Compose
*   **Messaging:** Apache Kafka
*   **Databases:**
    *   Apache Cassandra (NoSQL, long-term storage)
    *   Redis (In-memory, caching/prioritization)

## Prerequisites

*   Docker
*   Docker Compose

## How to Run

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd <repository-directory>
```

### 2. Directory Structure

Ensure your ODD YAML files are placed in a directory named `generated_synthetic_odd` within the project's root folder.

```
├── generated_synthetic_odd/
│   ├── synthetic_odd_1.yaml
│   └── ...
├── docker-compose.yml
├── Dockerfile
├── process_odds.py
└── requirements.txt
```

### 3. Start the Entire System

Run the following command from the project's root directory. This will build the Python application container and start all the necessary services (Kafka, Zookeeper, Redis, Cassandra, and the application itself).

```bash
docker-compose up --build
```

### 4. (One-Time Setup) Create Cassandra Keyspace and Table

The first time you run the system, you need to set up the database schema in Cassandra.

1.  Wait for all containers to be up and running.
2.  Open a **new terminal** and execute the following command to enter the Cassandra container's command-line shell (`cqlsh`):

    ```bash
    docker-compose exec cassandra cqlsh
    ```

3.  Once you see the `cqlsh>` prompt, paste and run the following commands:

    ```cql
    CREATE KEYSPACE IF NOT EXISTS cap_odd_system
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

    USE cap_odd_system;

    CREATE TABLE IF NOT EXISTS odds (
        odd_id TEXT PRIMARY KEY,
        category TEXT,
        raw_data TEXT
    );
    ```

The Python application will now be able to write data to Cassandra.

## Verifying the Results

### Check Redis for High-Priority ODDs

1.  Open a new terminal.
2.  Connect to the Redis container: `docker-compose exec redis redis-cli`
3.  Check the contents of the high-priority list: `LRANGE high_priority_odds 0 -1`

### Check Cassandra for All ODDs

1.  Connect to the Cassandra container's shell: `docker-compose exec cassandra cqlsh`
2.  Run a query to see all stored records: `SELECT * FROM cap_odd_system.odds;`