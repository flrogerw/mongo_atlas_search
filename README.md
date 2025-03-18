# Station, Show, and Podcast Ingester

This project represents the next evolution of an enterprise-grade search platform, designed to explore and integrate various search technologies for hosting and indexing millions of podcasts. Leveraging multithreading, it efficiently ingests, processes, vectorizes, and inserts vast numbers of RSS feeds into a search system. These scripts are built for high performance, incorporating Redis caching, advanced NLP processing, and optimized database interactions to seamlessly manage massive datasets at scale.
## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Environment Variables](#environment-variables)
- [Usage](#usage)
- [Components](#components)
- [Error Handling](#error-handling)
- [Monitoring](#monitoring)

## Overview

The system is built to process large datasets from CSV files and databases, utilizing NLP and machine learning techniques. It uses threading to improve performance and Redis for caching. The key components include:

- **Station Ingester**: Processes station data from CSV files.
- **Show Ingester**: Fetches and processes show records from a database.
- **Podcast Producer**: Produces and processes podcast data for Kafka ingestion.

## Setup

### Prerequisites

- Python 3.x
- Redis Server
- PostgreSQL Database
- Kafka (for Podcast Producer)
- Required Python libraries (see `requirements.txt`)

### Installation

1. Clone the repository:
   ```sh
   git clone <repository-url>
   cd <repository-folder>
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Set up the environment variables (see below).

## Environment Variables

Create a `.env` file in the root directory and define the following variables:

```ini
THREAD_COUNT=4
JOB_RECORDS_TO_PULL=1000
FLUSH_REDIS_ON_START=True
JOB_QUEUE_SIZE=5000
DB_USER=username
DB_PASS=password
DB_DATABASE=database_name
DB_HOST=localhost
DB_SCHEMA=schema_name
STATIONS_CSV_FILE=stations.csv
REDIS_HOST=localhost
LANGUAGES=en,es
VECTOR_MODEL_NAME=sentence-transformers/paraphrase-mpnet-base-v2
```

Modify values as needed based on your system setup.

## Usage

### Running the Station Ingester

```sh
python StationIngester.py
```

### Running the Show Ingester

```sh
python ShowIngester.py
```

### Running the Podcast Producer

```sh
python podcast_producer.py <SERVER_CLUSTER_SIZE> <CLUSTER_SERVER_ID>
```

Replace `<SERVER_CLUSTER_SIZE>` and `<CLUSTER_SERVER_ID>` with appropriate values.

## Components

### **Station Ingester**

Processes station data from a CSV file and queues records for processing. Uses NLP to analyze text fields.

### **Show Ingester**

Fetches show data from an SQLite database and processes records using NLP and sentence embeddings.

### **Podcast Producer**

Fetches podcast records, processes data, and produces messages to Kafka topics.

## Error Handling

All errors encountered during processing are logged into the `error_log` database table via the `ErrorLogger` class. Additionally, error queues store processing failures for later analysis.

## Monitoring

A monitoring thread continuously tracks the processing status, queue sizes, and elapsed time, printing periodic updates to the console.

## License

This project is licensed under the MIT License.

---

For any issues or contributions, feel free to submit a pull request or raise an issue!

