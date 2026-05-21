## Overview
This project is an end-to-end ELT pipeline focused on traffic accidents in the Czech Republic.

<img width="761" height="421" alt="diagram (1)" src="https://github.com/user-attachments/assets/2951f1b4-8570-4032-9755-2bf843f9e888" />

---

## Sources
The project uses multiple sources as input data
- **Traffic accidents** - For now, the data is manually downloaded once a year from the official website of the Czech Police in .geojson format. https://nehody.policie.gov.cz
- **Road data** - For road data I chose OSM open street maps, because it is open source and has the necessary information, the source is a .pbf file, Czech Republic
- **Weather data** - The data source is the weather forecast API.

---

## Architecture & Processing Flow

The pipeline follows a layered Medallion ELT architecture inside PostgreSQL:

[Sources] ➔ [Bronze (Raw Payload)] ➔ [Silver (Cleaned & Filtered)] ➔ [Gold (Star Schema: Fact/Dim)] ➔ [Power BI Analytics]

## Ingest strategy

- Data is always taken for the past year
- The entire payload is stored
- Idempotency is ensured by tracking payload IDs and using incremental filtering / constraint checks during the transformation phase.

---

## Transformation & Data cleaning

- Implemented in **Python**
- Only the necessary information is selected from the entire payload
- Handling conditions and exceptions

---

## Data Storage

All data is stored in a **PostgreSQL** database, which serves as the central data warehouse for the project.

---

## Infrastructure

The entire pipeline is containerized using **Docker**, ensuring:

- Reproducibility across environments
- Easy setup and deployment
- Isolation of dependencies

---

## Orchestration

- Managed by **Apache Airflow**
- Executes manually
- Handles task dependencies and scheduling

---

## Tech Stack

- **Python** – data extraction and transformation
- **PostgreSQL** – data warehouse
- **Apache Airflow** – pipeline orchestration
- **Docker** – containerization and environment management

All Python dependencies are managed via a `requirements.txt` file.

---

## Getting Started

## Prerequisites

- Docker
- Docker Compose

---

## Setup & Installation

1. Clone the repository:

```bash
git clone https://github.com/GruWar/sql-data-warehouse-project.git
```

2. Build the Airflow environment:

Due to additional Python dependencies required transformations, a custom Docker image must be built:

```bash
docker-compose build
```

3. Run the Pipeline

Start the services:
```bash
docker-compose up
```

This will start:

- Apache Airflow
- PostgreSQL database

Access Airflow

- Once running, open:
http://localhost:8080

From there, you can:

- Trigger DAGs manually
- Monitor pipeline runs
- Inspect task execution
---

