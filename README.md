# App Consumer Data FASTAPI

## Architecture Overview
![Image](https://github.com/user-attachments/assets/3a4be860-844e-4b85-b74c-0921269d4ef7)

A **FastAPI-based endpoint** designed to serve real-time, updating business applications by consuming user device information. The system leverages **Kafka** for streaming, **Redis** for caching, and integrates with a **Delta Lake** architecture to provide scalable, low-latency(< 8ms) data access.

---

## Features

- Real-time ingestion of user device data  
- Authentication and user session management with **Redis caching**  
- Streaming pipeline using **Kafka** and **PySpark** for efficient data processing  
- Integration with **Delta Lake**, **Trino**, and **Hive** for historical and analytical queries  
- Realtime API endpoints using **FastAPI** for business applications  
- Optimized for both active and inactive users with persistent Redis caching  

---

**Components:**

- **Auth Service:** Manages user sign-up/sign-in, token generation, and session storage in Redis  
- **Sync Service:** Handles incoming user requests, checks for active Delta Lake connections, and routes data accordingly  
- **Redis Service:** Caches user streaming data and persists last known states for inactive users  
- **Data Lake:** Uses Kafka for real-time streaming, Trino for querying, and Delta Lake for storage  

---

## Tech Stack

- **Backend:** FastAPI  
- **Streaming:** Kafka  
- **Caching:** Redis
- **Data Storage:** Redis AOF, Trino  
- **Deployment:** Docker 

---

![Image](https://github.com/user-attachments/assets/94814f79-5b68-437f-9b12-614ef7d69678)

## Installation

Clone the repository:

```bash
git clone https://github.com/<your-username>/consumer-referral-api.git
cd consumer-referral-api



