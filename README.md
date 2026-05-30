# fraudstream
-- Real-Time Fraud Detection System (Kafka + Streamlit + PostgreSQL)
-- A real-time streaming data pipeline that simulates financial transactions and detects fraudulent behavior using rule-based analytics. The system processes live transaction data, applies anomaly detection logic, and stores detected fraud events in a structured database for analytics and visualization.

-- The pipeline is built using a distributed event-driven architecture with Kafka for messaging, Python for stream processing, PostgreSQL for persistence, and Streamlit for real-time monitoring dashboards.

-- Key Features
-- Real-time transaction data simulation using Faker
-- Event streaming pipeline using Apache Kafka
-- Stateful fraud detection using Python consumer logic

-- Fraud detection rules:
-- High-value transaction detection
-- Velocity-based fraud detection
-- Device change anomaly detection
-- Location change detection
-- Spending spike detection
-- Persistent storage of fraud alerts in PostgreSQL
I-- nteractive real-time dashboard using Streamlit
-- Analytics layer with fraud categorization and user risk profiling

-- Architecture
-- Producer → Kafka → Consumer → Fraud Detection Engine → PostgreSQL → Streamlit Dashboard

-- Tech Stack
Python
Apache Kafka
PostgreSQL
Docker
Streamlit
Faker
Pandas
Plotly

-- Outcome
The system demonstrates how real-time financial fraud detection systems are built in production environments using streaming data pipelines and event-driven architecture.
