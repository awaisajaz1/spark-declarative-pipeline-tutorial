# Databricks SDP Tutorial Project for Beginners

Welcome to this beginner-friendly tutorial project demonstrating modern data engineering practices using **Databricks** and the **Spark Declarative Pipeline (SDP)**. This project showcases the power of SDP combined with Medallion Architecture to build a scalable, maintainable, and idempotent data pipeline.

---

## 🚀 Project Overview

This tutorial project guides you through building a data pipeline on Databricks using the latest **SDP** features such as:

- **Materialized Views**
- **Temporary Views**
- **Streaming Tables**
- **Idempotent processing**

You'll also explore how to utilize **Databricks Unity Catalog** for secure, centralized data governance.

The pipeline implements a classic **Medallion Architecture** pattern, transforming raw data through Bronze, Silver, and Gold layers, with a focus on building a **Star Schema** data model that includes:

- **Fact_Sales** (Fact table)
- **Dim_Product** (Dimension table with SCD Type 2)
- **Dim_Store** (Dimension table)

---

## 📚 Key Concepts Covered

- **Spark Declarative Pipeline (SDP)**: Declarative APIs for defining data transformations in a clean, modular way.
- **Medallion Architecture**: Organizing data lakes in Bronze, Silver, and Gold layers for incremental and reliable data refinement.
- **Unity Catalog**: Managing data access and governance centrally in Databricks.
- **Idempotency**: Ensuring pipelines can run multiple times without duplication or data corruption.
- **Slowly Changing Dimensions (SCD Type 2)**: Handling changes in dimensional data over time.
- **Star Schema Modeling**: Designing fact and dimension tables for efficient analytics.

---

## 🛠️ Technologies Used

- Databricks
- Apache Spark 3.x
- Spark Declarative Pipeline (SDP)
- Databricks Unity Catalog
- Delta Lake (native databricks storage for this demo)
- Streaming and Batch Processing

---

