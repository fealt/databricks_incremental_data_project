<img src="https://docs.databricks.com/aws/en/img/logo.svg" height="60">

<br>

## Databricks Incremental Data Injestion Project

<a name="medallion-architecture"></a>

## Medallion Architecture Implementation

The project is structured around the Medallion Architecture, which consists of three layers:

### Bronze Layer
- `Data Ingestion:` Incremental data loading using **Spark Structured Streaming** and **Databricks Autoloader**.
- `Source Data:` Primarily focuses on file-based data sources, reflecting common big data scenarios.
- `Dynamic Solution:` Emphasizes building **dynamic**, deployable notebooks for the bronze layer using **parameter parsing** and **control flow** in Databricks jobs, rather than static solutions.

### Silver Layer
- `Data Transformation:` Utilizes **Databricks Lakeflow Declarative Pipelines** (formerly Delta Live Tables - DLT) for data transformations.
- `Advanced Techniques:` Incorporates **PySpark**, **function calling**, and other newly introduced features within **Databricks Lakeflow** for an enhanced development experience.
- `Dynamic Approach:` Similar to the bronze layer, the silver layer will also be built using a **dynamic approach** for deployable notebooks.

### Gold Layer
- `Core of the Project:` This layer is highlighted as the "heart" of the project due to its innovative approach.
- `Dimensional Data Model:` Building a **star schema**.
- `Automated Builders:` A key innovation is the creation of a **Slowly Changing Dimensions (SCD)** builder and an **automatic fact builder**. These builders will automate the creation and deployment of SCDs and fact tables, making the solution **reusable** across multiple data warehouses. This reflects an industry-level approach to data warehouse development.

<br>


