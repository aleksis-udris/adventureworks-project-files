# Data Warehousing Documentation


# AdventureWorks Data Warehouse - Documentation Index

**Project**: AdventureWorks Data Warehouse (PostgreSQL → ClickHouse)  
**Version**: 1.0  
**Last Updated**: December 2025  
**Target Audience**: Data Engineers, DevOps, Data Analysts

---

## Quick Start

- **New to the project?** Start with [Architecture & Design Rationale](reading/architecture_rationale.md)
- **Setting up?** Go to [Setup & Installation Guide](reading/setup_installation.md)
- **Troubleshooting?** Check [FAQ & Troubleshooting](reading/faq_troubleshooting.md)

---

## BLOCK A: Schema Design

Understanding the data warehouse structure and design decisions.

| Document | Description | Pages |
|----------|-------------|-------|
| [Schema Design Specification](reading/schema_specification.md) | Complete logical and physical schema design with all fact and dimension tables | 8-10 |
| [Data Lineage & Mapping](reading/data_mapping.md) | Source-to-target mappings, transformation rules, and data flow diagrams | 5-7 |
| [ClickHouse Physical Design](reading/clickhouse_physical_design.md) | ClickHouse-specific implementation: engines, partitioning, indexes | 10-15 |

**Key Topics**: Star schema, SCD Type 2, fact tables, dimension tables, grain definition

---

## BLOCK B: ETL Pipeline

How data moves from PostgreSQL to ClickHouse and transforms along the way.

| Document | Description | Pages |
|----------|-------------|-------|
| [ETL Architecture Overview](reading/etl_architecture.md) | High-level pipeline design, incremental loading, data freshness SLA | 4-6 |
| [Airflow DAG Specification](reading/airflow_dag_spec.md) | Task dependencies, scheduling, XCom usage, retry logic | 8-12 |
| [Transformation Logic](reading/transformation_logic.md) | SCD Type 2 merge, fact loading, foreign key resolution, aggregations | 8-10 |
| [Error Handling & Monitoring](reading/error_handling.md) | **CRITICAL**: Error classification, recovery strategies, alerting | 12-15 |

**Key Topics**: Incremental loads, SCD merge logic, error_records table, retry strategies

---

## BLOCK C: Implementation & Deployment

Practical guides for setting up and operating the data warehouse.

| Document | Description | Pages |
|----------|-------------|-------|
| [Setup & Installation Guide](reading/setup_installation.md) | Prerequisites, environment setup, initial data loads | 6-8 |
| [Deployment & Operations Runbook](reading/deployment_runbook.md) | Production deployment, monitoring, incident response | 10-12 |
| [Performance & Capacity Planning](reading/performance_capacity.md) | Query optimization, partition pruning, scaling strategies | 6-8 |

**Key Topics**: Docker setup, Airflow configuration, production checklist, runbooks

---

## BLOCK D: Reference & Supporting Information

Glossaries, best practices, and troubleshooting guides.

| Document | Description | Pages |
|----------|-------------|-------|
| [Glossary & Terminology](reading/glossary.md) | Data warehouse terms, ClickHouse concepts, acronyms | 4-6 |
| [FAQ & Troubleshooting](reading/faq_troubleshooting.md) | Common issues, solutions, debugging tips | 6-8 |
| [Development Standards](reading/dev_standards.md) | Coding conventions, SQL style guide, testing practices | 5-7 |
| [Architecture & Design Rationale](reading/architecture_rationale.md) | Why ClickHouse? Why star schema? Design decisions explained | 4-6 |

**Key Topics**: Naming conventions, error codes, performance patterns

---

## Common Use Cases

**I want to...**

- **Understand the overall architecture** → [ETL Architecture Overview](reading/etl_architecture.md)
- **Add a new fact table** → [Transformation Logic](reading/transformation_logic.md) + [Airflow DAG Spec](reading/airflow_dag_spec.md)
- **Debug a failed DAG run** → [Error Handling & Monitoring](reading/error_handling.md)
- **Optimize slow queries** → [Performance & Capacity Planning](reading/performance_capacity.md)
- **Deploy to production** → [Deployment & Operations Runbook](reading/deployment_runbook.md)
- **Understand SCD Type 2** → [Transformation Logic](reading/transformation_logic.md)

---

## Project Statistics

- **Fact Tables**: 9 (Sales, Purchases, Inventory, Production, Employee Sales, Customer Feedback, Promotion Response, Finance, Returns)
- **Dimension Tables**: 16 (Customer, Product, Store, Employee, Date, Vendor, Promotion, etc.)
- **Aggregate Tables**: 6 (Daily/Weekly/Monthly sales, Daily inventory, Monthly product performance, Regional sales)
- **Analytical Views**: 17+ (Extended sales, Customer analysis, Product analysis, etc.)
- **Source System**: PostgreSQL AdventureWorks database
- **Target System**: ClickHouse data warehouse
- **Orchestration**: Apache Airflow

---

## Key Features

- **SCD Type 2 History Tracking** - Full dimensional history with IsCurrent flags  
- **Incremental Loading** - Only load changed data (361x faster than full loads)  
- **Comprehensive Error Handling** - Recoverable vs non-recoverable error classification  
- **Partition Pruning** - Monthly partitions for 50x query speed improvements  
- **Pre-Aggregated Tables** - Fast reporting without scanning fact tables  
- **Automated Retry Logic** - Smart error recovery with exponential backoff  

---

## Critical Notes

**NOT INCLUDED IN THIS PROJECT**:
- Advanced monitoring tools (basic monitoring only)
- Real-time streaming (batch processing only)

**INCLUDED & CRITICAL**:
- Error handling framework (mandatory for production)
- SCD Type 2 merge logic (version control)
- Airflow DAG specifications (orchestration)
- Operational runbooks (incident response)

---

## Support & Contact

- **Feedback**: Use thumbs down button in chat or email data-warehouse@company.com
- **Issues**: Log in JIRA project DWH
- **Updates**: Check this index for latest document versions

---

## Document Versioning

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Dec 2025 | Initial release |

---

**Next Steps**: New to the project? Start with [Schema Design Specification](reading/schema_specification.md) to understand the data model.
This project, as the name suggests, 
exists for the sole purpose of learning 
how to practically create and maintain a 
Data Warehouse and its associated systems:
- ETL Pipeline
- Dimensions
- Facts
- Aggregates
- Data Stores
- Analytics
- Etc.

# Index

### Schema Information
1. [Schema Specification](reading/schema_specification.md)
2. [Data Mapping](reading/data_mapping.md)
3. Clickhouse Design

### ETL Pipeline
1. ETL Architecture
2. Airflow DAG Specification
3. Transformation Logic
4. Error Handling

### Implementation & Deployement
1. Setup & Installation
2. Deployment Runbook
3. Performance & Capacity

### Citations & Base Information
1. Terminology
2. Troubleshooting
3. Best Practices
4. Architecture & Rationale