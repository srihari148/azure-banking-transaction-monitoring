# 🏦 Azure Banking Transaction Monitoring System

![Azure](https://img.shields.io/badge/Azure-Databricks-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-blue)
![ADF](https://img.shields.io/badge/Azure-Data_Factory-green)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![Python](https://img.shields.io/badge/Python-3.10-yellow)

## 📌 Project Overview

An **enterprise-grade, end-to-end Azure Data Engineering project** that monitors banking transactions, detects fraud in real-time, and provides business intelligence through an interactive Streamlit dashboard.

---

## 🏗️ Architecture

```
CSV Files (Raw Data)
      ↓
Azure Data Factory (Orchestration)
      ↓
ADLS Gen2 (Data Lake)
      ↓
Azure Databricks (Processing)
  ├── Bronze Layer  (Raw Delta Tables)
  ├── Silver Layer  (Cleaned & Validated)
  ├── Gold Layer    (Business Aggregations)
  └── Fraud Detection (4 Rules)
      ↓
Snowflake (Data Warehouse)
      ↓
Streamlit Dashboard (Visualization)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Data Processing | Azure Databricks (PySpark) |
| Storage Format | Delta Lake |
| Orchestration | Azure Data Factory |
| Data Warehouse | Snowflake |
| Dashboard | Streamlit in Snowflake |
| Language | Python, PySpark, SQL |

---

## 📊 Dataset

| Dataset | Records | Description |
|---|---|---|
| `customer.csv` | 5,000 | Customer account details |
| `branch.csv` | 50 | Bank branch information |
| `transactions.csv` | 1,00,098 | Banking transactions (Jan 2025 - Jun 2026) |

### Transaction Channels
- UPI, ATM, CARD, NETBANKING

### Account Types
- SAVINGS, CURRENT, SALARY, NRI

---

## 🏛️ Medallion Architecture

### 🥉 Bronze Layer
- Raw CSV files ingested as-is into Delta format
- No transformations applied
- Immutable source of truth

### 🥈 Silver Layer
- Data validation and cleansing
- Null checks, duplicate removal
- Type casting and standardization
- Rejected records stored separately

### 🥇 Gold Layer
- Business-ready aggregations
- Customer KPIs and monthly summaries
- Branch and regional analytics
- Fraud alert tables

---

## 🚨 Fraud Detection Rules

| Rule | Description | Records Found |
|---|---|---|
| High Value Transaction | Amount > ₹1,00,000 | 182 |
| Rapid Burst | >3 transactions within 10 minutes | 30 |
| High Daily Spend | Daily spend > ₹10 lakh | 19 |
| Dormant Account Active | No activity 180+ days, suddenly active | 59 |
| **Total** | | **290** |

---

## 📁 Repository Structure

```
azure-banking-transaction-monitoring/
│
├── README.md
│
├── datasets/
│   ├── branch.csv
│   ├── customer.csv
│   └── transactions.csv
│
├── databricks/
│   ├── 01_Bronze_Load.py
│   ├── 02_Silver_Customer.py
│   ├── 03_Silver_Branch.py
│   ├── 04_Silver_Transaction.py
│   ├── 05_Fraud_Detection.py
│   ├── 06_Gold_Customer_Summary.py
│   ├── 07_Gold_Branch_Summary.py
│   └── 08_Gold_To_Snowflake.py
│
├── snowflake/
│   └── create_tables.sql
│
├── streamlit/
│   └── Banking_Transaction_Dashboard.py
│
├── adf/
│   └── PL_Master_Banking_pipeline.json
│
└── screenshots/
    ├── 01_executive_overview.png
    ├── 02_fraud_analysis.png
    ├── 03_branch_analysis.png
    └── 04_monthly_trends.png
```

---

## 🚀 Pipeline Flow

```
PL_Master_Banking (ADF)
│
├── 1. Bronze_Load          → Reads CSVs → Delta (bronze/)
├── 2. Silver_Customer      → Validates → Delta (silver/customer)
├── 3. Silver_Branch        → Validates → Delta (silver/branch)
├── 4. Silver_Transaction   → Validates → Delta (silver/transactions)
├── 5. Fraud_Detection      → 4 rules  → Delta (gold/fraud_alerts/)
├── 6. Gold_Customer_Summary→ KPIs     → Delta (gold/customer_summary/)
├── 7. Gold_Branch_Summary  → Revenue  → Delta (gold/branch_summary/)
└── 8. Snowflake_Load       → Loads all Gold tables to Snowflake
```

---

## 📈 Dashboard KPIs

### Executive Overview
- Total Customers: **5,000**
- Total Transactions: **1,00,098**
- Total Debit Amount: **₹131 Cr**
- Total Fraud Alerts: **290**

### Branch Analysis
- Total Branches: **50**
- Total Amount: **₹255 Cr**
- Regions: North, South, East, West, Central

### Monthly Trends
- Monthly Records: **37,895**
- Average Monthly Spend: **₹34,558**
- Max Monthly Spend: **₹19,60,632**

---

## ⚙️ Azure Resources

| Resource | Name |
|---|---|
| Resource Group | rg-banking-transaction-monitoring |
| Storage Account | stbankingtransactions |
| Databricks Workspace | adb-banking-transaction |
| Data Factory | adf-banking-transaction |
| Snowflake Account | HSZBUUH-ME65874 |

### ADLS Gen2 Container Structure
```
stbankingtransactions/
├── raw/
│   ├── customer/
│   ├── branch/
│   └── transactions/
├── bronze/
│   ├── customer/
│   ├── branch/
│   └── transactions/
├── silver/
│   ├── customer/
│   ├── branch/
│   └── transactions/
├── gold/
│   ├── customer_summary/
│   ├── branch_summary/
│   └── fraud_alerts/
├── audit/
└── rejected/
```

---

## 🔑 Key Engineering Concepts Used

- **Medallion Architecture** (Bronze → Silver → Gold)
- **Delta Lake** (ACID transactions, Time Travel)
- **Incremental Loading** (Watermark pattern)
- **Window Functions** (Fraud burst detection)
- **Data Quality Framework** (Null checks, deduplication)
- **Audit Logging** (Pipeline tracking)
- **Service Principal Authentication** (ADLS → Databricks)
- **Partitioning** (Silver transactions by date)

---

## 📸 Screenshots

### Executive Overview
<img width="1805" height="847" alt="image" src="https://github.com/user-attachments/assets/8707860c-6a7f-4048-b423-52ef30411d4c" />

### Fraud Analysis
<img width="1796" height="857" alt="image" src="https://github.com/user-attachments/assets/f461e7da-d3c8-4100-863e-7c85a0f4e8ca" />

### Branch Analysis
<img width="1792" height="862" alt="image" src="https://github.com/user-attachments/assets/151d7c65-8598-46fd-8bac-7a914bbe4631" />

### Monthly Trends
<img width="1801" height="858" alt="image" src="https://github.com/user-attachments/assets/f7a61318-a6aa-4488-8926-5ec558b1a7ee" />

---

## 👤 Author

**Srihari Ramanadham**
- LinkedIn: [Srihari Ramanadham](https://www.linkedin.com/in/srihari-ramanadham)
- GitHub: [@srihari148](https://github.com/srihari148)

---

## 📝 License

This project is for educational and portfolio purposes.
