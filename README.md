# E-commerce Sales Analysis Dashboard

## Project Goal
Providing automated monthly sales insights for a B2B e-commerce SME by analyzing product profitability, pricing strategies, and inventory performance across multiple platforms.

---

## Data Source
Dataset used: [E-commerce Sales Dataset from Kaggle](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data/data)  
This dataset contains sales, pricing, stock, and profitability data from an e-commerce business operating across multiple platforms like Amazon, Flipkart, Myntra, Ajio, etc.

---

## Tools & Technologies Used
- Python (Pandas, NumPy, Matplotlib, Seaborn)
- Metabase (for dashboard creation)
- Jupyter Notebook (for data analysis & processing)
- Excel/CSV for raw data files

---

## Process Summary

### Data Loading & Integration
The project used 7 CSV files from the dataset, mainly:
- Cloud Warehouse Comparison Chart
- Sale Report
- P & L March 2021
- (plus 4 other supporting files)

Data was loaded using Python’s Pandas library and merged based on common identifiers like `SKU Code` and `Sku` to create a unified dataset.

---

### Data Cleaning & Preparation
Key actions:
- Handled missing values in product details & pricing.
- Resolved data inconsistencies (e.g., different column names for SKUs).
- Removed duplicates and irrelevant rows.
- Standardized column naming across files for easier joins.

---

### KPI Calculations
Some of the main KPIs calculated and visualized in the dashboard:

| KPI | Definition |
|-----|------------|
| Gross Profit | Revenue - Cost of Goods Sold |
| Profitability by Platform | Comparison of margins across Amazon, Flipkart, Myntra, etc. |
| Inventory Value | Stock * MRP |
| Top Performing Products | Highest profit-generating SKUs |
| Warehouse Efficiency | Comparison of Shiprocket vs INCREFF profitability |

---

## Automation Strategy (For Real-World Scenario)

Although the current dataset was static, the project has been designed with automation in mind.

### Proposed Automation Pipeline:
1. Monthly raw data (CSV files) dropped into a designated folder.
2. Python scripts automatically:
   - Load new files.
   - Clean & preprocess data.
   - Calculate KPIs.
   - Export a final clean dataset.

3. The clean dataset is connected to Metabase.

4. Metabase dashboards automatically update with the new data.

### Scheduling Tools for Automation:
- Apache Airflow (for enterprise-level workflows)
- Windows Task Scheduler / Cron (for simple scheduling)
- Metabase Scheduled Dashboards for automated reporting

---

## How to View or Run the Project

### To Run the Analysis:
1. Clone this repository:
```bash
git clone https://github.com/paakaer/ecommerce-sales-analysis.git
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the Jupyter notebook:
```bash
jupyter notebook
```

4. Execute `main.ipynb` to generate clean data & KPIs.

---

### To View the Dashboard:
- The dashboard has been built using Metabase.
- Link: [Insert Metabase Dashboard Link if public]
- Alternatively, screenshots of the dashboard are available in the `dashboard/` folder.

---

## Folder Structure
```
├── data/                   # Raw data files (CSV)
├── notebooks/              # Jupyter notebooks for analysis
├── scripts/                # Python scripts for automation
├── dashboard/              # Dashboard screenshots or exports
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

---

## Future Improvements
- Automate end-to-end pipeline with Airflow.
- Connect directly to a cloud warehouse (BigQuery / Snowflake).
- Implement email alerts for key KPI thresholds.
- Deploy the dashboard online for client access.
