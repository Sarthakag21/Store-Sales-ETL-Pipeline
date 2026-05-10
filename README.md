# Store Sales ETL Pipeline — PySpark

A **Data Engineering** project implementing a full ETL pipeline on the classic
Store dataset using **PySpark** on **Databricks**.

---

## Project Architecture

```
CSV File (store_sales.csv)
            ↓
       PySpark Read
            ↓
    Schema Inspection
            ↓
    Data Profiling         ← null counts, loss analysis, discount distribution
            ↓
    Null Handling          ← drop critical nulls, fill non-critical
            ↓
    Deduplication          ← dropDuplicates()
            ↓
    Filter Invalid Data    ← Sales > 0, Quantity > 0, Discount 0–1
            ↓
    Drop Constant Column   ← 'Country' (always United States)
            ↓
    Transformations        ← profit_margin_pct, is_loss, discount_tier,
                             effective_revenue, UPPERCASE text
            ↓
    Aggregations           ← category, region, segment, sub-category,
                             ship mode, discount tier, loss orders
            ↓
    Save as Parquet        ← 8 output tables + partitioned by Region/Category
            ↓
    Validate Output
```

---

## Dataset

| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| Ship Mode    | String  | Shipping method used                     |
| Segment      | String  | Customer segment (Consumer / Corporate / Home Office) |
| Country      | String  | Always "United States" — dropped         |
| City         | String  | Order city                               |
| State        | String  | Order state                              |
| Postal Code  | Integer | ZIP code                                 |
| Region       | String  | US region (East / West / Central / South)|
| Category     | String  | Product category                         |
| Sub-Category | String  | Product sub-category                     |
| Sales        | Double  | Revenue from the order ($)               |
| Quantity     | Integer | Units sold                               |
| Discount     | Double  | Discount ratio applied (0.0 – 0.8)       |
| Profit       | Double  | Profit/loss from the order ($)           |

**Total rows:** ~9,995  
**Unique Categories:** Furniture, Office Supplies, Technology  
**Regions:** East, West, Central, South

---

## ETL Pipeline Steps

| Step | Description |
|------|-------------|
| 1  | Start SparkSession |
| 2  | Read CSV with `inferSchema` |
| 3  | Inspect schema, display sample rows |
| 4  | **Data Profiling** — null counts, numeric stats, loss-order count, discount distribution |
| 5  | **Null Handling** — drop rows with null `Sales`/`Profit`/`Category`; fill others |
| 6  | **Deduplication** — remove exact duplicate rows |
| 7  | **Filter Invalid Data** — `Sales > 0`, `Quantity > 0`, `Discount` between 0–1 |
| 8  | **Drop Redundant Column** — remove `Country` (zero variance) |
| 9  | **Transformations** — 4 derived columns + uppercase standardisation |
| 10 | **Aggregations** — 7 business-level summary tables |
| 11 | **Save Parquet** — 8 output files + partitioned write |
| 12 | **Validate** — re-read parquet, confirm counts and schema |

---

## Derived Columns (Step 9)

| New Column          | Formula / Logic                                              |
|---------------------|--------------------------------------------------------------|
| `profit_margin_pct` | `(Profit / Sales) × 100` — rounded to 2 decimal places      |
| `is_loss`           | `"Yes"` if Profit < 0, else `"No"`                           |
| `discount_tier`     | `No Discount` / `Low (1-20%)` / `Medium (21-50%)` / `High (>50%)` |
| `effective_revenue` | `Sales × (1 - Discount)` — actual revenue after discount     |

---

## Aggregations (Step 10)

| Aggregation Table   | Grouped By               | Metrics                                    |
|---------------------|--------------------------|--------------------------------------------|
| category_sales      | Category                 | total_sales, total_profit, order_count, avg_profit_margin_pct, avg_discount |
| region_sales        | Region                   | total_sales, total_profit, order_count, avg_profit_margin_pct |
| segment_sales       | Segment                  | total_sales, total_profit, order_count, avg_profit_margin_pct |
| subcategory_sales   | Category + Sub-Category  | total_sales, total_profit, order_count, avg_profit_margin_pct |
| shipmode_sales      | Ship Mode                | total_sales, total_profit, order_count, avg_discount |
| discount_analysis   | discount_tier            | order_count, total_sales, total_profit, avg_profit_margin_pct |
| loss_orders         | Filter: is_loss = "Yes"  | Detailed loss-order records for root cause analysis |

---

## Output Files

| Output | Path | Description |
|--------|------|-------------|
| Cleaned Data | `/Volumes/workspace/default/project_csv/store_sales/output/superstore_cleaned` | Full cleaned dataset with all derived columns |
| Category Sales | `/Volumes/workspace/default/project_csv/store_sales/output/category_sales` | Revenue & profit by category |
| Region Sales | `/Volumes/workspace/default/project_csv/store_sales/output/region_sales` | Revenue & profit by region |
| Segment Sales | `/Volumes/workspace/default/project_csv/store_sales/output/segment_sales` | Revenue & profit by customer segment |
| Sub-Category Sales | `/Volumes/workspace/default/project_csv/store_sales/output/subcategory_sales` | Granular product-level performance |
| Ship Mode Sales | `/Volumes/workspace/default/project_csv/store_sales/output/shipmode_sales` | Performance by shipping method |
| Discount Analysis | `/Volumes/workspace/default/project_csv/store_sales/output/discount_analysis` | Profit impact of each discount tier |
| Loss Orders | `/Volumes/workspace/default/project_csv/store_sales/output/loss_orders` | All orders where Profit < 0 |
| Partitioned | `/Volumes/workspace/default/project_csv/store_sales/output/superstore_partitioned` | Cleaned data partitioned by Region + Category |

---

## Project Structure

```
store-sales-etl-pipeline/
│
├── data/
│   ├── raw/
│   │   └── store_sales.csv
│   └── processed/
│
├── notebooks/
│   └── store_etl.py       ← main ETL script
│
├── output/
│   └── parquet_files/
│
├── screenshots/                ← Databricks run screenshots
│
└── README.md
```

---

## How to Run

### Option 1 — Databricks
1. Upload `store_sales.csv` to DBFS: `/Volumes/workspace/default/project_csv/store_sales/input/store_sales.csv`
2. Open a new notebook and paste the contents of `store_etl.py`
3. Update the path at the top of the script:
   ```python
   CSV_PATH = "/Volumes/workspace/default/project_csv/store_sales/input/store_sales.csv"
   ```
4. Run all cells

### Option 2 — Local PySpark
```bash
pip install pyspark
python store_etl.py
```
> Make sure `store_sales.csv` is in the same directory as the script.

---

## Key Business Insights This Pipeline Surfaces

- Which **product categories** are most/least profitable
- Which **regions** generate the highest losses
- How **discount levels** destroy profit margins (high discount → loss)
- Which **sub-categories** should be reviewed (e.g. Tables, Bookcases often show negative profit)
- Which **ship modes** are used most and their average discounting behaviour

---

## Skills Demonstrated

- PySpark DataFrame API
- ETL pipeline design & orchestration
- Data quality checks (nulls, duplicates, invalid ranges)
- Derived metric engineering (`profit_margin_pct`, `discount_tier`)
- Multi-level GroupBy aggregations
- Parquet storage & partitioning strategy
- Business insight extraction via data engineering

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.x | Programming language |
| PySpark 3.x | Distributed data processing |
| Databricks | Cloud execution environment |
| Parquet | Optimised columnar storage format |
| GitHub | Version control & portfolio |
