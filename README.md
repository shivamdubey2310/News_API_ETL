# 📰 Tech News ETL Pipeline

An automated ETL pipeline that extracts the top daily technology news from NewsAPI, transforms the data using PySpark, and loads it into MongoDB Atlas. The pipeline is scheduled to run daily using cron jobs, with intermediate data stored in Parquet format.

---

## 📌 Project Overview

This project aims to build a lightweight, automated ETL workflow to gather and store the latest tech news headlines. The resulting data can be used for analytics, dashboards, or further downstream processing.

---

## ⚙️ ETL Workflow

- **Extract**: Fetches top technology headlines from [NewsAPI](https://newsapi.org/) using Python's `requests` library.
- **Transform**: Processes and cleans the extracted data using PySpark, and saves it in Parquet format.
- **Load**: Uploads the transformed data into a MongoDB Atlas collection.
- **Schedule**: A cron job automates the pipeline to run daily.

---

## 🧰 Tech Stack

- **Programming Language**: Python
- **Libraries**: `requests`, `pyspark`, `pymongo`
- **Data Format**: JSON (raw), Parquet (processed)
- **Database**: MongoDB Atlas
- **Orchestration**: Cron (Linux scheduler)

---

## 📁 Directory Structure

```
tech-news-etl/
├── Extract.py         # Extracts news data from NewsAPI
├── Transform.py       # Cleans and formats data using PySpark
├── Load.py            # Loads data into MongoDB Atlas
├── requirements.txt   # Python dependencies
├── Main.sh            # Shell script to run the pipeline
└── README.md          # Project documentation
```

---

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/tech-news-etl.git
cd tech-news-etl
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
create `.env` to include:
- Your **NewsAPI key**
- Your **MongoDB Atlas URI**

### 4. Run the Pipeline Manually
```bash
python Extract.py
python Transform.py
python Load.py
```

### 5. Automate with Cron (Optional)
Add the following line to your crontab (`crontab -e`) to schedule daily execution:
```bash
0 8 * * * /path/to/Main.sh
```

---

## ✅ Sample Output
- **Input**: Raw JSON response from NewsAPI
- **Output**: Cleaned tech news articles stored in MongoDB with fields like `title`, `source`, `publishedAt`, `description`, and `url`

---

## 🙌 Acknowledgements
- [NewsAPI.org](https://newsapi.org/) for providing free access to real-time news data.

---

> Built with ❤️ by Shivam

