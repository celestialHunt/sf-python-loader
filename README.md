# ⚡ SF-Bulk-Flow

A robust Python-based data migration tool designed for architects handling massive Salesforce datasets (1M - 100M+ rows).

## 🌟 Key Features
* **Chunked Processing:** Seamlessly handles 8M+ rows without exhausting system RAM.
* **Waterfall Deduplication:** Multi-level matching logic (Name + DOB -> Email -> Phone).
* **Idempotent Operations:** Safely restart interrupted migrations using External IDs.
* **Auto-Formatting:** Built-in date normalization for Salesforce-ready uploads.
* **Self-Healing:** Automatically creates required `data/` and `logs/` directories.

## 📂 Project Structure
* `src/`: Core logic and Salesforce processor.
* `data/`: Place your source CSV files here.
* `logs/`: Detailed migration logs and error reports.
* `scripts/`: Utility scripts for data generation.

## 📥 Installation & Setup
1. **Clone & Install:**
   ```bash
   git clone [https://github.com/celestialHunt/sf-python-loader.git](https://github.com/celestialHunt/sf-python-loader.git)
   pip install -r requirements.txt