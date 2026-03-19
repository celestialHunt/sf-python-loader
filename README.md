# ⚡ SF-Bulk-Flow (or your chosen name)

A robust Python-based data migration tool designed for architects and developers handling massive Salesforce datasets (1M - 100M+ rows). 

## 🌟 Key Features
* **Chunked Processing:** Seamlessly handles 8M+ rows without exhausting system RAM.
* **Waterfall Deduplication:** Multi-level matching logic (Name + DOB -> Email -> Phone -> Address).
* **Idempotent Operations:** Safely restart interrupted migrations without creating duplicates.
* **Auto-Formatting:** Built-in date and currency normalization for Salesforce-ready uploads.
* **Real-time Monitoring:** Detailed progress tracking with timestamps and Bulk Job ID logging.

## 🛠️ Tech Stack
* **Python 3.9+**
* **Pandas:** High-speed data manipulation.
* **Simple-Salesforce:** REST/Bulk API interaction.
* **ConfigParser:** Decoupled settings management.

## 📥 Installation
1. Clone the repo: `git clone https://github.com/celestialHunt/repo-name.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure your `config.ini` with Salesforce credentials.

## 🚦 Quick Start
```bash
python run.py


## ⚖️ License

Distributed under the MIT License. See `LICENSE` for more information.