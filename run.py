import configparser
import time
from datetime import timedelta
from src.processor import SalesforceDataProcessor

def main():
    start_time = time.time()
    config = configparser.ConfigParser()
    config.read('config.ini')

    print(f"🚀 Migration Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    processor = SalesforceDataProcessor(config['SALESFORCE'])

    if processor.process_and_upload_data(config['DATA'], config['MAPPING']):
        print("\n🏆 JOB COMPLETED SUCCESSFULLY")
    else:
        print("\n⚠️ JOB FINISHED WITH ERRORS.")

    duration = str(timedelta(seconds=round(time.time() - start_time)))
    print(f"⏱️ Total Execution Time: {duration}")

if __name__ == "__main__":
    main()