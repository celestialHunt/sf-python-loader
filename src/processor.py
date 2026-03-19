import pandas as pd
from simple_salesforce import Salesforce
import sys
import re
import os
import uuid
from datetime import datetime

class SalesforceDataProcessor:
    def __init__(self, config):
        try:
            self.sf = Salesforce(
                instance_url=config['instance_url'],
                username=config['username'],
                password=config['password'],
                security_token=config.get('token', '')
            )
            print("✅ Salesforce Connection Established")
        except Exception as e:
            print(f"❌ Connection Error: {e}")
            sys.exit(1)

    def process_and_upload_data(self, data_config, mapping_config):
        # 1. Configuration Setup
        input_file = data_config['input_file']
        log_file = data_config.get('log_file', 'migration_log.csv')
        object_name = data_config['target_object']
        
        upsert_key = data_config.get('external_id', 'Row_ID__c')
        dup_field = data_config.get('duplicate_indicator_field', 'IsDuplicate__c')
        comp_field = data_config.get('composite_key_field', 'Composite_Key__c')
        batch_size = int(data_config.get('batch_size', 10000))
        key_rule = data_config.get('unique_key_rule', '')
        strategy = data_config.get('strategy', 'unique_only').lower()

        # 2. THE TECH LEAD GUARDRAIL
        # Prevents accidental data squashing/overwriting in tag_all mode
        if strategy == 'tag_all' and upsert_key == comp_field:
            print("\n❌ ARCHITECTURAL CONFLICT DETECTED:")
            print(f"Strategy '{strategy}' requires a unique Physical ID (like Row_ID__c).")
            print(f"Using '{upsert_key}' would cause Salesforce to overwrite duplicates, losing data.")
            print("Please fix your config.ini and try again.")
            return False

        try:
            if not os.path.exists(input_file):
                print(f"❌ Error: {input_file} not found.")
                return False

            chunks = pd.read_csv(input_file, chunksize=100000)
            seen_keys = set()
            first_chunk = True

            print(f"🚀 Starting Migration in '{strategy}' mode using '{upsert_key}' as anchor...")

            for chunk_num, chunk in enumerate(chunks, start=1):
                # 3. GENERATE LOGICAL COMPOSITE KEY
                rule_cols = re.findall(r'\{(.*?)\}', key_rule)
                def build_key(row):
                    k = str(key_rule)
                    for col in rule_cols:
                        val = str(row[col]) if pd.notnull(row[col]) else ""
                        k = k.replace(f'{{{col}}}', val.lower().strip())
                    return k
                
                chunk['TEMP_COMPOSITE_KEY'] = chunk.apply(build_key, axis=1)

                # 4. MAPPING & AUTO-DATE FORMATTING
                upload_df = pd.DataFrame()
                for sf_field, mapping_val in mapping_config.items():
                    if '{' in mapping_val:
                        map_cols = re.findall(r'\{(.*?)\}', mapping_val)
                        def build_val(row):
                            m = str(mapping_val)
                            for col in map_cols:
                                val = str(row[col]) if pd.notnull(row[col]) else ""
                                m = m.replace(f'{{{col}}}', val.strip())
                            return m
                        col_series = chunk.apply(build_val, axis=1)
                    else:
                        col_series = chunk[mapping_val]

                    # Auto-fix dates (Matches fields ending in Date__c or PersonBirthdate etc)
                    if any(x in sf_field.lower() for x in ['date', 'birthdate']):
                        try:
                            # Handles formats like 5/15/1990, 15-05-1990, etc.
                            col_series = pd.to_datetime(col_series).dt.strftime('%Y-%m-%d')
                        except:
                            pass 
                    
                    upload_df[sf_field] = col_series

                # 5. SYSTEM FIELD INJECTION
                upload_df[comp_field] = chunk['TEMP_COMPOSITE_KEY']
                
                # Flag Logic: True if duplicated in this chunk OR seen in previous chunks
                is_dup = chunk['TEMP_COMPOSITE_KEY'].duplicated(keep='first') | chunk['TEMP_COMPOSITE_KEY'].isin(seen_keys)
                upload_df[dup_field] = is_dup
                seen_keys.update(chunk['TEMP_COMPOSITE_KEY'].tolist())

                # Inject Unique Row IDs if they are not in the CSV but needed for upload
                if upsert_key not in upload_df.columns:
                    upload_df[upsert_key] = [str(uuid.uuid4()) for _ in range(len(chunk))]

                # 6. EXECUTION
                sf_bulk = getattr(self.sf.bulk, object_name)
                
                if strategy == 'tag_all':
                    data_to_send = upload_df.to_dict('records')
                    print(f"📤 Chunk {chunk_num}: Uploading {len(data_to_send)} records (Full Staging)...")
                    sf_bulk.upsert(data_to_send, upsert_key, batch_size=batch_size)
                else:
                    # Filter out duplicates before sending to API
                    uniques = upload_df[upload_df[dup_field] == False].to_dict('records')
                    if uniques:
                        print(f"📤 Chunk {chunk_num}: Uploading {len(uniques)} unique records (Clean Merge)...")
                        sf_bulk.upsert(uniques, upsert_key, batch_size=batch_size)
                    print(f"⏭️ Skipped {len(upload_df) - len(uniques)} logical duplicates.")

                # 7. LOGGING (Captures what was actually sent/flagged)
                mode = 'w' if first_chunk else 'a'
                upload_df.to_csv(log_file, mode=mode, index=False, header=first_chunk)
                first_chunk = False
                print(f"✅ Chunk {chunk_num} Complete.")
                print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] Progress: {((chunk_num-1) * 100000) + len(upload_df):,} rows processed.")

            print("\n🏆 MIGRATION COMPLETED SUCCESSFULLY")
            return True

        except Exception as e:
            print(f"❌ Critical Runtime Error: {e}")
            return False