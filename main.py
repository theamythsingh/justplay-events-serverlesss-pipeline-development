import pandas as pd
import yaml
import time
import os
import logging
import mysql.connector
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine, text

logging.basicConfig(filename='conversion_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVHandler(FileSystemEventHandler):
    def __init__(self, input_folder, output_folder, schema_file):
        super().__init__()
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.schema_file = schema_file
        self.schema = self.load_schema(schema_file)
        self.counter = 0
        self.conversion_complete_flag = False
        self.parquet_processing_times = []
        self.total_csv_size = 0
        self.total_parquet_files = 0
        self.dfs = []

    def load_schema(self, schema_file):
        with open(schema_file, 'r') as f:
            return yaml.safe_load(f)

    def validate_schema(self, df):
        return set(df.columns) == set(self.schema.keys())

    def conversion_complete(self):
        return self.conversion_complete_flag

    def get_database_credentials(self):
        try:
            with open("config.yaml", 'r') as stream:
                credentials = yaml.safe_load(stream)
                return credentials['database']
        except FileNotFoundError:
            logging.error("Config file not found!")
            return None

    def get_schema_from_file(self):
        try:
            with open("schema_sql.yaml", 'r') as stream:
                schema = yaml.safe_load(stream)
                return schema
        except FileNotFoundError:
            logging.error("Schema file not found!")
            return None

    @staticmethod
    def table_exists(cursor, table_name):
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return bool(cursor.fetchone())

    def create_table(self):
        conn = None
        db_credentials = self.get_database_credentials()
        schema = self.get_schema_from_file()
        
        if db_credentials and schema:
            try:
                conn = mysql.connector.connect(**db_credentials)
                cursor = conn.cursor()
                table_name = next(iter(schema))
                
                if not self.table_exists(cursor, table_name):
                    columns = schema[table_name]
                    create_table_query = f"CREATE TABLE {table_name} ("
                    
                    for column in columns:
                        column_name = column['name']
                        column_type = column['type']
                        create_table_query += f"{column_name} {column_type}, "
                        
                    create_table_query = create_table_query[:-2]
                    create_table_query += ")"
                    
                    cursor.execute(create_table_query)
                    logging.info(f"'{table_name}' table created successfully in schema '{db_credentials['database']}'")
                else:
                    logging.info("Table already exists")
                    
            except mysql.connector.Error as e:
                logging.error(f"Error creating table: {e}")
                
            finally:
                if conn and conn.is_connected():
                    cursor.close()
                    conn.close()
        else:
            logging.error("Database credentials or schema not available")

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        start_time_csv = time.time()
        try:
            df = pd.read_csv(event.src_path, delimiter=';').dropna()
        except Exception as e:
            logging.error(f"Error processing file: {event.src_path}. {e}")
            return

        df.columns = map(str.lower, df.columns)
        df = df.map(lambda x: str(x).lower() if isinstance(x, str) else str(x))
        df = df.drop_duplicates()
        total_rows = len(df)
        logging.info(f'Total no of rows in DataFrame{total_rows}')
        print(f'Total rows for this file :{total_rows}')

        parquet_filename = os.path.join(self.output_folder, f"{os.path.splitext(os.path.basename(event.src_path))[0]}.parquet")
        start_time_parquet = time.time()
        df.to_parquet(parquet_filename)
        end_time_parquet = time.time()
        parquet_processing_time = end_time_parquet - start_time_parquet
        self.parquet_processing_times.append(parquet_processing_time)
        csv_size = os.path.getsize(event.src_path)
        self.total_csv_size += csv_size
        logging.info(f"Converted {event.src_path} to Parquet format and uploaded to {parquet_filename}")
        print(f"Conversion successful: {event.src_path} uploaded to {parquet_filename}")
        self.delete_csv(event.src_path)
        self.total_parquet_files += 1
        self.conversion_complete_flag = False

        try:
            self.load_df_to_mysql(df, self.get_database_credentials(), "student_data", if_exists='append')
        except KeyboardInterrupt:
            logging.warning("KeyboardInterrupt: Data loading operation interrupted.")
            return

    def move_to_failed(self, csv_file):
        failed_folder = os.path.join(os.path.dirname(csv_file), 'output_failed')
        os.makedirs(failed_folder, exist_ok=True)
        dest = os.path.join(failed_folder, os.path.basename(csv_file))
        os.rename(csv_file, dest)
        logging.info(f"Moved unsuccessful CSV file {csv_file} to output_failed folder")

    def load_df_to_mysql(self, df, db_config, table_name, if_exists='append'):
        try:
            connection_string = f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            engine = create_engine(connection_string)
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info(f"DataFrame successfully loaded into MySQL table '{table_name}'")
        except Exception as e:
            logging.error(f"An error occurred while loading DataFrame into MySQL: {e}")

    @staticmethod
    def delete_csv(csv_file):
        os.remove(csv_file)
        logging.info(f"Deleted CSV file {csv_file}")

def watch_input_csv_folder(input_folder, output_folder, schema_file):
    observer = Observer()
    handler = CSVHandler(input_folder, output_folder, schema_file)
    observer.schedule(handler, path=input_folder)
    observer.start()
    logging.info("Watching input CSV folder...")
    print("Watching input CSV folder...")
    try:
        while not handler.conversion_complete():
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()

    observer.stop()
    observer.join()

    handler.conversion_complete_flag = True
    return True

if __name__ == "__main__":
    input_csv_folder = 'input_csv'
    output_parquet_folder = 'output_parquet'
    schema_file = 'schema.yaml'
    os.makedirs(output_parquet_folder, exist_ok=True)
    observer = Observer()   
    watch_input_csv_folder(input_csv_folder, output_parquet_folder, schema_file)
    observer.start() 

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()

    total_parquet_size = sum(os.path.getsize(os.path.join(output_parquet_folder, f)) for f in os.listdir(output_parquet_folder) if f.endswith('.parquet'))
    total_parquet_size_mb = total_parquet_size / (1024 * 1024)   
    csv_handler = CSVHandler(input_csv_folder, output_parquet_folder, schema_file)  
    table_name = "student_data"
    db_config = csv_handler.get_database_credentials()
    csv_handler.create_table()

    logging.info(f"Total Parquet processing time: {sum(csv_handler.parquet_processing_times):.2f} seconds")
    logging.info(f"Total Parquet size: {total_parquet_size_mb:.2f} MB")
    logging.info(f"Total Parquet files converted: {csv_handler.total_parquet_files}")