import psycopg2
import csv
import logging
import configparser
import os

# Set up logging
logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), '..', 'logs', 'etl_log.log'),
                    level=logging.INFO,  # Change this line
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Read configuration from config.ini
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))

class ETLPipeline:
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def _transform_data(self, row):
        # Combine first name and last name into a single 'full_name' field
        full_name = f"{row['first_name']} {row['last_name']}"
        row['full_name'] = full_name
        return row

    def run(self):
        try:
            # Establish database connection
            conn = psycopg2.connect(
                host=config['postgresql']['host'],
                database=config['postgresql']['database'],
                user=config['postgresql']['user'],
                password=config['postgresql']['password']
            )
            cursor = conn.cursor()

            # Create table if it doesn't exist
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR,
                email VARCHAR,
                department VARCHAR
            );
            '''
            cursor.execute(create_table_query)
            conn.commit()

            # Read CSV file and insert data into the database
            with open(os.path.join(os.path.dirname(__file__), '..', 'data', self.csv_file), 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    transformed_row = self._transform_data(row)
                    insert_query = '''
                    INSERT INTO employees (first_name, last_name, full_name, email, department)
                    VALUES (%(first_name)s, %(last_name)s, %(full_name)s, %(email)s, %(department)s);
                    '''
                    cursor.execute(insert_query, transformed_row)
                    conn.commit()

            cursor.close()
            conn.close()
            logger.info("ETL process completed successfully.")

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    etl = ETLPipeline('employees.csv')
    etl.run()
