import psycopg2
import csv
import logging
import configparser
import os
from datetime import datetime

# Set up logging
logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), '..', 'logs', 'etl_log.log'),
                    level=logging.INFO,
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
        full_name = f"{row['Firstname']} {row['Lastname']}"
        row['full_name'] = full_name

        # Convert dates to proper format
        row['Join Date'] = datetime.strptime(row['Join Date'], '%Y-%m-%d').date()
        row['Date of Birth'] = datetime.strptime(row['Date of Birth'], '%Y-%m-%d').date()

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
                full_name VARCHAR,
                employee_id VARCHAR,
                manager_name VARCHAR,
                join_date DATE,
                date_of_birth DATE,
                employee_age INTEGER,
                employee_salary INTEGER,
                employee_department VARCHAR
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
                    INSERT INTO employees (full_name, employee_id, manager_name, join_date, date_of_birth, employee_age, employee_salary, employee_department)
                    VALUES (%(full_name)s, %(Employee ID)s, %(Employee Manager Name)s, %(Join Date)s, %(Date of Birth)s, %(Employee Age)s, %(Employee Salary)s, %(Employee Department Name)s);
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
