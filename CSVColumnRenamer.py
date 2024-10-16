import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import errorcode
import numpy as np

class CSVtoDatabase:
    def __init__(self, host, user, password, database, csv_filepath):
        """
        Initialize the class by loading the CSV, renaming columns, and creating a database and table.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.csv_filepath = csv_filepath
        
        # Load the CSV file
        self.data = pd.read_csv(csv_filepath)
        # Rename columns based on the rules provided

        # Create the database and table based on the CSV columns
        self.create_database_and_table()

        # Set up the engine for future database queries
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
        self.connection = self.engine.connect()

    def create_database_and_table(self):
        """
        Create a database and table based on the CSV columns.
        """
        try:
            cnx = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            cursor = cnx.cursor()

            # Create the database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database};")
            cursor.execute(f"USE {self.database};")

            # Create the table based on the CSV column names
            create_table_query = "CREATE TABLE IF NOT EXISTS upload_data_d1 (\n"
            for col in self.data.columns:
                create_table_query += f"`{col}` VARCHAR(255),\n"
            create_table_query = create_table_query.rstrip(",\n")  # Remove trailing comma
            create_table_query += "\n);"

            cursor.execute(create_table_query)
            cnx.commit()

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        finally:
            cursor.close()
            cnx.close()

    def insert_data(self):
        """
        Inserts the CSV data into the database table.
        """
        try:
            cnx = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = cnx.cursor()

            # Replace NaN values with None
            df = self.data.replace({np.nan: None})

            # Generate the insert query dynamically based on the DataFrame columns
            columns = df.columns
            insert_query = f"""
            INSERT INTO upload_data_d3 ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            """

            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))

            cnx.commit()

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
            cnx.close()

    def create_new_database_from_query(self, new_db_name, query):
        """
        Creates a new database and inserts data into it based on a given query,
        excluding null values from the inserted records.
        """
        try:
            # Connect to the MySQL server
            cnx = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            cursor = cnx.cursor()

            # Create a new database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {new_db_name};")
            cursor.execute(f"USE {new_db_name};")

            # Execute the query to fetch data
            result_df = pd.read_sql(query, self.connection)

            # Create a new table in the new database based on the query result
            create_table_query = "CREATE TABLE IF NOT EXISTS upload_data_avg (\n"
            for col in result_df.columns:
                create_table_query += f"`{col}` VARCHAR(255),\n"
            create_table_query = create_table_query.rstrip(",\n")  # Remove trailing comma
            create_table_query += "\n);"

            cursor.execute(create_table_query)

            # Insert the data into the new table, excluding null values
            for _, row in result_df.iterrows():
                # Create a dictionary with non-null values
                non_null_row = {col: row[col] for col in result_df.columns if pd.notnull(row[col])}

                if non_null_row:  # Only proceed if there are non-null values
                    columns = ', '.join(non_null_row.keys())
                    placeholders = ', '.join(['%s'] * len(non_null_row))
                    values = tuple(non_null_row.values())

                    insert_query = f"""
                    INSERT INTO upload_data_avg ({columns})
                    VALUES ({placeholders})
                    """
                    cursor.execute(insert_query, values)

            cnx.commit()

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
            cnx.close()


    def close_connection(self):
        self.connection.close()
path = 'mlab_uploads_d1_data.csv'
db = CSVtoDatabase('localhost', 'root', 'young2003Toro', 'mlab_data', path)
# db.insert_data()  # Insert CSV data into the database
query = """
    SELECT 
	date AS Day,
        AVG(MeanThroughputMbps) AS Throughput, 
        AVG(MinRTT) AS AVGMinRTT,
        CountryName,
        city, 
        Latitude, Longitude,
        ASName AS ISP 
    FROM
	    upload_data_d1
    GROUP BY 
		Day, 
		city, 
        CountryName,
        Latitude, Longitude,
		ASName;
    """
db.create_new_database_from_query('mlab_data', query)
