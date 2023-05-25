#libries needed
import psycopg2
from datetime import datetime
from snowflake.connector import connect

#import logger function
from logger import logger

#creds needs to be hidden
# PostgreSQL database credentials


def merge_data():
    try:
        logger("merge_data", "start", datetime.now(), "")  
        # Connect to PostgreSQL database
        pg_conn = psycopg2.connect(
            user=pg_username,
            password=pg_password,
            host=pg_host,
            port=pg_port,
            database=pg_database
        )

        # Create a cursor to execute PostgreSQL queries
        pg_cursor = pg_conn.cursor()

        try:
            # Connect to Snowflake data warehouse
            sf_conn = connect(
                user=sf_username,
                password=sf_password,
                account=sf_account,
                warehouse=sf_warehouse,
                database=sf_database,
                schema=sf_schema
            )

            # Retrieve all table names in the PostgreSQL database
            pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            table_names = pg_cursor.fetchall()

            # Loop through each table name
            for table_name in table_names:
                # Retrieve the data type of each column in the table
                pg_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name[0]}'")
                columns = pg_cursor.fetchall()

                # Filter out the BYTEA columns
                columns = [(col[0], col[1]) for col in columns if col[1] != 'bytea']

                # Generate the target table name in Snowflake
                sf_target_table = table_name[0]

                # Retrieve the data from the table in PostgreSQL
                pg_cursor.execute(f"SELECT {', '.join([col[0] for col in columns if col[1] != 'bytea'])} FROM {table_name[0]}")
                data = pg_cursor.fetchall()

                if data:
                    # Check if the table exists in Snowflake
                    sf_cursor = sf_conn.cursor()
                    sf_cursor.execute(f"SHOW TABLES LIKE '{sf_target_table}'")
                    table_exists = sf_cursor.fetchone() is not None
                    sf_cursor.close()

                    if not table_exists:
                        # Generate the CREATE TABLE statement in Snowflake
                        create_table_stmt = f"CREATE TABLE {sf_target_table} ("
                        create_table_stmt += ', '.join([f"{col[0]} {col[1]}" for col in columns])
                        create_table_stmt += ", created_at TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP)"

                        # Create the target table in Snowflake
                        sf_cursor = sf_conn.cursor()
                        sf_cursor.execute(create_table_stmt)
                        sf_cursor.close()

                        print(f"Table '{sf_target_table}' created in Snowflake.")

                        # Insert all data into the newly created table
                        if data:
                            # Convert memoryview objects to bytes
                            data = [tuple(bytes(value) if isinstance(value, memoryview) else value for value in row) for row in data]

                            # Merge the data into the target table in Snowflake
                            sf_cursor = sf_conn.cursor()
                            for row in data:
                                converted_row = list(row)
                                converted_row.append(datetime.now())
                                sf_cursor.execute(f"INSERT INTO {sf_target_table} VALUES ({','.join(['%s' for _ in range(len(converted_row))])})", converted_row)
                            sf_conn.commit()
                            sf_cursor.close()

                            print(f"Data merged from {table_name[0]} to Snowflake successfully!")

                    else: 
                        # Check the row count in Snowflake
                        sf_cursor = sf_conn.cursor()
                        sf_cursor.execute(f"SELECT COUNT(*) FROM {sf_target_table}")
                        sf_row_count = sf_cursor.fetchone()[0]

                        if sf_row_count < len(data):
                            # Convert memoryview objects to bytes
                            data = [tuple(bytes(value) if isinstance(value, memoryview) else value for value in row) for row in data]

                            # Get the column names from the Snowflake table
                            sf_cursor = sf_conn.cursor()
                            sf_cursor.execute(f"DESCRIBE TABLE {sf_target_table}")
                            columns = sf_cursor.fetchall()
                            sf_cursor.close()

                            # Find the index of the unique column in the Snowflake table
                            unique_column_index = None
                            for i, column in enumerate(columns):
                                if column[2].lower() == 'unique_column':  # Replace 'unique_column' with the actual column name
                                    unique_column_index = i
                                    break

                            if unique_column_index is not None:
                                # Filter out the data that is already present in Snowflake
                                existing_data = []
                                sf_cursor = sf_conn.cursor()
                                for row in data:
                                    # Check if the row exists in Snowflake based on the unique identifier
                                    sf_cursor.execute(f"SELECT COUNT(*) FROM {sf_target_table} WHERE {columns[unique_column_index][0]} = %s", (row[unique_column_index],))
                                    count = sf_cursor.fetchone()[0]
                                    if count == 0:
                                        existing_data.append(row)
                                sf_cursor.close()

                                if existing_data:
                                    # Merge the filtered data into the target table in Snowflake
                                    sf_cursor = sf_conn.cursor()
                                    for row in existing_data:
                                        converted_row = list(row)
                                        converted_row.append(datetime.now())
                                        sf_cursor.execute(f"INSERT INTO {sf_target_table} VALUES ({','.join(['%s' for _ in range(len(converted_row))])})", converted_row)
                                    sf_conn.commit()
                                    sf_cursor.close()

                            print(f"Data merged from {table_name[0]} to Snowflake successfully!")
                        else:
                            print(f"No new data to merge from {table_name[0]}. Skipping merge operation.")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL or merging data to Snowflake:", error)
        finally:
            # Close the PostgreSQL cursor and connection
            pg_cursor.close()
            pg_conn.close()

            # Close the Snowflake connection
            sf_conn.close()
        logger("merge_data", "end", datetime.now(), len(existing_data))  #-----------logger
    except Exception as E:
        logger("merge_data", "error", datetime.now(), E)  #-------------logger


