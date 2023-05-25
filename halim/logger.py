from snowflake.connector import connect



def logger(file, status, time, desc):
    # Connect to Snowflake
    conn = connect(
        user=sf_username,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

    try:
        # Create a cursor to execute Snowflake queries
        cursor = conn.cursor()

        # Prepare the query to insert the log data into Snowflake
        insert_query = f"INSERT INTO {sf_table} (date_time, task, status, description) VALUES (%s, %s, %s, %s)"
        values = (time, file, status, desc)

        # Execute the query
        cursor.execute(insert_query, values)

        # Commit the changes
        conn.commit()

        print("Log data inserted into Snowflake successfully!")
    except Exception as e:
        print("Error while inserting log data into Snowflake:", e)
    finally:
        # Close the cursor and the connection
        cursor.close()
        conn.close()