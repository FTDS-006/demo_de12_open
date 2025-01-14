from datetime import datetime
import snowflake.connector

def monthly_orders_category_level():
    schema = 'datamart'
    newtable = 'NORTHWIND.datamart.monthly_orders_category_level'
    columns = [
        {'name': 'date_key', 'type': 'DATE', 'column_type': 'matchon'},
        {'name': 'category_id', 'type': 'NUMBER', 'column_type': 'matchon'},
        {'name': 'category_name', 'type': 'string', 'column_type': 'matchon'},
        {'name': 'orders', 'type': 'NUMBER', 'column_type': 'update'}
    ]
    query = '''
    SELECT
        DISTINCT
        DATE_TRUNC('MONTH', ORDER_DATE) AS date_key,
        category_id,category_name,
        count(distinct order_id) orders
    FROM
        NORTHWIND.RAW.ORDERS
    LEFT JOIN
        NORTHWIND.RAW.ORDER_DETAILS
        USING (order_id)
    LEFT JOIN 
        NORTHWIND.RAW.PRODUCTS
        USING (product_id)
    LEFT JOIN 
        NORTHWIND.RAW.categories
        USING(category_id)
    GROUP BY 1,2,3
    '''

    # Connect to Snowflake data warehouse
    sf_conn = snowflake.connector.connect(
        user=sf_username,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

    # Check if table name exists in the schema
    cursor = sf_conn.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{newtable.upper()}'")
    exists = len(cursor.fetchall()) > 0

    if exists:
        # Merge data into the existing table
        merge_query = f'''
            MERGE INTO {newtable} AS target
            USING ({query}) AS source
            ON {' AND '.join(f'target.{col["name"]} = source.{col["name"]}' for col in columns if col['column_type'] == 'matchon')}
            WHEN MATCHED THEN
                UPDATE SET
                    {', '.join(f'target.{col["name"]} = source.{col["name"]}' for col in columns if col['column_type'] == 'update')}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(col["name"] for col in columns if col['column_type'] == 'matchon' or col['column_type'] == 'update')})
                VALUES ({', '.join(f'source.{col["name"]}' for col in columns if col['column_type'] == 'matchon' or col['column_type'] == 'update')})
        '''
        cursor.execute(merge_query)
    else:
        # Generate column definitions
        column_defs = ',\n'.join(f"{col['name']} {col['type']}" for col in columns)

        # Create the table
        create_query = f'''
            CREATE OR REPLACE TABLE {newtable} (
                {column_defs}
            )
            CLUSTER BY (date_key)
            AS
            {query}
        '''
        cursor.execute(create_query)

    # Close the cursor and connection
    cursor.close()
    sf_conn.close()
