import psycopg2
import pandas as pd

from prefect import task, flow
from prefect.variables import Variable
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials

from google.cloud import bigquery

from datetime import datetime, timedelta

#defining variables
db_host = Secret.load("db-host").get()
db_name = Secret.load("db-name").get()
db_user = Secret.load("db-username").get()
db_password = Secret.load("db-password").get()
gcp_creds = GcpCredentials.load("gcp-service-account")
google_auth_creds = gcp_creds.get_credentials_from_service_account()
client = bigquery.Client(credentials=google_auth_creds)


#task for extracting data from database
@task(retries=3, retry_delay_seconds=60)
def extract_data(table_name):
    get_last_extracted_at = Variable.get("last_extracted_at", default=None)
    if get_last_extracted_at is not None:
        last_extracted_at = datetime.fromisoformat(get_last_extracted_at.value)
    else:
        last_extracted_at = datetime.now() - timedelta(days=20)

    try:
        with psycopg2.connect(
            host=db_host,
            port=5432,
            database=db_name,
            user=db_user,
            password=db_password
        ) as connection:
            with connection.cursor() as cursor:

                # Check for existing columns in the table
                cursor.execute(f"""
                    SELECT 
                        EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'created_at') AS has_created_at,
                        EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'updated_at') AS has_updated_at,
                        EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'deleted_at') AS has_deleted_at;
                """)

                has_created_at, has_updated_at, has_deleted_at = cursor.fetchone()

                # Build WHERE clause dynamically
                where_conditions = []
                params = []
                if has_created_at:
                    where_conditions.append("created_at >= %s")
                    params.append(last_extracted_at)
                if has_updated_at:
                    where_conditions.append("updated_at >= %s")
                    params.append(last_extracted_at)
                if has_deleted_at:
                    where_conditions.append("deleted_at IS NULL")

                # Construct the query
                query = f"SELECT * FROM {table_name}"
                if where_conditions:
                    query += " WHERE " + " OR ".join(where_conditions)

                cursor.execute(query, params)
                rows = cursor.fetchall()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=column_names)
                    print(f"Successfully extracted {len(rows)} rows from {table_name}")
                    return df
                else:
                    print(f"No new or updated data found in {table_name}")
                    return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

#task for transforming data
@task(task_run_name="transform_users")
def transform_users(users):
    if users is not None:
        timestamps = ['created_at', 'updated_at', 'deleted_at']
        users['fullname'] = users['first_name'] + ' ' + users['last_name']
        df_transformed = users[['id', 'fullname', 'email', 'phone', 'gender', 'date_of_birth'] + timestamps]
        
        return df_transformed
    else:
        print("Empty DataFrame from extract_data")
        return None

@task(task_run_name="transform_user_addresses")
def transform_user_address(addresses):
    if addresses is not None:
        timestamps = ['created_at', 'updated_at', 'deleted_at']
        df_transformed = addresses[['id', 'user_id', 'label', 'address', 'city', 'province', 'postal_code', 'is_primary'] + timestamps]
        
        return df_transformed
    else:
        print("Empty DataFrame from extract_data")
        return None

@task(task_run_name="transform_products")
def transform_products(products):
    if products is not None:
        timestamps = ['created_at', 'updated_at', 'deleted_at']
        df_transformed = products[['id', 'category_id', 'name', 'description'] + timestamps]
        
        return df_transformed
    else:
        print("Empty DataFrame from extract_data")
        return None

@task(task_run_name="transform_product_transactions")
def transform_product_transactions(product_transactions, cart_items, product_variants, products):
    if product_transactions is not None:
        df_transformed = (
            product_transactions
            .merge(cart_items, left_on='cart_id', right_on='cart_id')
            .merge(product_variants, left_on='product_variant_id', right_on='id')
            .merge(products, left_on='product_id', right_on='id', suffixes=["_a","_b"])
            [["id_x", "user_id", "product_id", "category_id", "tracsaction_date", "quantity", "total_amount", "transaction_status", "transaction_method" ]]
            .rename(columns={"id_x": "id", "tracsaction_date": "transaction_date"})
        )
        
        return df_transformed
    else:
        print("Empty DataFrame from extract_data")
        return None

@task(task_run_name="transform_events")
def transform_events(events):
    if events is not None:
        timestamps = ['created_at', 'updated_at', 'deleted_at']
        df_transformed = events[['id', 'location_id', 'category_id', 'name', 'description', 'date'] + timestamps]
        
        return df_transformed
    else:
        print("Empty DataFrame from extract_data")
        return None

@task()
def transform_event_transactions(event_transactions, event_prices, events):
    if event_transactions is not None:
        df_transformed = (
            event_transactions
            .merge(event_prices, left_on='event_price_id', right_on='id')
            .merge(events, left_on="event_id", right_on='id')
            [["id_x", "user_id", "event_id","category_id", "event_price_id", "transaction_date",'quantity', "total_amount","transaction_status", "transaction_method"]]
            .rename(columns={"id_x": "id"})
        )
    
        return df_transformed
    else:
        print("Empty DataFrame from extract_data")
        return None

#task for loading data into bigquery
@task(retries=3, retry_delay_seconds=60)
def load_data(df, table_name, primary_key="id"):
    
    if df is None:
        print(f"No data to load into {table_name}")
        return
    
    project_id = 'de-cloud-07'
    table_id = f"{project_id}.kreasi_nusantara.{table_name}"
    staging_table_id = f"{project_id}.staging_tables.{table_name}"

    # load data into staging table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=",",
        autodetect=True,
        write_disposition='WRITE_APPEND'
    )

    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()

    # MERGE statement
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{staging_table_id}` AS source
    ON target.{primary_key} = source.{primary_key}
    WHEN MATCHED THEN
        UPDATE SET 
    """

    for column in df.columns:
        if column != primary_key:
            merge_query += f"target.{column} = source.{column}, "

    merge_query = merge_query[:-2]  
    merge_query += """
    WHEN NOT MATCHED THEN
        INSERT ({all_columns})
        VALUES ({all_columns});
    """

    all_columns = ", ".join(df.columns)  
    merge_query = merge_query.format(all_columns=all_columns)

    job = client.query(merge_query)
    job.result()

    # Truncate staging table 
    truncate_query = f"TRUNCATE TABLE `{staging_table_id}`"
    job = client.query(truncate_query)
    job.result()

@flow(name="etl-flow", log_prints=True)
def data_pipeline():
    
    # defines table names to be extracted and loaded
    tables = ['users', 'user_addresses', 'products', 'product_categories', 'product_pricings','product_variants','product_transactions', 'cart_items','product_reviews', 'events', 'event_categories', 'event_locations', 'event_prices', 'event_transactions', 'event_ticket_types']

    extracted_data = {}
    for table in tables:
        extracted_data[table] = extract_data.with_options(task_run_name=f"extract_{table}")(table)

    # Handle None results from extraction tasks
    tables_to_remove = []  # Create a separate list to store tables to remove
    for table, df in extracted_data.items():
        if df is None:
            print(f"Extract task returned None for {table}. Skipping this table.")
            tables_to_remove.append(table)

    # Remove the tables outside the iteration
    for table in tables_to_remove:
        del extracted_data[table]
    
    if not extracted_data:
        print("Extract task returned empty DataFrame for all tables")
        return

    df_users = transform_users(extracted_data.get('users'))
    df_user_addresses = transform_user_address(extracted_data.get('user_addresses'))
    df_products = transform_products(extracted_data.get('products'))
    df_events = transform_events(extracted_data.get('events'))
    df_product_transactions = transform_product_transactions(extracted_data.get('product_transactions'), extracted_data.get('cart_items'), extracted_data.get('product_variants'), extracted_data.get('products'))
    df_event_transactions = transform_event_transactions(extracted_data.get('event_transactions'), extracted_data.get('event_prices'), extracted_data.get('events'))

    load_tables = {
        'users': df_users, 
        'user_addresses': df_user_addresses, 
        'products': df_products, 
        'product_categories': extracted_data.get('product_categories'),
        'product_pricings' : extracted_data.get('product_pricings'),
        'product_transactions': df_product_transactions,
        'product_reviews': extracted_data.get('product_reviews'),
        'events': df_events,
        'event_categories': extracted_data.get('event_categories'),
        'event_locations': extracted_data.get('event_locations'),
        'event_transactions': df_event_transactions,
        'event_prices': extracted_data.get('event_prices'),
        'event_ticket_types': extracted_data.get('event_ticket_types'),
    }

    for table, df in load_tables.items():
        load_data.with_options(task_run_name=f"load_{table}")(df, table)
    
    last_extracted_at = datetime.now().isoformat()
    Variable.set(name="last_extracted_at", value=last_extracted_at, overwrite=True)