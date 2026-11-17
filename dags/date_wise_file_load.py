from airflow.sdk import DAG,task
import pandas as pd
from airflow.sensors.filesystem import FileSensor
from datetime import datetime,date,time
import pendulum
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

import os
import psycopg2

from airflow.hooks.base import BaseHook


def check_datatype(filename):

    print('----------- In data type check function-----------------')
    fn = f"/opt/airflow/data/{filename}.csv"
    df=pd.read_csv(fn)

    main_columns = {'order_id':'object','order_item_id':'int64','customer_id':'object','customer_name':'object','customer_email':'object','country_code':'object','country_name':'object','city_name':'object','city_timezone':'object','store_id':'object','store_name':'object','store_country_code':'object','store_country_name':'object','store_city_name':'object','product_id':'object','product_sku':'object','product_name':'object','category_name':'object','brand_name':'object','quantity':'int64','unit_price_inr':'float64','discount_inr':'float64','line_amount_inr':'float64','order_amount_inr':'float64','order_status':'object','payment_mode':'object','payment_status':'object','amount_paid_inr':'float64','event_created_at':'object','event_paid_at':'object','event_shipped_at':'object','event_delivered_at':'object','event_cancelled_at':'object','use_date':'object'}
    file_columns = df.columns

    main_len = len(main_columns)
    df_len = len(file_columns)

    print(main_len)
    print(df_len)

    if main_len!=df_len:
        raise Exception("Not the same number of columns")

    print('----------- Checkimg presence of column -----------------')
    for col in main_columns.keys():
        if col not in file_columns:
            raise Exception(f"{col} column not present in file")

    print('----------- Comparing data types check function-----------------')

    for col in file_columns:
        if df[col].dtype == main_columns[col]:
            print(col,'OK')
        else:
            print(col,'NOT OK')




def func_transformation(filename):

    print('----------- In file transformation function-----------------')

    print(f"/opt/airflow/data/{filename}.csv")
    fn = f"/opt/airflow/data/{filename}.csv"
    df=pd.read_csv(fn)

    # transformations 
    # df = df.drop(columns = ['use_date'])
    df['customer_name_lower'] = df['customer_name'].str.lower() #1
    df['domain'] = df['customer_email'].str.split('@').str[-1] #2
    df['token1'] = df['customer_name_lower'].str.split(' ').str[0]
    df['token2'] = df['customer_name_lower'].str.split(' ').str[-1]
    df['match'] = df.apply(lambda x : (x['token1'] in x['customer_email']) and (x['token2'] in x['customer_email']), axis = 1) # 3

    df.to_csv(f"/opt/airflow/data/{filename}.csv", index = False)

    print(df.columns)
    # print(df['domain'].unique())

def file_check_transform(filename):
    check_datatype(filename)
    func_transformation(filename)


def print_sensor_directory():

    fs_conn = BaseHook.get_connection("fs_default")
    directory = fs_conn.extra_dejson.get("path")
    print("FileSensor directory:", directory)


def func_connect_postgres(filename):

    # Read File
    fn = f"/opt/airflow/data/{filename}.csv"
    df = pd.read_csv(fn)

    file_columns = df.columns
    print(f'Columns in dataframe: {file_columns}')
    

    filename = pd.to_datetime(filename,format="%Y-%m-%d" ).strftime("%d-%m-%Y")

    csv_count = df.shape[0]
    
    # File count 
    print("CSV record count:", csv_count)
    print("Execution date:", filename)
    print("CSV row count:", df.shape[0])

    print("Use date column datatype:")
    print(df["use_date"].dtype)

    print("Unique dates in CSV:")
    print(df["use_date"].unique())

    # Database credentials
    host = os.getenv("POSTGRES_HOST")
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT", "5432")

    engine = None

    try:

        # Create database URL
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=int(port),
            database=database
        )

        # Creating engine
        engine = create_engine(url)

        table_name = "amazon"
        schema_name = "public"

        with engine.begin() as conn:

            # ------------------------ STEP 1: Check whether table exists ------------------------ 
            table_query = text("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables
                    WHERE table_schema = :schema_name
                    AND table_name = :table_name
                    AND table_type = 'BASE TABLE'
                )
            """)

            # Does a table with this name exist inside this schema?" 
            # PostgreSQL answers TRUE or FALSE, and .scalar() stores that answer in table_exists.
            table_exists = conn.execute(
                table_query,
                {
                    "schema_name": schema_name,
                    "table_name": table_name
                }
            ).scalar()

            # ------------------------  STEP 2: If table does not exist ------------------------ 
            if not table_exists:

                # Inserting in DB    
                df.to_sql(name=table_name,con=conn,schema=schema_name,if_exists="fail",index=False)

                print("----------------Table created successfully!------------------------")
                print(f"Data inserted for {filename}!")
                print("Inserted File records:", csv_count)

            # ------------------------ If table exists ------------------------
            else:

                # STEP 3: Check whether filename date exists
                date_query = text("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM public.amazon
                        WHERE use_date = :check_date
                    )
                """)

                date_exists = conn.execute(date_query,{"check_date": filename}).scalar()

                # print(date_query)
                print('Date exists : ', date_exists)

                # STEP 4: If date exists, compare counts
                if date_exists:

                    print(f"Data already exists for {filename}")

                    count_query = text("""
                        SELECT COUNT(*)
                        FROM public.amazon
                        WHERE use_date = :check_date
                    """)

                    db_count = conn.execute(count_query,{"check_date": filename}).scalar()

                    print("CSV record count:", csv_count)
                    print("Database record count:", db_count)

                    if csv_count == db_count:

                        print("CSV count and database count match!")
                        print("Skipping insertion.")

                    else:

                        raise Exception(
                            f"Record count mismatch for {filename}! "
                            f"CSV count: {csv_count}, "
                            f"Database count: {db_count}"
                        )

                # ------------------------ STEP 5: If date does not exist, insert ------------------------
                else:

                    df.to_sql(name=table_name,con=conn,schema=schema_name,if_exists="append",index=False)

                    print(f"No data exists for {filename}.")
                    print(f"Data inserted successfully for {filename}!")

                    # STEP 6: Verify inserted record count
                    db_count = conn.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM public.amazon
                            WHERE use_date = :check_date
                        """),
                        {"check_date": filename}
                    ).scalar()

                    print("CSV record count:", csv_count)
                    print("Database record count:", db_count)

                    if csv_count == db_count:

                        print("CSV count and database count match!")

                    else:

                        raise Exception(
                            f"Count mismatch after insertion! "
                            f"CSV: {csv_count}, Database: {db_count}"
                        )

    except Exception as e:

        print("Database operation failed!")
        print("Exception type:", type(e).__name__)
        print("Error:", str(e))

        raise

    finally:

        if engine is not None:
            engine.dispose()


# ==================================================================================================
# ======================================== CREATING THE DAG ======================================
# ==================================================================================================
with DAG(
    dag_id = 'date_wise_file_load',
    start_date = pendulum.datetime(2026, 11, 10, 14, 0, tz = 'Asia/Kolkata'),
    schedule = "@daily",
    catchup = True
) as dag:


    # ================ FILE SENSOR ====================================
    is_file_present = FileSensor(
        task_id = 'is_file_present',
        # filepath = '/opt/airflow/data/{{ ds }}.csv',
        filepath="{{ ds }}.csv",
        poke_interval = 60,
        timeout = 120,
        mode = 'reschedule'
    )

    # ============ NOT SURE ======================
    file_path = PythonOperator(
            task_id = 'file_path',
            python_callable = print_sensor_directory
        )


    # ============= MAKE TRANSFORMATIONS IN THE FILE =====================
    transformation = PythonOperator(
        task_id = 'transformation',
        python_callable = file_check_transform,
        op_kwargs={"filename": "{{ ds }}"}
        )

    # ============= POSTGRES ACTIVITY =====================
    connecting_postgres = PythonOperator(
        task_id = 'connecting_postgres',
        python_callable = func_connect_postgres,
        op_kwargs={"filename": "{{ ds }}"}
        )

    file_path>> is_file_present >> transformation >> connecting_postgres
