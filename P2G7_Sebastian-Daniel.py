'''
=================================================
Graded Challenge 7

Nama  : Sebastian Daniel Parlindungan
Batch : FTDS-023-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Dataset yang dipakai adalah dataset mengenai fenomena tidak menghadiri konsultasi medis yang telah di booking.
=================================================
'''

import psycopg2
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def get_table(database, user, password, host, port, table_name):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        # Create a cursor object using the cursor() method
        cur = conn.cursor()

        # Execute a PostgreSQL query
        cur.execute(f"SELECT * FROM table_gc7")

        # Fetch all rows from the result set
        rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

        return df

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close communication with the PostgreSQL database
        if cur:
            cur.close()
        if conn:
            conn.close()


def clean_table(df):
    df.drop_duplicates(inplace= True)

    df.dropna(inplace=True)

    df.columns = df.columns.str.strip()

    df['patient_id'] = df['PatientId']
    df.drop(columns=['PatientId'], inplace=True)

    df['appointment_id'] = df['AppointmentID']
    df.drop(columns=['AppointmentID'], inplace=True)

    df['gender'] = df['Gender']
    df.drop(columns=['Gender'], inplace=True)

    df['scheduled_day'] = df['ScheduledDay']
    df.drop(columns=['ScheduledDay'], inplace=True)

    df['appointment_day'] = df['AppointmentDay']
    df.drop(columns=['AppointmentDay'], inplace=True)

    df['age'] = df['Age']
    df.drop(columns=['Age'], inplace=True)

    df['neighbourhood'] = df['Neighbourhood']
    df.drop(columns=['Neighbourhood'], inplace=True)

    df['scholarship'] = df['Scholarship']
    df.drop(columns=['Scholarship'], inplace=True)

    df['hipertension'] = df['Hipertension']
    df.drop(columns=['Hipertension'], inplace=True)

    df['diabetes'] = df['Diabetes']
    df.drop(columns=['Diabetes'], inplace=True)
    
    df['alcoholism'] = df['Alcoholism']
    df.drop(columns=['Alcoholism'], inplace=True)

    df['handicap'] = df['Handcap'].apply(lambda x: True if x==1 else False)
    df.drop(columns=['Handcap'], inplace=True)
    df['handicap'] = df['handicap'].astype(bool)

    df['sms_received'] = df['SMS_received']
    df.drop(columns=['SMS_received'], inplace=True)

    df['no_show'] = df['No-show']
    df.drop(columns=['No-show'], inplace=True)

    return df


def elastic_transfer():
    es = Elasticsearch("http://localhost:9200")
    df_clean = pd.read_csv('P2G7_Sebastian_Daniel_data_clean.csv')
    for i, r in df_clean.iterrows():
        doc = r.to_json()  
        res = es.index(index="gc7_test_2", doc_type="doc", body=doc)  
        print(res)


database = "gc7"
user = "postgres"
password = "Glance@u1"
host = "localhost"
port = "5432"
table_name = "table_gc7" 

data = get_table(database, user, password, host, port, table_name)

cleaned_table = clean_table(data)

cleaned_table.to_csv('P2G7_Sebastian_Daniel_data_clean.csv')

elastic_transfer()

