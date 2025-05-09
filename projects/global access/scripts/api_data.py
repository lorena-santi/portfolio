import os
import random
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from ipaddress import ip_address, ip_network

print("Starting \n")

#list of bogon networks that should be avoided
bogon_networks = [
    ip_network("0.0.0.0/8"),
    ip_network("10.0.0.0/8"),
    ip_network("127.0.0.0/8"),
    ip_network("169.254.0.0/16"),
    ip_network("172.16.0.0/12"),
    ip_network("192.0.0.0/24"),
    ip_network("192.0.2.0/24"),
    ip_network("192.88.99.0/24"),
    ip_network("192.168.0.0/16"),
    ip_network("198.18.0.0/15"),
    ip_network("198.51.100.0/24"),
    ip_network("203.0.113.0/24"),
    ip_network("224.0.0.0/4"),   
    ip_network("240.0.0.0/4"),     
]

def is_bogon(ip):
    ip_obj = ip_address(ip)
    return any(ip_obj in net for net in bogon_networks)

#generate random public IP
def generate_public_ip():
    while True:
        ip = ".".join(str(random.randint(1, 255)) for _ in range(4))
        try:
            ip_obj = ip_address(ip)
            if (
                ip_obj.is_global and not is_bogon(ip)
            ):
                return ip
        except ValueError:
            continue

#api credentials
load_dotenv()
API_KEY = os.getenv("API_KEY")
     
#searching for information on the api
def check_ip(ip):
    url = "https://api.ipgeolocation.io/ipgeo"
    params = {"apiKey": API_KEY, "ip": ip}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            "ip": ip,
            "zipcode": data.get("zipcode"),
            "city": data.get("city"),
            "state": data.get("state_prov"),
            "country": data.get("country_name"),
            "country_code": data.get("country_code3"),
            "continent_name": data.get("continent_name"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "timezone": data.get("time_zone", {}).get("name"),
            "timezone_offset": data.get("time_zone", {}).get("offset"),
            "timezone_offset_with_dst": data.get("time_zone", {}).get("offset_with_dst"),
            "dst_exists": data.get("time_zone", {}).get("dst_exists"),
            "is_dst": data.get("time_zone", {}).get("is_dst"),
            "current_time": data.get("time_zone", {}).get("current_time"),
            "organization": data.get("organization"),
            "internet_service_provider": data.get("isp"),
            "connection_type": data.get("connection_type")
        } 
    else:
        return {
            "ip": ip,
            "error": f"{response.status_code} - {response.text}"
        }

#main function
def main(amount=50):
    results = []
    print("Querying data in the API \n")
    for _ in range(amount):
        ip = generate_public_ip()
        info = check_ip(ip)
        results.append(info)

    df = pd.DataFrame(results)
    return df 

#running the function
if __name__ == "__main__":
    ip_data = main()  

#list of numeric columns in the dataframe
numeric_columns = ['latitude', 'longitude', 'timezone_offset', 'timezone_offset_with_dst']

#convert columns to numeric
for col in numeric_columns:
    ip_data[col] = pd.to_numeric(ip_data[col], errors='coerce')
    
print('Uploading table to BigQuery \n')

#setting up bigquery
os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = bigquery.Client()
project_id = 'portfolio-408419'
dataset = 'network'
table_id = f"{project_id}.{dataset}.ip_data"

#function to create the BigQuery schema
def create_schema():
    return [
        bigquery.SchemaField("ip", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("zipcode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("continent_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("timezone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("timezone_offset", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("timezone_offset_with_dst", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("dst_exists", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_dst", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("current_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("organization", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("internet_service_provider", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("connection_type", "STRING", mode="NULLABLE"),
    ]

#function to load the dataframe into BigQuery
def load_bigquery(df, table_id, schema):
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() 

#uploading data to the table
try:
    if not client.get_table(table_id, retry=bigquery.DEFAULT_RETRY):
        schema = create_schema()
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = client.load_table_from_dataframe(ip_data, table_id, job_config=job_config)
    job.result()

except Exception as e:
    print(f"Error loading table: {e}")

print('Completed')
