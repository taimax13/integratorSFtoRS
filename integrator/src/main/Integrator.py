import psycopg2
from simple_salesforce import Salesforce
import boto3

from integrator.src.main.csvLoader import CsvLoader


class Integrator(object):
    def __init__(self, csv_loader):
        self.csv_loader = csv_loader

    ### read credentials from config
    ### Integration with salesforce
    sf = Salesforce(username='uname', password=123, organizationId="um6.salesforce.com")

    s3_resource = boto3.resource('s3')
    s3_resource.Object('bucket_name', 'file_name').upload_file(Filename='filename')

    client = boto3.client('redshift')

    conn = psycopg2.connect(
        host='########', ###'mydb.mydatabase.eu-west-2.redshift.amazonaws.com',
        user='########',
        port=5439,
        password='########',
        dbname='########')

    cur = conn.cursor()

    cur.execute(CsvLoader().read_csv())
    conn.commit()

    sql = """copy stack_overflow_survey_data from 's3://an-example-bucket/survey_data.csv'
        access_key_id '<access_key_id>'
        secret_access_key '<secret_access_key>'
        region 'us-west-1'
        ignoreheader 1
        null as 'NA'
        removequotes
        delimiter ',';"""
    cur.execute(sql)
    conn.commit()

