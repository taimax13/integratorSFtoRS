from csv import DictWriter
from datetime import date
from pathlib import Path

import psycopg2
from simple_salesforce import Salesforce, SalesforceMalformedRequest


def write_to_csv(sf, datapath, name):
    salesforceObject = sf.__getattr__(name)
    # so get a list of the object fields for this object.
    fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
    # then build a SOQL query against that object and do a query_all
    try:
        results = sf.query_all("SELECT " + ", ".join(fieldNames) + " FROM " + name)['records']
    except SalesforceMalformedRequest as e:
        # ignore objects with rules about getting queried.
        pass
    outputfile = datapath / (name + ".csv")
    with outputfile.open(mode='w', encoding='utf_8_sig') as csvfile:
        writer = DictWriter(csvfile, fieldnames=fieldNames)
        writer.writeheader()
        for row in results:
            # each row has a strange attributes key we don't want
            row.pop('attributes', None)
            print(row)
            writer.writerow(row)
    print("Done!" + csvfile.name)


def main():
    print("Start")
    sf = Salesforce(username='username', password="pass", security_token="token")
    description = sf.describe()
    names = [obj['name'] for obj in description['sobjects'] if obj['queryable']]
    print(sf.session)
    print(sf.sf_instance)
    # print(sf.describe())
    print(names)

    datapath = Path() / date.today().isoformat()
    print(datapath)
    try:
        datapath.mkdir(parents=True)
    except FileExistsError:
        pass
    print(datapath)
    for name in names:
        print(name)
        print("")
        #write_to_csv(sf, datapath, name)




# cur = conn.cursor()
# print(cur)
if __name__ == "__main__":
    main()
