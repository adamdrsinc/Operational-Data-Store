import json
import pandas as pd
from ODS import ODS
from DateHelper import DateHelper

class ParseJSON:
    def __init__(self):
        print("Building ParseJSON via Constructor")
        with open("BuildJSON.json") as f:
            self.data = json.load(f)

    def parseJSON(self):
        print("Parsing JSON Files")
        self.parseDates()
        self.parseCustomers()
        self.parseStoreAddresses()
        self.parseProducts()
        self.parseCategories()
        self.parseOrders()

    def parseDates(self):
        print("\tParsing JSON Dates")
        sales_df = pd.json_normalize(data=self.data['Sales'])
        sales_df = sales_df.rename(columns={'Order Date': 'FullDate'})
        dates_df = pd.DataFrame(sales_df['FullDate'])

        dates_df = DateHelper().convertDateValues(df=dates_df, date_format="%d/%m/%Y")

        ODS.dimDate_df = pd.concat([dates_df, ODS.dimDate_df])
        ODS.dimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)

    def parseCustomers(self):
        print("\tParsing JSON Customers")

    def parseStoreAddresses(self):
        print("\tParsing JSON Store Addresses")

    def parseProducts(self):
        print("\tParsing JSON Products")

    def parseCategories(self):
        print("\tParsing JSON Categories")

    def parseOrders(self):
        print("\tParsing JSON Orders")
