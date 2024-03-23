import json
import pandas as pd
from ODS import ODS
from DateHelper import DateHelper


class ParseJSON:
    def __init__(self):
        self.addresses_df = None
        self.customer_df = None
        self.dates_df = None
        self.products_df = None
        print("Building ParseJSON via Constructor")
        with open("BuildJSON.json") as f:
            self.data = json.load(f)

    def parseJSON(self):
        print("Parsing JSON Files")
        self.parseDates()
        self.parseCustomers()
        self.parseStoreAddresses()
        self.parseProducts()
        self.parseOrders()

    def parseDates(self):
        print("\tParsing JSON Dates")
        sales_df = pd.json_normalize(data=self.data['Sales'])
        sales_df = sales_df.rename(columns={'Order Date': 'FullDate'})
        dates_df = pd.DataFrame(sales_df['FullDate'])

        self.dates_df = DateHelper().convertDateValues(df=dates_df, date_format="%d/%m/%Y")

        ODS.DimDate_df = pd.concat([ODS.DimDate_df, self.dates_df])
        ODS.DimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)

    def parseCustomers(self):
        print("\tParsing JSON Customers")
        sales_df = pd.json_normalize(data=self.data['Sales'])
        customer_df = pd.DataFrame(sales_df['Customer ID'])
        customer_df = customer_df.rename(columns={"Customer ID": "CustomerID"})
        self.customer_df = customer_df

        ODS.DimCustomer_df = pd.concat([ODS.DimCustomer_df, self.customer_df])
        ODS.DimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        # print(ODS.DimCustomer_df.to_string())

    def parseStoreAddresses(self):
        print("\tParsing JSON Store Addresses")
        sales_df = pd.json_normalize(data=self.data['Sales'])
        sales_df = sales_df.rename(columns={"Postal Code": "AddressID"})
        self.addresses_df = pd.DataFrame({
            "AddressID": sales_df['AddressID'],
            "City": sales_df['City'],
            "StateProvince": sales_df['State'],
            "Country": sales_df['Country']
        })

        ODS.DimStoreAddress_df = pd.concat([ODS.DimStoreAddress_df, self.addresses_df])
        ODS.DimStoreAddress_df.drop_duplicates(subset='AddressID', keep='first', inplace=True)
        # print(ODS.DimStoreAddress_df.to_string())

    def parseProducts(self):
        print("\tParsing JSON Products")
        sales_df = pd.DataFrame(pd.json_normalize(data=self.data['Sales']))
        exploded_df = sales_df.explode('Items')
        items_df = pd.json_normalize(exploded_df['Items'])
        items_df = items_df.rename(columns={"Product ID": "ProductID",
                                            "Sales": "SaleAmount"})

        self.products_df = pd.DataFrame({
            "ProductID": items_df["ProductID"],
        })

        new_df = pd.merge(items_df, ODS.DimProduct_df[['Cost', 'ProductPrice', 'ProductID']], left_on='ProductID',
                          right_on='ProductID', how='left')

        ODS.DimProduct_df = pd.concat([ODS.DimProduct_df, new_df])
        ODS.DimProduct_df.drop_duplicates(subset='ProductID', keep='first', inplace=True)
        # print(ODS.DimProduct_df.to_string())

    def parseOrders(self):
        print("\tParsing JSON Orders")
        sales_df = pd.DataFrame(pd.json_normalize(data=self.data['Sales']))
        exploded_df = sales_df.explode('Items')
        items_df = pd.json_normalize(exploded_df['Items'])

        orders_df = pd.DataFrame({
            "OrderID": sales_df["Order ID"],
            "ProductID": self.products_df["ProductID"],
            "Quantity": items_df["Quantity"],
            "SaleAmount": items_df["Sales"],
            "CustomerID": self.customer_df["CustomerID"],
            "DateID": self.dates_df["DateID"]
        })

        new_df = pd.merge(orders_df, ODS.DimProduct_df[['ProductID', 'Cost', 'ProductPrice']], how='left')

        ODS.FactOrder_df = pd.concat([ODS.FactOrder_df, new_df])
        ODS.FactOrder_df.drop_duplicates(subset='OrderID', keep='first', inplace=True)

        print(ODS.FactOrder_df.to_string())
