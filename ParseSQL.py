import pyodbc as pdbc
import pandas as pd
from ODS import ODS
from DateHelper import DateHelper


class ParseSQL:
    def __init__(self):
        print("Building ParseSQL via Constructor")
        connectionString = ("DRIVER={SQL Server};"
                            "SERVER=mssql.chester.network;"
                            "DATABASE=db_2209107_operational_data_store;"
                            "UID=user_db_2209107_operational_data_store;"
                            "PWD=P@55w0rd")
        self.conn = pdbc.connect(connectionString)
        self.cursor = self.conn.cursor()

    def parseSQL(self):
        print("Parsing SQL")
        self.parseDates()
        self.parseStoreAddresses()
        self.parseCustomers()
        self.parseProducts()
        self.parseParentCategories()
        self.parseOrder()

    def parseDates(self):
        print("\tParsing SQL Dates")
        dates_df = pd.read_sql_query("SELECT DateOfSale as FullDate FROM Sale", self.conn)
        # Need to convert this to a function
        df = DateHelper().convertDateValues(df=dates_df)

        ODS.dimDate_df = pd.concat([df, ODS.dimDate_df])
        ODS.dimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)
        # print(ODS.dimDate_df.to_string())

    def parseStoreAddresses(self):
        print("\tParsing SQL Store Addresses")
        locations_df = pd.read_sql_query("SELECT PostalCode as PostCode, City, State, Country FROM Sale", self.conn)
        locations_df['AddressID'] = locations_df['PostCode']
        locations_df['City'] = locations_df['City']
        locations_df['StateProvince'] = locations_df['State']
        locations_df['Country'] = locations_df['Country']
        locations_df = locations_df.drop(columns=['PostCode'])
        ODS.dimStoreAddress_df = pd.concat([locations_df, ODS.dimStoreAddress_df])
        ODS.dimStoreAddress_df.drop_duplicates(subset='AddressID', keep='first', inplace=True)
        # print(ODS.dimStoreAddress_df.to_string())

    def parseCustomers(self):
        print("\tParsing SQL Customers")
        customer_df = pd.read_sql_query(
            "SELECT CustomerID, FirstName, Surname, CustomerType FROM Customer",
            self.conn)
        ODS.dimCustomer_df = pd.concat([customer_df, ODS.dimCustomer_df])
        ODS.dimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        # print(ODS.dimCustomer_df.to_string())

    def parseProducts(self):
        print("\tParsing SQL Products")
        product_df = pd.read_sql_query(
            "SELECT ProductID, ProductName, Category, Subcategory, Cost, ProductPrice FROM Product",
            self.conn)
        ODS.dimProduct_df = pd.concat([product_df, ODS.dimProduct_df])
        ODS.dimProduct_df.drop_duplicates(subset='ProductID', keep='first', inplace=True)
        # print(ODS.dimProduct_df.to_string())

    def parseParentCategories(self):
        print("\tParsing SQL ParentCategories")
        pCategories_df = pd.read_sql_query("SELECT CategoryName, ParentCategory FROM Category", self.conn)
        ODS.dimParentCategory_df = pd.concat([pCategories_df, ODS.dimParentCategory_df])
        ODS.dimParentCategory_df.drop_duplicates(subset='CategoryName', keep='first', inplace=True)
        # print(ODS.dimParentCategory_df.to_string())

    def parseOrder(self):
        print("\tParsing SQL Orders")
        sale_item = pd.read_sql_query("SELECT OrderID, ProductID, Quantity FROM SaleItem", self.conn)
        sale = pd.read_sql_query("SELECT OrderID, CustomerID, DateOfSale as DateID, SaleAmount FROM Sale",
                                 self.conn)
        sale['DateID'] = (pd.to_datetime(sale['DateID'])).dt.strftime('%Y%m%d')
        new_df = pd.merge(sale_item, sale, left_on='OrderID', right_on='OrderID', how='left')
        new_df = pd.merge(new_df, ODS.dimProduct_df[['ProductID', 'ProductPrice']], left_on='ProductID',
                          right_on='ProductID', how='left')
        ODS.factOrder_df = pd.concat([new_df, ODS.factOrder_df])
        # print(ODS.factOrder_df.head().to_string())
