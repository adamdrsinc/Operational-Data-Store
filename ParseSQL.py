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

        ODS.DimDate_df = pd.concat([ODS.DimDate_df, df])
        ODS.DimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)
        # print(ODS.DimDate_df.to_string())

    def parseStoreAddresses(self):
        print("\tParsing SQL Store Addresses")
        locations_df = pd.read_sql_query("SELECT PostalCode as PostCode, City, State, Country FROM Sale", self.conn)
        locations_df['AddressID'] = locations_df['PostCode']
        locations_df['City'] = locations_df['City']
        locations_df['StateProvince'] = locations_df['State']
        locations_df['Country'] = locations_df['Country']
        locations_df = locations_df.drop(columns=['PostCode', 'State'])
        ODS.DimStoreAddress_df = pd.concat([ODS.DimStoreAddress_df, locations_df])
        ODS.DimStoreAddress_df.drop_duplicates(subset='AddressID', keep='first', inplace=True)
        # print(ODS.DimStoreAddress_df.to_string())

    def parseCustomers(self):
        print("\tParsing SQL Customers")
        customer_df = pd.read_sql_query(
            "SELECT CustomerID, FirstName, Surname, CustomerType FROM Customer",
            self.conn)
        ODS.DimCustomer_df = pd.concat([ODS.DimCustomer_df, customer_df])
        ODS.DimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        # print(ODS.DimCustomer_df.to_string())

    def parseProducts(self):
        print("\tParsing SQL Products")
        product_df = pd.read_sql_query(
            "SELECT ProductID, ProductName, Category, Subcategory, Cost, ProductPrice FROM Product",
            self.conn)
        ODS.DimProduct_df = pd.concat([ODS.DimProduct_df, product_df])
        ODS.DimProduct_df.drop_duplicates(subset='ProductID', keep='first', inplace=True)
        # print(ODS.DimProduct_df.to_string())

    def parseParentCategories(self):
        print("\tParsing SQL ParentCategories")
        pCategories_df = pd.read_sql_query("SELECT CategoryName, ParentCategory FROM Category", self.conn)
        ODS.DimParentCategory_df = pd.concat([ODS.DimParentCategory_df, pCategories_df])
        ODS.DimParentCategory_df.drop_duplicates(subset='CategoryName', keep='first', inplace=True)
        # print(ODS.DimParentCategory_df.to_string())

    def parseOrder(self):
        print("\tParsing SQL Orders")
        sale_item = pd.read_sql_query("SELECT OrderID, ProductID, Quantity FROM SaleItem", self.conn)
        sale = pd.read_sql_query("SELECT OrderID, CustomerID, DateOfSale as DateID, SaleAmount FROM Sale",
                                 self.conn)
        products = pd.read_sql_query("SELECT ProductID, Cost, ProductPrice FROM Product", self.conn)

        sale['DateID'] = (pd.to_datetime(sale['DateID'])).dt.strftime('%Y%m%d')
        new_df = pd.merge(sale_item, sale, left_on='OrderID', right_on='OrderID', how='left')
        new_df = pd.merge(new_df, ODS.DimProduct_df[['ProductID', 'ProductPrice']], left_on='ProductID',
                          right_on='ProductID', how='left')
        new_df = pd.merge(new_df, products, on=['ProductID','ProductPrice'], how='left')

        ODS.FactOrder_df = pd.concat([ODS.FactOrder_df, new_df])
        ODS.FactOrder_df.drop_duplicates(subset='OrderID', keep='first', inplace=True)

