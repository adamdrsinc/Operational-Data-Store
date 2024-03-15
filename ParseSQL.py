import pyodbc as pdbc
import pandas as pd
from ODS import ODS


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
        dates_df = pd.read_sql_query("SELECT DateOfSale FROM InternetSale", self.conn)
        dates_df['FullDate'] = pd.to_datetime(dates_df['DateOfSale'])
        dates_df['DateID'] = dates_df['FullDate'].dt.strftime('%Y%m%d')
        dates_df['Day'] = dates_df['FullDate'].dt.strftime('%A')
        dates_df['Month'] = dates_df['FullDate'].dt.strftime('%B')
        dates_df['Year'] = dates_df['FullDate'].dt.strftime('%Y')
        dates_df['DayOfWeek'] = dates_df['FullDate'].dt.strftime('%w')
        dates_df['DayOfYear'] = dates_df['FullDate'].dt.strftime('%j')
        dates_df['Quarter'] = dates_df['FullDate'].dt.quarter
        dates_df = dates_df.drop(columns=['DateOfSale'])
        ODS.dimDate_df = pd.concat([dates_df, ODS.dimDate_df])
        ODS.dimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)
        # print(ODS.dimDate_df.to_string())

    def parseStoreAddresses(self):
        print("\tParsing SQL Store Addresses")
        locations_df = pd.read_sql_query("SELECT * FROM Supplier", self.conn)
        locations_df['StoreID'] = locations_df['SupplierID']
        locations_df['Address'] = locations_df['SupplierAddress']
        locations_df['City'] = locations_df['SupplierCity']
        locations_df['StateProvince'] = locations_df['SupplierStateProvince']
        locations_df['Country'] = locations_df['SupplierCountry']
        locations_df['PostCode'] = locations_df['SupplierPostCode']
        locations_df = locations_df.drop(columns=['SupplierID', 'SupplierAddress', 'SupplierCity',
                                                  'SupplierStateProvince', 'SupplierCountry', 'SupplierPostCode',
                                                  'SupplierPhone'])
        ODS.dimStoreAddress_df = pd.concat([locations_df, ODS.dimStoreAddress_df])
        ODS.dimStoreAddress_df.drop_duplicates(subset='StoreID', keep='first', inplace=True)
        # print(ODS.dimStoreAddress_df.to_string())

    def parseCustomers(self):
        print("\tParsing SQL Customers")
        customer_df = pd.read_sql_query(
            "SELECT CustomerID, FirstName, SecondName as Surname, CustomerType FROM Customer",
            self.conn)
        ODS.dimCustomer_df = pd.concat([customer_df, ODS.dimCustomer_df])
        ODS.dimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        # print(ODS.dimCustomer_df.to_string())

    def parseProducts(self):
        print("\tParsing SQL Products")
        product_df = pd.read_sql_query(
            "SELECT ProductID, ProductDescription as ProductName, CategoryID as Category, SupplierPrice as Cost, ProductPrice, SupplierID as StoreID FROM Product",
            self.conn)
        ODS.dimProduct_df = pd.concat([product_df, ODS.dimProduct_df])
        ODS.dimProduct_df.drop_duplicates(subset='ProductID', keep='first', inplace=True)
        # print(ODS.dimProduct_df.to_string())

    def parseParentCategories(self):
        print("\tParsing SQL ParentCategories")
        pCategories_df = pd.read_sql_query("SELECT CategoryID, ParentCategory FROM Category", self.conn)
        ODS.dimParentCategory_df = pd.concat([pCategories_df, ODS.dimParentCategory_df])
        ODS.dimParentCategory_df.drop_duplicates(subset='CategoryID', keep='first', inplace=True)
        #print(ODS.dimParentCategory_df.to_string())

    def parseOrder(self):
        print("\tParsing SQL Orders")
        internet_sale_item = pd.read_sql_query("SELECT SaleID as OrderID, ProductID, Quantity FROM InternetSaleItem", self.conn)
        internet_sale = pd.read_sql_query("SELECT SaleID as OrderID, CustomerID, DateOfSale as DateID, SaleAmount FROM InternetSale", self.conn)
        internet_sale['DateID'] = (pd.to_datetime(internet_sale['DateID'])).dt.strftime('%Y%m%d')
        new_df = pd.merge(internet_sale_item, internet_sale, left_on='OrderID', right_on='OrderID', how='left')
        new_df = pd.merge(new_df, ODS.dimProduct_df[['ProductID', 'ProductPrice', 'StoreID']], left_on='ProductID', right_on='ProductID', how='left')
        ODS.factOrder_df = pd.concat([new_df, ODS.factOrder_df])
        #print(ODS.factOrder_df.head().to_string())
#wofjnw