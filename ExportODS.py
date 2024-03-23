import pyodbc as pdbc
from ODS import ODS
import pandas as pd


class ExportODS:
    build_schema = ''' 
    DROP TABLE IF EXISTS FactOrder
DROP TABLE IF EXISTS DimCustomer
DROP TABLE IF EXISTS DimStoreAddress
DROP TABLE IF EXISTS DimDate 
DROP TABLE IF EXISTS DimProduct
DROP TABLE IF EXISTS DimParentCategory

CREATE TABLE DimCustomer(
	CustomerID nvarchar(50) PRIMARY KEY,
	FirstName nvarchar(50),
	Surname nvarchar(50),
	CustomerType nvarchar(50)
);

CREATE TABLE DimStoreAddress(
	AddressID nvarchar(20) PRIMARY KEY,
	City nvarchar(50),
	StateProvince nvarchar(50),
	Country nvarchar(80)
);

CREATE TABLE DimDate(
	DateID nvarchar(20) PRIMARY KEY,
	FullDate nvarchar(50),
	Day nvarchar(10),
	Month nvarchar(20),
	Year nvarchar(5),
	DayOfYear nvarchar(5),
	DayOfWeek nvarchar(10),
	Quarter nvarchar(10)
);

CREATE TABLE DimProduct(
	ProductID nvarchar(50) PRIMARY KEY,
	ProductName nvarchar(MAX),
	Category nvarchar(100),
	Subcategory nvarchar(100),
	Cost nvarchar(10),
	ProductPrice nvarchar(10)
);

CREATE TABLE DimParentCategory(
	CategoryName nvarchar(100) PRIMARY KEY,
	ParentCategory nvarchar(100)
);

CREATE TABLE FactOrder(
	OrderID nvarchar(50) PRIMARY KEY,
	ProductID nvarchar(50),
	Quantity nvarchar(10),
	Cost nvarchar(10),
	ProductPrice nvarchar(10),
	SaleAmount nvarchar(20),
	CustomerID nvarchar(50),
	DateID nvarchar(20),

	FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
	FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
	FOREIGN KEY (DateID) REFERENCES DimDate(DateID)
);


    '''

    def __init__(self):
        print("Building SQL Data Warehouse from Schema")
        connectionString = ("DRIVER={SQL Server};"
                            "SERVER=mssql.chester.network;"
                            "DATABASE=db_2209107_da_data_warehouse;"
                            "UID=user_db_2209107_da_data_warehouse;"
                            "PWD=P@55w0rd")
        self.conn = pdbc.connect(connectionString)
        self.cursor = self.conn.cursor()

    def buildTables(self):
        print("\tBuilding Tables from ODS into SQL Warehouse")
        self.cursor.execute(self.build_schema)
        self.conn.commit()
        print("\tFinished building tables")

    def exportUsingCSV(self, table, dataframe, chunksize):
        rows = len(dataframe.index)
        current = 0
        while current < rows:
            if rows - current < chunksize:
                stop = rows
            else:
                stop = current + chunksize

            CSV = dataframe.iloc[current:stop].to_csv(index=False, header=False, quoting=1, quotechar="'",
                                                      lineterminator="),\n(")
            CSV = CSV[:-3]
            values = f"({CSV}"
            SQL = f"\t \t INSERT INTO {table} VALUES {values}".replace("''", 'NULL')
            current = stop
            print(SQL)
            self.cursor.execute(SQL)

        self.conn.commit()

    def cleanODSTables(self):

        ODS.DimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        ODS.DimStoreAddress_df.drop_duplicates(subset='AddressID', keep='first', inplace=True)
        ODS.DimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)
        ODS.DimProduct_df.drop_duplicates(subset='ProductID', keep='first', inplace=True)
        ODS.DimParentCategory_df.drop_duplicates(subset='CategoryName', keep='first', inplace=True)
        ODS.FactOrder_df.drop_duplicates(subset='OrderID', keep='first', inplace=True)

        ODS.FactOrder_df = ODS.FactOrder_df[ODS.FactOrder_df['OrderID'].isnull() == False]

        ODS.DimCustomer_df = ODS.DimCustomer_df[['CustomerID', 'FirstName', 'Surname', 'CustomerType']]
        ODS.DimStoreAddress_df = ODS.DimStoreAddress_df[['AddressID', 'City', 'StateProvince', 'Country']]
        ODS.DimDate_df = ODS.DimDate_df[['DateID', 'FullDate', 'Day', 'Month', 'Year', 'DayOfYear', 'DayOfWeek', 'Quarter']]
        ODS.DimProduct_df = ODS.DimProduct_df[['ProductID', 'ProductName', 'Category', 'Subcategory', 'Cost', 'ProductPrice']]
        ODS.DimParentCategory_df = ODS.DimParentCategory_df[['CategoryName', 'ParentCategory']]
        ODS.FactOrder_df = ODS.FactOrder_df[['OrderID', 'ProductID', 'Quantity', 'Cost', 'ProductPrice',
                                             'SaleAmount', 'CustomerID', 'DateID']]

    def exportODS(self):
        print("Exporting ODS to SQL")
        chunksize = 999

        for table in ODS.tables:
            df = getattr(ODS, f"{table}_df")
            print(f"\tExporting to {table} from {table}_df with {len(df.index)} rows")
            self.exportUsingCSV(table=table, dataframe=df, chunksize=chunksize)
