import pandas as pd
from ODS import ODS
from DateHelper import DateHelper


class ParseCSV:
    def __init__(self):
        print("Building ParseCSV via Constructor")
        self.sales_df = pd.read_csv("BuildCSV.csv", encoding="ISO-8859-1")
        # print(self.sales_df.head().to_string())

    def parseCSV(self):
        print("Parsing CSV Files")
        self.parseDates()
        self.parseCustomers()
        self.parseStoreAddresses()
        self.parseProducts()
        self.parseCategories()
        self.parseOrders()

    def parseDates(self):
        print("\tParsing CSV Dates")
        self.sales_df = self.sales_df.rename(columns={"Order Date": "FullDate"})

        dates_df = pd.DataFrame(self.sales_df['FullDate'])
        dates_df = DateHelper().convertDateValues(df=dates_df, date_format="%d/%m/%Y")

        ODS.dimDate_df = pd.concat([dates_df, ODS.dimDate_df])
        ODS.dimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)

    def parseCustomers(self):
        print("\tParsing CSV Customers")
        self.sales_df = self.sales_df.rename(columns={"Customer ID": "CustomerID", "Segment": "CustomerType"})
        customer_df = pd.DataFrame({'CustomerID': self.sales_df['CustomerID'],
                                    'FirstName': self.sales_df['FirstName'],
                                    'Surname': self.sales_df['Surname'],
                                    'CustomerType': self.sales_df['CustomerType']})
        ODS.dimCustomer_df = pd.concat([customer_df, ODS.dimCustomer_df])
        ODS.dimCustomer_df.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        #print(ODS.dimCustomer_df.to_string())

    def parseStoreAddresses(self):
        print("\tParsing CSV Store Addresses")
        self.sales_df = self.sales_df.rename(columns={"Postal Code": "AddressID", "State": "StateProvince"})
        addresses_df = pd.DataFrame({'AddressID': self.sales_df['AddressID'],
                                     'StateProvince': self.sales_df['StateProvince'],
                                     'Country': self.sales_df['Country'],
                                     'Region': self.sales_df['Region'],
                                     'City': self.sales_df['City']})
        ODS.dimStoreAddress_df = pd.concat([addresses_df, ODS.dimStoreAddress_df])
        ODS.dimStoreAddress_df.drop_duplicates(subset='AddressID', keep='first', inplace=True)
        #print(ODS.dimStoreAddress_df.to_string())

    def parseProducts(self):
        print("\tParsing CSV Products")
        temp_df = self.sales_df.rename(columns={"Product ID": "ProductID",
                                                "Sub-Category": "Subcategory",
                                                "Product Name": "ProductName"
                                                })
        products_df = pd.DataFrame({'ProductID': temp_df['ProductID'],
                                    'Category': temp_df['Category'],
                                    'Subcategory': temp_df['Subcategory'],
                                    'ProductName': temp_df['ProductName']})

        ODS.dimProduct_df = pd.concat([products_df, ODS.dimProduct_df])
        ODS.dimProduct_df.drop_duplicates(subset='ProductID', keep='first', inplace=True)
        #print(ODS.dimProduct_df.to_string())

    def parseCategories(self):
        print("\tParsing CSV Categories")
        temp_df = self.sales_df.rename(columns={"Sub-Category": "Subcategory"})

        cat_df = pd.DataFrame({'CategoryName': temp_df['Subcategory'],
                               'ParentCategory': temp_df['Category']})

        ODS.dimParentCategory_df = pd.concat([cat_df, ODS.dimParentCategory_df])
        ODS.dimParentCategory_df.drop_duplicates(subset='CategoryName', keep='first', inplace=True)
        #print(ODS.dimParentCategory_df.to_string())

    def parseOrders(self):
        print("\tParsing CSV Orders")

        temp_df = self.sales_df.rename(columns={
            "Order ID": "OrderID",
            "Product ID": "ProductID",
            "Sales": "SaleAmount",
            "Customer ID": "CustomerID",
            "FullDate": "DateID"
        })

        temp_df['DateID'] = pd.to_datetime(temp_df['DateID'], format="%d/%m/%Y")
        temp_df['DateID'] = temp_df['DateID'].dt.strftime('%Y%m%d')

        orders_df = pd.DataFrame({
            "OrderID": temp_df["OrderID"],
            "ProductID": temp_df["ProductID"],
            "Quantity": temp_df["Quantity"],
            "SaleAmount": temp_df["SaleAmount"],
            "CustomerID": temp_df["CustomerID"],
            "DateID": temp_df["DateID"]
        })

        ODS.factOrder_df = pd.concat([orders_df, ODS.factOrder_df])
        ODS.factOrder_df.drop_duplicates(subset='OrderID', keep='first', inplace=True)
        #print(ODS.factOrder_df.to_string())




