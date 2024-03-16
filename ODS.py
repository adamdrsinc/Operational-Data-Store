import pandas as pd


class ODS:
    tables = [
        'DimCustomer',
        'DimStoreAddress',
        'DimDate',
        'DimProduct',
        'DimParentCategory',
        'FactOrder'
    ]

    dimCustomer_df = pd.DataFrame(columns=[
        'CustomerID',
        'FirstName',
        'Surname',
        'CustomerType'
    ])
    dimStoreAddress_df = pd.DataFrame(columns=[
        'AddressID',
        'City',
        'StateProvince',
        'Region',
        'Country',
    ])
    dimDate_df = pd.DataFrame(columns=[
        'FullDate',
        'DateID',
        'Day',
        'Month',
        'Year',
        'DayOfYear',
        'DayOfWeek',
        'Quarter'
    ])
    dimProduct_df = pd.DataFrame(columns=[
        'ProductID',
        'ProductName',
        'Category',
        'Subcategory',
        'Cost',
        'ProductPrice',
    ])
    dimParentCategory_df = pd.DataFrame(columns=[
        'CategoryName',
        'ParentCategory'
    ])
    factOrder_df = pd.DataFrame(columns=[
        'OrderID',
        'ProductID',
        'Quantity',
        'ProductPrice',
        'SaleAmount',
        'CustomerID',
        'DateID',
    ])
