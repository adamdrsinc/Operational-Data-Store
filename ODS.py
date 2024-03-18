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

    DimCustomer_df = pd.DataFrame(columns=[
        'CustomerID',
        'FirstName',
        'Surname',
        'CustomerType'
    ])
    DimStoreAddress_df = pd.DataFrame(columns=[
        'AddressID',
        'City',
        'StateProvince',
        'Country',
    ])
    DimDate_df = pd.DataFrame(columns=[
        'DateID',
        'FullDate',
        'Day',
        'Month',
        'Year',
        'DayOfYear',
        'DayOfWeek',
        'Quarter'
    ])
    DimProduct_df = pd.DataFrame(columns=[
        'ProductID',
        'ProductName',
        'Category',
        'Subcategory',
        'Cost',
        'ProductPrice',
    ])
    DimParentCategory_df = pd.DataFrame(columns=[
        'CategoryName',
        'ParentCategory'
    ])
    FactOrder_df = pd.DataFrame(columns=[
        'OrderID',
        'ProductID',
        'Quantity',
        'Cost',
        'ProductPrice',
        'SaleAmount',
        'CustomerID',
        'DateID'
    ])
