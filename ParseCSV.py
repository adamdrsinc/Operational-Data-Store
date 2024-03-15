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

    def parseDates(self):
        print("\tParsing CSV Dates")
        self.sales_df = self.sales_df.rename(columns={"Order Date": "FullDate"})

        dates_df = pd.DataFrame(self.sales_df['FullDate'])
        dates_df = DateHelper().convertDateValues(df=dates_df, date_format="%d/%m/%Y")

        ODS.dimDate_df = pd.concat([dates_df, ODS.dimDate_df])
        ODS.dimDate_df.drop_duplicates(subset='DateID', keep='first', inplace=True)
