import pandas as pd


class DateHelper:
    def convertDateValues(self, df, date_format=""):
        if date_format == "":
            df['FullDate'] = pd.to_datetime(df['FullDate'])
        else:
            df['FullDate'] = pd.to_datetime(df['FullDate'], format=date_format)

        df['DateID'] = df['FullDate'].dt.strftime('%Y%m%d')
        df['Day'] = df['FullDate'].dt.strftime('%A')
        df['Month'] = df['FullDate'].dt.strftime('%B')
        df['Year'] = df['FullDate'].dt.strftime('%Y')
        df['DayOfWeek'] = df['FullDate'].dt.strftime('%w')
        df['DayOfYear'] = df['FullDate'].dt.strftime('%j')
        df['Quarter'] = df['FullDate'].dt.quarter
        return df
