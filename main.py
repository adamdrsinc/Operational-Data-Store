import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

from ParseSQL import ParseSQL
from ParseCSV import ParseCSV


class DataStoreMain:
    def __init__(self):
        sqlParser = ParseSQL()
        sqlParser.parseSQL()
        csvParser = ParseCSV()
        csvParser.parseCSV()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    instance = DataStoreMain()
