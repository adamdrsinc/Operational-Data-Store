from ParseSQL import ParseSQL


class DataStoreMain:
    def __init__(self):
        sqlParser = ParseSQL()
        sqlParser.parseSQL()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    instance = DataStoreMain()
