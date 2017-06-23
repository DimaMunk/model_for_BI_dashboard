from CurrentDate import CurrentDate
from ConnectorToDB import ConnectorToDB
from dataFrameForModel import DataFrameForModel
from dataFrameForPolymBi import Filter
from dataFrameForPolymBi import DataFrame
from dataFrameCalculator import calculate

currentDate = CurrentDate()

dbName = 'polym_debit'
dbUser = 'postgres'
dbHost = '192.168.47.154'
dbPsswrd = '12345678'

connectorToDb = ConnectorToDB()
connectorToDb.cursorConnectionOpen(dbName, dbUser, dbHost, dbPsswrd)  # Cursor

if not 'myFilter' in locals():
    myFilter = Filter()
    myFilter.selected = [['1 квартал {0} года'.format(currentDate.currentYear)]]

#-Filter-
myFilter = Filter()
myFilter.values = [['1 квартал {0} года'.format(currentDate.currentYear),
                    '2 квартал {0} года'.format(currentDate.currentYear),
                    '3 квартал {0} года'.format(currentDate.currentYear),
                    '4 квартал {0} года'.format(currentDate.currentYear),
                    '1 полугодие {0} года'.format(currentDate.currentYear),
                    '2 полугодие {0} года'.format(currentDate.currentYear),
                    'С начала {0} года'.format(currentDate.currentYear)]]

myFilter.selected[0]=['1 квартал 2017 года']

DashboardDataFrame = DataFrameForModel()
DashboardDataFrame = calculate(DashboardDataFrame, myFilter, connectorToDb, currentDate)
connectorToDb.cursorConnectionClose()



#-DataFrame-
myDataFrame = DataFrame()
myDataFrame.cols = DashboardDataFrame.dataFrame.cols
myDataFrame.rows = DashboardDataFrame.dataFrame.rows
myDataFrame.values = DashboardDataFrame.dataFrame.values
########################### TESTS #######################
print(myDataFrame.values)
print(myDataFrame.cols)
print(myDataFrame.rows)
######################################################