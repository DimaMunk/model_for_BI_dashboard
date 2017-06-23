

def calculate(dataFrameToCalculate, myFilter, connectorToDb, currentDate):
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == '1квартал':
        dataFrameToCalculate.dataFrameForOneQuarter(connectorToDb,
                                             currentDate,
                                             currentYear=currentDate.currentYear,
                                             currentQuarter='1',
                                             prevYear=currentDate.prevYear)
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == '2квартал':
        dataFrameToCalculate.dataFrameForOneQuarter(connectorToDb,
                                             currentDate,
                                             currentYear=currentDate.currentYear,
                                             currentQuarter='2',
                                             prevYear=currentDate.prevYear)
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == '3квартал':
        dataFrameToCalculate.dataFrameForOneQuarter(connectorToDb,
                                             currentDate,
                                             currentYear=currentDate.currentYear,
                                             currentQuarter='3',
                                             prevYear=currentDate.prevYear)
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == '4квартал':
        dataFrameToCalculate.dataFrameForOneQuarter(connectorToDb,
                                             currentDate,
                                             currentYear=currentDate.currentYear,
                                             currentQuarter='4',
                                             prevYear=currentDate.prevYear)
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == '1полугодие':
        dataFrameToCalculate.dataFrameForFirstHalfYear(connectorToDb,
                                                currentDate,
                                                currentYear=currentDate.currentYear,
                                                prevYear=currentDate.prevYear)
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == '2полугодие':
        dataFrameToCalculate.dataFrameForSecondHalfYear(connectorToDb,
                                                 currentDate,
                                                 currentYear=currentDate.currentYear,
                                                 prevYear=currentDate.prevYear)
    if myFilter.selected[0][0].split()[0] + myFilter.selected[0][0].split()[1] == 'Сначала':
        dataFrameToCalculate.dataFrameSinceBegin(connectorToDb,
                                                 currentDate,
                                                 currentYear=currentDate.currentYear,
                                                 prevYear=currentDate.prevYear)


    return dataFrameToCalculate