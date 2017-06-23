from QueryExecutor import QueryExecutor
from dataFrameForPolymBi import DataFrame
from itertools import zip_longest


class DataFrameForModel:
    # Класс формирует дата фрейм
    # currentDate не formated!!!

    def __init__(self):
        # -DataFrame-
        self.dataFrame = DataFrame()
        self.dataFrame.cols = [['План'], ['Факт'], ['Факт за аналогичный период предыдущего года']]
        self.dataFrame.values = [[], [], []]
        self.dataFrame.rows = []

    def dataFrameForOneQuarter(self,
                 connectorToDb,
                 currentDate,
                 currentQuarter,
                 factFromExcel ='fact_debit_usd',
                 tableFactFromExcel = 'f_department_data_excel',
                 firstFactFrom1C ='cost_usd',
                 secondFactFrom1C = 'prime_cost_usd',
                 tableFactFrom1C = 'f_realization_1c',
                 currentYear = '2016',
                 prevYear = '2016',
                 planFromExcel = 'plan_cost_usd',
                 tablePlanFromExcel = True):

        if tablePlanFromExcel == True: tablePlanFromExcel = tableFactFromExcel


        # Записываются департаменты из базы в ДФ
        query = QueryExecutor(connectorToDb.connectiontodb)
        departmentsForDataFrame = query.factSumExcel(factFromExcel,
                                                     tableFactFromExcel,
                                                     currentQuarter,
                                                     currentYear)
        for element in departmentsForDataFrame.iterrows():
            self.dataFrame.rows.append(
                [element[1]['department_name']])  # записываются строки из базы в департаменты в ДФ

        # Проверка на дату, текущий квартал равен ли текущей дате

        if str(currentDate.currentQuarter) == currentQuarter:
        # Вычисление при условии то что не за весь квартал а за только часть его

            # тут сначала ищется факт в экселе
            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFromExcel = query.factSumExcelIfCurrDateInSelQuoter(factFromExcel,
                                                                  tableFactFromExcel,
                                                                  currentQuarter,
                                                                  currentYear,
                                                                  currentDate.currentdateformated)
            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol1CIfCurDateInSelQuoter(firstFactFrom1C,
                                                                                     tableFactFrom1C,
                                                                                     currentQuarter,
                                                                                     currentYear,
                                                                                     secondFactFrom1C,
                                                                                     currentDate.currentdateformated)
            array1 = []
            array2 = []


            for element in factForCurrentPeriodUsdFromExcel.iterrows():
                array1.append(element[1]['diff'])
            for element in factForCurrentPeriodUsdFrom1C.iterrows():
                array2.append(element[1]['diff'])

            zippedArrs = zip_longest(array1,array2)
            print(array2)
            for i in zippedArrs:
                if i[0] == 0 or i[0] is None:
                    self.dataFrame.values[1].append(i[1])
                else:
                    self.dataFrame.values[1].append(i[0])

            # Теперь ищется план в экселе

            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFromExcel = query.factSumExcelIfCurrDateInSelQuoter(planFromExcel,
                                                                  tablePlanFromExcel,
                                                                  currentQuarter,
                                                                  currentYear,
                                                                  currentDate.currentdateformated)

            for element in factForCurrentPeriodUsdFromExcel.iterrows():
                # тут записывается в дата фрейм план из экселя
                self.dataFrame.values[0].append(element[1]['diff'])

            # Факт за предыдущий год

            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFromExcel = query.factSumExcelIfCurrDateInSelQuoter(factFromExcel,
                                                                  tableFactFromExcel,
                                                                  currentQuarter,
                                                                  prevYear,
                                                                  currentDate.currentdateformated)
            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol1CIfCurDateInSelQuoter(firstFactFrom1C,
                                                                                     tableFactFrom1C,
                                                                                     currentQuarter,
                                                                                     prevYear,
                                                                                     secondFactFrom1C,
                                                                                     currentDate.currentdateformated)

            array1 = []
            array2 = []

            for element in factForCurrentPeriodUsdFromExcel.iterrows():
                array1.append(element[1]['diff'])
            for element in factForCurrentPeriodUsdFrom1C.iterrows():
                array2.append(element[1]['diff'])
            zippedArrs = zip_longest(array1, array2)

            for i in zippedArrs:
                if i[0] == 0 or i[0] is None:
                    self.dataFrame.values[2].append(i[1])
                else:
                    self.dataFrame.values[2].append(i[0])



        ######################################
        else:


            # тут сначала ищется факт в экселе
            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFromExcel = query.factSumExcel(factFromExcel,
                                                                  tableFactFromExcel,
                                                                  currentQuarter,
                                                                  currentYear)
            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol1C(firstFactFrom1C,
                                                                 tableFactFrom1C,
                                                                 currentQuarter,
                                                                 currentYear,
                                                                 secondFactFrom1C)

            array1 = []
            array2 = []

            for element in factForCurrentPeriodUsdFromExcel.iterrows():
                array1.append(element[1]['diff'])
            for element in factForCurrentPeriodUsdFrom1C.iterrows():
                array2.append(element[1]['diff'])
            zippedArrs = zip_longest(array1, array2)

            for i in zippedArrs:
                if i[0] == 0 or i[0] is None:
                    self.dataFrame.values[1].append(i[1])
                else:
                    self.dataFrame.values[1].append(i[0])



            # Теперь ищется план в экселе

            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFromExcel = query.factSumExcel(planFromExcel,
                                                                  tablePlanFromExcel,
                                                                  currentQuarter,
                                                                  currentYear)

            for element in factForCurrentPeriodUsdFromExcel.iterrows():
                # тут записывается в дата фрейм план из экселя
                self.dataFrame.values[0].append(element[1]['diff'])

            # Факт за предыдущий год

            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFromExcel = query.factSumExcel(factFromExcel,
                                                                  tableFactFromExcel,
                                                                  currentQuarter,
                                                                  prevYear)

            query = QueryExecutor(connectorToDb.connectiontodb)
            factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol1C(firstFactFrom1C,
                                                                 tableFactFrom1C,
                                                                 currentQuarter,
                                                                 prevYear,
                                                                 secondFactFrom1C)

            array1 = []
            array2 = []

            for element in factForCurrentPeriodUsdFromExcel.iterrows():
                array1.append(element[1]['diff'])
            for element in factForCurrentPeriodUsdFrom1C.iterrows():
                array2.append(element[1]['diff'])
            zippedArrs = zip_longest(array1, array2)

            for i in zippedArrs:
                if i[0] == 0 or i[0] is None:
                    self.dataFrame.values[2].append(i[1])
                else:
                    self.dataFrame.values[2].append(i[0])



    def dataFrameForFirstHalfYear(self,
                                connectorToDb,
                                currentDate,
                                factFromExcel ='plan_debit_usd',
                                tableFactFromExcel = 'f_department_data_excel',
                                currentYear = '2016',
                                  firstFactFrom1C ='cost_usd',
                                  tableFactFrom1C = 'f_realization_1c',
                                  secondFactFrom1C = 'prime_cost_usd',
                                  planFromExcel = 'plan_cost_usd',
                                  prevYear = '2016',
                                  tablePlanFromExcel = True,
                                  ):

        if tablePlanFromExcel == True: tablePlanFromExcel = tableFactFromExcel

        # Записываются департаменты из базы в ДФ
        query = QueryExecutor(connectorToDb.connectiontodb)
        departmentsForDataFrame = query.factSumExcel1HalfYear(factFromExcel,
                                                             tableFactFromExcel,
                                                             currentYear,
                                                             currentDate.currentdateformated
                                                              )
        for element in departmentsForDataFrame.iterrows():
            self.dataFrame.rows.append(
                [element[1]['department_name']])  # записываются строки из базы в департаменты в ДФ




        # тут сначала ищется факт в экселе
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.factSumExcel1HalfYear(factFromExcel,
                                                                       tableFactFromExcel,
                                                                       currentYear,
                                                                       currentDate.currentdateformated)
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol1HalfYear(firstFactFrom1C,
                                                                    tableFactFrom1C,
                                                                    currentYear,
                                                                    secondFactFrom1C,
                                                                    currentDate.currentdateformated)

        array1 = []
        array2 = []

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            array1.append(element[1]['diff'])
        for element in factForCurrentPeriodUsdFrom1C.iterrows():
            array2.append(element[1]['diff'])
        zippedArrs = zip_longest(array1, array2)

        for i in zippedArrs:
            if i[0] == 0 or i[0] is None:
                self.dataFrame.values[1].append(i[1])
            else:
                self.dataFrame.values[1].append(i[0])




        # Теперь ищется план в экселе

        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.factSumExcel1HalfYear(planFromExcel,
                                                                                   tablePlanFromExcel,

                                                                                   currentYear,
                                                                                   currentDate.currentdateformated)

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            # тут записывается в дата фрейм план из экселя
            self.dataFrame.values[0].append(element[1]['diff'])

        # Факт за предыдущий год

        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.factSumExcel1HalfYear(factFromExcel,
                                                                                   tableFactFromExcel,

                                                                                   prevYear,
                                                                                   currentDate.currentdateformated)
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol1HalfYear(firstFactFrom1C,
                                                                    tableFactFrom1C,

                                                                    prevYear,
                                                                    secondFactFrom1C,
                                                                    currentDate.currentdateformated)

        array1 = []
        array2 = []

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            array1.append(element[1]['diff'])
        for element in factForCurrentPeriodUsdFrom1C.iterrows():
            array2.append(element[1]['diff'])
        zippedArrs = zip_longest(array1, array2)

        for i in zippedArrs:
            if i[0] == 0 or i[0] is None:
                self.dataFrame.values[2].append(i[1])
            else:
                self.dataFrame.values[2].append(i[0])





##############################################################

    def dataFrameForSecondHalfYear(self,
                                  connectorToDb,
                                  currentDate,
                                  factFromExcel='plan_debit_usd',
                                  tableFactFromExcel='f_department_data_excel',
                                  currentYear='2016',
                                  firstFactFrom1C='cost_usd',
                                  tableFactFrom1C='f_realization_1c',
                                  secondFactFrom1C='prime_cost_usd',
                                  planFromExcel='plan_cost_usd',
                                  prevYear='2016',
                                  tablePlanFromExcel=True,
                                  ):


        if tablePlanFromExcel == True: tablePlanFromExcel = tableFactFromExcel

        # Записываются департаменты из базы в ДФ
        query = QueryExecutor(connectorToDb.connectiontodb)
        departmentsForDataFrame = query.factSumExcel2HalfYear(factFromExcel,
                                                              tableFactFromExcel,
                                                              currentYear,
                                                              currentDate.currentdateformated
                                                              )
        for element in departmentsForDataFrame.iterrows():
            self.dataFrame.rows.append(
                [element[1]['department_name']])  # записываются строки из базы в департаменты в ДФ

        # тут сначала ищется факт в экселе
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.factSumExcel2HalfYear(factFromExcel,
                                                                       tableFactFromExcel,
                                                                       currentYear,
                                                                       currentDate.currentdateformated)
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol2HalfYear(firstFactFrom1C,
                                                                    tableFactFrom1C,
                                                                    currentYear,
                                                                    secondFactFrom1C,
                                                                    currentDate.currentdateformated)

        array1 = []
        array2 = []

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            array1.append(element[1]['diff'])
        for element in factForCurrentPeriodUsdFrom1C.iterrows():
            array2.append(element[1]['diff'])
        zippedArrs = zip_longest(array1, array2)

        for i in zippedArrs:
            if i[0] == 0 or i[0] is None:
                self.dataFrame.values[1].append(i[1])
            else:
                self.dataFrame.values[1].append(i[0])





        # Теперь ищется план в экселе

        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.factSumExcel2HalfYear(planFromExcel,
                                                                       tablePlanFromExcel,

                                                                       currentYear,
                                                                       currentDate.currentdateformated)

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            # тут записывается в дата фрейм план из экселя
            self.dataFrame.values[0].append(element[1]['diff'])

        # Факт за предыдущий год

        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.factSumExcel2HalfYear(factFromExcel,
                                                                       tableFactFromExcel,

                                                                       prevYear,
                                                                       currentDate.currentdateformated)
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFrom1C = query.diffOfTwoCol2HalfYear(firstFactFrom1C,
                                                                    tableFactFrom1C,

                                                                    prevYear,
                                                                    secondFactFrom1C,
                                                                    currentDate.currentdateformated)

        array1 = []
        array2 = []

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            array1.append(element[1]['diff'])
        for element in factForCurrentPeriodUsdFrom1C.iterrows():
            array2.append(element[1]['diff'])
        zippedArrs = zip_longest(array1, array2)

        for i in zippedArrs:
            if i[0] == 0 or i[0] is None:
                self.dataFrame.values[2].append(i[1])
            else:
                self.dataFrame.values[2].append(i[0])





###############################################################################

    def dataFrameSinceBegin(self,
                                   connectorToDb,
                                   currentDate,
                                   factFromExcel='plan_debit_usd',
                                   tableFactFromExcel='f_department_data_excel',
                                   currentYear='2016',
                                   firstFactFrom1C='cost_usd',
                                   tableFactFrom1C='f_realization_1c',
                                   secondFactFrom1C='prime_cost_usd',
                                   planFromExcel='plan_cost_usd',
                                   prevYear='2016',
                                   tablePlanFromExcel=True,
                                   ):


        if tablePlanFromExcel == True: tablePlanFromExcel = tableFactFromExcel

        # Записываются департаменты из базы в ДФ
        query = QueryExecutor(connectorToDb.connectiontodb)
        departmentsForDataFrame = query.sinceBeginOfYearForExcel(factFromExcel,
                                                              tableFactFromExcel,
                                                              currentYear,
                                                              currentDate.currentdateformated
                                                              )
        for element in departmentsForDataFrame.iterrows():
            self.dataFrame.rows.append(
                [element[1]['department_name']])  # записываются строки из базы в департаменты в ДФ

        # тут сначала ищется факт в экселе
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.sinceBeginOfYearForExcel(factFromExcel,
                                                                       tableFactFromExcel,
                                                                       currentYear,
                                                                       currentDate.currentdateformated)
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFrom1C = query.sinceBeginOfYear1CDiff(firstFactFrom1C,
                                                                     tableFactFrom1C,
                                                                     currentYear,
                                                                     secondFactFrom1C,
                                                                     currentDate.currentdateformated)

        array1 = []
        array2 = []

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            array1.append(element[1]['diff'])
        for element in factForCurrentPeriodUsdFrom1C.iterrows():
            array2.append(element[1]['diff'])
        zippedArrs = zip_longest(array1, array2)

        for i in zippedArrs:
            if i[0] == 0 or i[0] is None:
                self.dataFrame.values[1].append(i[1])
            else:
                self.dataFrame.values[1].append(i[0])




        # Теперь ищется план в экселе

        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.sinceBeginOfYearForExcel(planFromExcel,
                                                                       tablePlanFromExcel,

                                                                       currentYear,
                                                                       currentDate.currentdateformated)

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            # тут записывается в дата фрейм план из экселя
            self.dataFrame.values[0].append(element[1]['diff'])

        # Факт за предыдущий год

        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFromExcel = query.sinceBeginOfYearForExcel(factFromExcel,
                                                                       tableFactFromExcel,

                                                                       prevYear,
                                                                       currentDate.currentdateformated)
        query = QueryExecutor(connectorToDb.connectiontodb)
        factForCurrentPeriodUsdFrom1C = query.sinceBeginOfYear1CDiff(firstFactFrom1C,
                                                                     tableFactFrom1C,

                                                                     prevYear,
                                                                     secondFactFrom1C,
                                                                     currentDate.currentdateformated)
        array1 = []
        array2 = []

        for element in factForCurrentPeriodUsdFromExcel.iterrows():
            array1.append(element[1]['diff'])
        for element in factForCurrentPeriodUsdFrom1C.iterrows():
            array2.append(element[1]['diff'])
        zippedArrs = zip_longest(array1, array2)

        for i in zippedArrs:
            if i[0] == 0 or i[0] is None:
                self.dataFrame.values[2].append(i[1])
            else:
                self.dataFrame.values[2].append(i[0])


