import pandas as pd

class QueryExecutor:
    def __init__(self, connection):
        self.connection = connection
    def currentQuoter(self, currentDate):
        self.currentQuoter = pd.read_sql(""" SELECT quarter
                                                    FROM s_calendar
                                                    WHERE date='{}'""".format(currentDate), self.connection)
        return self.currentQuoter
    def singleColumnSum1C(self, columnCostName, tableRealizationName, quoterNumber, year):
        ### Агрегация за квартал в одной колонке
        # return sum, department_name
        self.sumSingleCol1C = pd.read_sql(""" WITH query_1 AS (
                      SELECT sum({0}) as sum, department_id as department
                      FROM {1}
                      JOIN s_calendar ON {1}.date_id = s_calendar.date
                      WHERE quarter = {2} AND year = '{3}'
                      GROUP BY department_id
                    )
                    SELECT  sum, department_name
                    FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            quoterNumber,
            year
        ), self.connection)
        return self.sumSingleCol1C
    def doubleColumnSum1C(self, columnCostName, tableRealizationName, quoterNumber, year, columnPrimeCostName):
        ### Сумма двух агрегированных колонок
        # return sum,prime_cost_sum, department_name
        self.sumDoubleCol1C = pd.read_sql(""" WITH query_1 AS (
                      SELECT sum({0}) as sum,sum({4}) as prime_cost_sum, department_id as department
                      FROM {1}
                      JOIN s_calendar ON {1}.date_id = s_calendar.date
                      WHERE quarter = {2} AND year = '{3}'
                      GROUP BY department_id
                    )
                    SELECT  sum,prime_cost_sum, department_name
                    FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            quoterNumber,
            year,
            columnPrimeCostName
        ), self.connection)
        return self.sumDoubleCol1C
    def diffOfTwoCol1C(self, columnCostName, tableRealizationName, quoterNumber, year, columnPrimeCostName):
        ### Разница двух агрегированных колонок
        # return (sum - prime_cost_sum) AS diff, department_name
        self.diffOfTwoCol1C = pd.read_sql(""" WITH query_1 AS (
                      SELECT sum({0}) as sum,sum({4}) as prime_cost_sum, department_id as department
                      FROM {1}
                      JOIN s_calendar ON {1}.date_id = s_calendar.date
                      WHERE quarter = {2} AND year = '{3}'
                      GROUP BY department_id
                    )
                    SELECT (sum - prime_cost_sum) AS diff, department_name
                    FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            quoterNumber,
            year,
            columnPrimeCostName
        ), self.connection)
        return self.diffOfTwoCol1C
    def factSumExcel(self, columnCostName, tableRealizationName, quoterNumber, year):
        ### Факт из таблицы эксель и агрегация по кварталу значений
        # return diff, department_name
        self.factSumExcel = pd.read_sql(""" WITH query_1 AS (
                      SELECT sum({0}) as sum, department_id as department
                      FROM {1}
                      JOIN s_calendar ON {1}.date_id = s_calendar.date
                      WHERE quarter = {2} AND year = '{3}'
                      GROUP BY department_id
                    )
                    SELECT sum AS diff, department_name
                    FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            quoterNumber,
            year
        ), self.connection)
        return self.factSumExcel

    def diffOfTwoCol1CIfCurDateInSelQuoter(self,
                                           columnCostName,
                                           tableRealizationName,
                                           quoterNumber,
                                           year,
                                           columnPrimeCostName,
                                           currentDateFormated):
        ### Разница двух агрегированных колонок
        # return (sum - prime_cost_sum) AS diff, department_name
        self.diffOfTwoCol1C = pd.read_sql(""" WITH query_1 AS (
                      SELECT sum({0}) as sum,sum({4}) as prime_cost_sum, department_id as department
                      FROM {1}
                      JOIN s_calendar ON {1}.date_id = s_calendar.date
                      WHERE quarter = {2} AND year = '{3}' AND date_id < '{5}'
                      GROUP BY department_id
                    )
                    SELECT (sum - prime_cost_sum) AS diff, department_name
                    FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            quoterNumber,
            year,
            columnPrimeCostName,
            currentDateFormated
        ), self.connection)
        return self.diffOfTwoCol1C

    def factSumExcelIfCurrDateInSelQuoter(self,
                                          columnCostName,
                                          tableRealizationName,
                                          quoterNumber,
                                          year,
                                          currentDateFormated):
        ### Факт из таблицы эксель и агрегация по кварталу значений
        # return diff, department_name
        self.factSumExcel = pd.read_sql(""" WITH query_1 AS (
                      SELECT sum({0}) as sum, department_id as department
                      FROM {1}
                      JOIN s_calendar ON {1}.date_id = s_calendar.date
                      WHERE quarter = {2} AND year = '{3}' AND date_id < '{4}'
                      GROUP BY department_id
                    )
                    SELECT sum AS diff, department_name
                    FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            quoterNumber,
            year,
            currentDateFormated
        ), self.connection)
        return self.factSumExcel

    def factSumExcel1HalfYear(self,
                              columnCostName,
                              tableRealizationName,
                              year,
                              currentDateFormated):
        ### Факт из таблицы эксель и агрегация по кварталу значений
        # return diff, department_name
        self.factSumExcel = pd.read_sql(""" WITH query_1 AS (
                          SELECT sum({0}) as sum, department_id as department
                          FROM {1}
                          JOIN s_calendar ON {1}.date_id = s_calendar.date
                          WHERE quarter IN (1,2) AND year = '{2}' AND date_id < '{3}'
                          GROUP BY department_id
                        )
                        SELECT sum AS diff, department_name
                        FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            year,
            currentDateFormated
        ), self.connection)
        return self.factSumExcel

    def factSumExcel2HalfYear(self,
                              columnCostName,
                              tableRealizationName,
                              year,
                              currentDateFormated):
        ### Факт из таблицы эксель и агрегация по кварталу значений
        # return diff, department_name
        self.factSumExcel = pd.read_sql(""" WITH query_1 AS (
                              SELECT sum({0}) as sum, department_id as department
                              FROM {1}
                              JOIN s_calendar ON {1}.date_id = s_calendar.date
                              WHERE quarter IN (3,4) AND year = '{2}' AND date_id < '{3}'
                              GROUP BY department_id
                            )
                            SELECT sum AS diff, department_name
                            FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            year,
            currentDateFormated
        ), self.connection)
        return self.factSumExcel

    def diffOfTwoCol1HalfYear(self,
                              columnCostName,
                              tableRealizationName,
                              year,
                              columnPrimeCostName,
                              currentDateFormated):
            ### Разница двух агрегированных колонок
            # return (sum - prime_cost_sum) AS diff, department_name
            self.diffOfTwoCol1C = pd.read_sql(""" WITH query_1 AS (
                          SELECT sum({0}) as sum,sum({3}) as prime_cost_sum, department_id as department
                          FROM {1}
                          JOIN s_calendar ON {1}.date_id = s_calendar.date
                          WHERE quarter IN (1,2) AND year = '{2}' AND date_id < '{4}'
                          GROUP BY department_id
                        )
                        SELECT (sum - prime_cost_sum) AS diff, department_name
                        FROM query_1 JOIN s_department ON department = s_department.id """.format(
                columnCostName,
                tableRealizationName,
                year,
                columnPrimeCostName,
                currentDateFormated
            ), self.connection)
            return self.diffOfTwoCol1C

    def diffOfTwoCol2HalfYear(self,
                              columnCostName,
                              tableRealizationName,
                              year,
                              columnPrimeCostName,
                              currentDateFormated):
        ### Разница двух агрегированных колонок
        # return (sum - prime_cost_sum) AS diff, department_name
        self.diffOfTwoCol1C = pd.read_sql(""" WITH query_1 AS (
                          SELECT sum({0}) as sum,sum({3}) as prime_cost_sum, department_id as department
                          FROM {1}
                          JOIN s_calendar ON {1}.date_id = s_calendar.date
                          WHERE quarter IN (3,4) AND year = '{2}' AND date_id < '{4}'
                          GROUP BY department_id
                        )
                        SELECT (sum - prime_cost_sum) AS diff, department_name
                        FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            year,
            columnPrimeCostName,
            currentDateFormated
        ), self.connection)
        return self.diffOfTwoCol1C

    def sinceBeginOfYear1CDiff(self,
                              columnCostName,
                              tableRealizationName,
                              year,
                              columnPrimeCostName,
                              currentDateFormated):

        ### Разница двух агрегированных колонок
        # return (sum - prime_cost_sum) AS diff, department_name
        self.diffOfTwoCol1C = pd.read_sql(""" WITH query_1 AS (
                              SELECT sum({0}) as sum,sum({3}) as prime_cost_sum, department_id as department
                              FROM {1}
                              JOIN s_calendar ON {1}.date_id = s_calendar.date
                              WHERE year = '{2}' AND date_id < '{4}'
                              GROUP BY department_id
                            )
                            SELECT (sum - prime_cost_sum) AS diff, department_name
                            FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            year,
            columnPrimeCostName,
            currentDateFormated
        ), self.connection)
        return self.diffOfTwoCol1C

    def sinceBeginOfYearForExcel(self,
                              columnCostName,
                              tableRealizationName,
                              year,
                              currentDateFormated):

        ### Факт из таблицы эксель и агрегация по кварталу значений
        # return diff, department_name
        self.factSumExcel = pd.read_sql(""" WITH query_1 AS (
                              SELECT sum({0}) as sum, department_id as department
                              FROM {1}
                              JOIN s_calendar ON {1}.date_id = s_calendar.date
                              WHERE year = '{2}' AND date_id < '{3}'
                              GROUP BY department_id
                            )
                            SELECT sum AS diff, department_name
                            FROM query_1 JOIN s_department ON department = s_department.id """.format(
            columnCostName,
            tableRealizationName,
            year,
            currentDateFormated
        ), self.connection)
        return self.factSumExcel


