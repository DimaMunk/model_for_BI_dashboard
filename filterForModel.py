from dataFrameForPolymBi import Filter


class FilterForModel(Filter):
    def create(self, currentDate):
        # -Filter-
        self.myFilter = Filter
        self.myFilter.values = [['1 квартал {0} года'.format(currentDate.currentYear),
                            '2 квартал {0} года'.format(currentDate.currentYear),
                            '3 квартал {0} года'.format(currentDate.currentYear),
                            '4 квартал {0} года'.format(currentDate.currentYear),
                            '1 полугодие {0} года'.format(currentDate.currentYear),
                            '2 полугодие {0} года'.format(currentDate.currentYear),
                            'С начала {0} года'.format(currentDate.currentYear)]]
        self.prevYear = currentDate.currentYear - 1