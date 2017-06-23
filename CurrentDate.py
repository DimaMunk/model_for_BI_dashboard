from datetime import datetime

class CurrentDate:
    def __init__(self):
        self._currentdate = datetime.now() # нужно для вычислений в классе, нигде не используется
        self.currentdateformated = datetime.strftime(self._currentdate, "%Y-%m-%d")
        self.currentQuarter = round(self._currentdate.month / 3 + 1)
        self.currentYear = self._currentdate.year
        self.prevYear = self.currentYear - 1

