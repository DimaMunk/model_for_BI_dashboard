import psycopg2

class ConnectorToDB:
    def cursorConnectionOpen(self, dbName, dbUser, dbHost, dbPsswrd):
        self.connectiontodb = psycopg2.connect(
            """ dbname = {} user = {} host = {} password = {} """.format(dbName, dbUser, dbHost, dbPsswrd))
        self.cursor = self.connectiontodb.cursor()
    def cursorConnectionClose(self):
        self.connectiontodb.close()



