import mysql.connector

class sql_db_connection:
    def __init__(self, log_event=None, port=3306, host="localhost", user="root", password="root", database="digital_hunter"):

        self.log_event = log_event
        self.port = port
        self.host=host
        self.user=user
        self.password=password
        self.database=database
        self.connection = self.Get_sql_db_connection()

        
    def Get_sql_db_connection(self):
        try:
            conn = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                database = self.database
                )
            if conn.is_connected():
                self.log_event(level="info",message=f"connected to db: {self.database}")

                return conn
        except Exception as e:
            self.log_event(level="error",message=e)

          



