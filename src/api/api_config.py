from logger.logger import log_event
import os

class Api_config:
    
    def __init__(self, log_event: log_event):

        self.log_event = log_event

        self.sql_port=os.getenv("sql_port")
        self.sql_host=os.getenv("sql_host")
        self.sql_user=os.getenv("sql_user")
        self.sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
        self.sql_database = os.getenv("MYSQL_DATABASE")


        self.validation()

    def validation(self):

        necessary_variables = {
            
            "sql_port": self.sql_port,
            "sql_host": self.sql_host,
            "sql_user": self.sql_user,
            "sql_password": self.sql_password,
            "sql_database": self.sql_database,
        }

        missing = []

        for name , value in necessary_variables.items():
            if value is None:
                missing.append(name)

        if missing:
            self.log_event(level="exception", message=f"Required environment variables are missing: {missing}")
            raise Exception(f"Required environment variables are missing: {missing}")

        else:
            self.log_event(level="info", message="All variables found and loaded.")