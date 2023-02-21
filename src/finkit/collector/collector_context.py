from datetime import timezone, timedelta, datetime, date

import chinese_calendar as cal
import sqlalchemy as sal
import toml


class CollectorContext(object):

    def __init__(self, config_file: str):
        self.config = toml.load(config_file)

        collector: dict = self.config.get("collector")
        if collector is None:
            raise ValueError("non collector jobs defined, exit...")

        tz = collector.get("timezone", 8)
        self.timezone = timezone(timedelta(hours=tz))

        start_date = collector.get("start_date", "2020-01-01")
        end_date = collector.get("end_date", "2023-12-31")
        self.workdays = cal.get_workdays(date.fromisoformat(start_date), date.fromisoformat(end_date))

        self.jobs = [x for x in collector.get("job", []) if "name" in x and "func" in x]
        self._engine = None

    def get_engine(self):
        if self._engine is None:
            if self.config.get("database") is None:
                raise ValueError("missing database config")
            database: dict = self.config.get("database")
            driver = database.get("driver")
            host = database.get("host")
            port = database.get("port")
            username = database.get("username")
            password = database.get("password")
            database_name = database.get("database")
            url = sal.URL.create(driver, username, password, host, port, database_name)
            self._engine = sal.create_engine(url)
        return self._engine
