from abc import ABC, abstractmethod
import psycopg2
from src.configuration import ConfigHandler
from psycopg2 import sql


class BasicDbManager(ABC):
    """
        Basic interface for database manager.
    """

    @abstractmethod
    def get_companies_and_vacancies_count(self):
        """
        Fetch companies and their vacancies count from the database.
        """
        pass

    @abstractmethod
    def get_all_vacancies(self):
        """
        Fetch openings from the database.
        """
        pass

    @abstractmethod
    def get_vacancies_with_keyword(self):
        """
        Fetch vacancies with specific keyword from the database.
        """
        pass

    @abstractmethod
    def get_avg_salary(self):
        """
        Fetch average salary from the database.
        """
        pass

    @abstractmethod
    def get_vacancies_with_higher_salary(self):
        """
        Fetch vacancies with higher than avg salary from the database.
        """
        pass


class DBManager(BasicDbManager, ConfigHandler):
    """
    Manage requests to database
    """

    def __init__(self, db_name: str):
        """
        Initialize the DBManager instance.
        """
        self.db_name = db_name
        super().__init__()

    def get_companies_and_vacancies_count(self, table_name: str = "employers") -> None:
        """
        Fetch companies and their vacancies count from the database.
        """
        params = super().config()
        conn = psycopg2.connect(dbname=self.db_name, **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""SELECT * FROM {}""").format(sql.Identifier(table_name)))
                print(cur.fetchall())
        conn.close()

    def get_all_vacancies(self, table_name: str = "openings") -> None:
        """
            Fetch openings from the database.
        """
        params = super().config()
        conn = psycopg2.connect(dbname=self.db_name, **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""SELECT * FROM {}""").format(sql.Identifier(table_name)))
                print(cur.fetchall())
        conn.close()

    def get_avg_salary(self, table_name: str = "openings") -> None:
        """
            Fetch average salary from the database.
        """
        params = super().config()
        conn = psycopg2.connect(dbname=self.db_name, **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""SELECT AVG(salary) FROM {}""").format(sql.Identifier(table_name)))
                print(f"Avg salary - {cur.fetchone()[0]}")
        conn.close()

    def get_vacancies_with_higher_salary(self, table_name: str = "openings") -> None:
        """
            Fetch vacancies with salary higher avg from the database.
        """
        params = super().config()
        conn = psycopg2.connect(dbname=self.db_name, **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    SELECT * FROM {} 
                    WHERE salary > (SELECT AVG(salary) FROM {})
                    """).format(sql.Identifier(table_name), sql.Identifier(table_name)))
                print(cur.fetchall())
        conn.close()

    def get_vacancies_with_keyword(self, key_word: str = "") -> None:
        """
            Fetch vacancies with specific keyword from the database.
        """
        params = super().config()
        conn = psycopg2.connect(dbname=self.db_name, **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL(
                    """
                    SELECT * FROM openings 
                    WHERE name ILIKE {kw}
                    OR requirement ILIKE {kw}
                    OR responsibility ILIKE {kw}
                    """
                ).format(kw=sql.Literal(f"%{key_word}%")))
                print(cur.fetchall())
        conn.close()
