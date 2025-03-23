from abc import ABC, abstractmethod
import psycopg2
from psycopg2 import sql
from src.configuration import ConfigHandler


class DbHandlerBasic(ABC):
    """
        Basic interface for database change (create db, tables, insert values).
    """

    @abstractmethod
    def create_db(self, db_name: str) -> None:
        """
            Create a new database, delete if it already exists.
        """
        pass

    @abstractmethod
    def create_tables(self, db_name: str) -> None:
        """
            Create necessary tables in the specified database.
        """
        pass

    @abstractmethod
    def insert_values(self, *args) -> None:
        """
            Insert data into the specified tables.
        """
        pass


class DbHandler(DbHandlerBasic, ConfigHandler):
    """
        Concrete implementation of DbHandlerBasic interface.
    """

    def __init__(self):
        """
            Initialize the DbHandler instance.
        """
        self.__db_name = ""
        super().__init__()

    def create_db(self, db_name: str) -> None:
        """
            Creates a new database, delete if it already exists
        """
        if db_name.lower().strip() in ["", "none"]:
            print("Database name should not be empty")
            return
        params = super().config()
        conn = psycopg2.connect(dbname='postgres', **params)
        with conn.cursor() as cur:
            conn.autocommit = True
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", [db_name])
            db_exists = cur.fetchone() is not None

            if db_exists:
                print(f"Database '{db_name}' already exists.")
                if input(f"Do you want to delete {db_name} and create a new one (yes/no): ").strip().lower() != "yes":
                    print("Operation cancelled")
                    return

                cur.execute(
                    sql.SQL("SELECT pg_terminate_backend(pg_stat_activity.pid) "
                            "FROM pg_stat_activity WHERE datname=%s AND pid <> pg_backend_pid();"),
                    [db_name]
                )
                cur.execute(sql.SQL("DROP DATABASE {};").format(sql.Identifier(db_name)))
                print(f"Database '{db_name}' deleted")

            cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name)))
            print(f"Database '{db_name}' created successfully")
            self.__db_name = db_name
        conn.close()

    def create_tables(self, first_table: str = "employers", second_table: str = "openings",
                      db_name: str = None) -> None:
        """
            Create two tables in the database.
        """
        if db_name is None:
            db_name = self.__db_name
        if db_name.lower().strip() in ["", "none"]:
            print("Database is not exist. Please create database using create_db method.")
            return
        self.employers_table: str = first_table
        self.openings_table: str = second_table
        params = super().config()
        conn = psycopg2.connect(dbname=db_name, **params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {} (
                            id INT PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            site_url TEXT,
                            area_name VARCHAR(255),
                            industries_name VARCHAR(255),
                            open_vacancies int
                            );
                            """).format(sql.Identifier(first_table))
                )
                cur.execute(
                    sql.SQL(""" 
                            CREATE TABLE IF NOT EXISTS {} (
                            id BIGINT PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            area_name VARCHAR(255) NOT NULL,
                            salary INT,
                            employer_id INT,
                            employer_name VARCHAR(255) NOT NULL,
                            requirement TEXT,
                            responsibility TEXT,
                            FOREIGN KEY (employer_id) REFERENCES {}(id)
                            );
                            """).format(sql.Identifier(second_table), sql.Identifier(first_table))
                )
                print(f"Tables '{first_table}' and '{second_table}' created successfully")
        conn.close()

    def insert_values(self, employers_list: list[dict], openings_list: list[dict]) -> None:
        """
            Insert data into employers and openings tables in the database.
        """
        db_name = self.__db_name
        if db_name.lower().strip() in ["", "none"]:
            print("Database is not exist. Please create database using create_db method.")
            return

        params = super().config()
        conn = psycopg2.connect(dbname=db_name, **params)
        with conn:
            with conn.cursor() as cur:
                query1 = sql.SQL("""
                    INSERT INTO {} (id, name, site_url, area_name, industries_name, open_vacancies)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """).format(sql.Identifier(self.employers_table))
                value_emp = [(emp["id"], emp["name"], emp["site_url"], emp["area_name"],
                              emp["industries_name"], emp["open_vacancies"]) for emp in employers_list]
                cur.executemany(query1, value_emp)

                query2 = sql.SQL("""
                    INSERT INTO {} (id, name, area_name, salary, employer_id, employer_name, requirement,
                     responsibility)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """).format(sql.Identifier(self.openings_table))
                value_open = [(opening["id"], opening["name"], opening["area_name"], opening["salary"],
                               opening["employer_id"], opening["employer_name"], opening["requirement"],
                               opening["responsibility"]) for opening in openings_list]
                cur.executemany(query2, value_open)
                print("Data inserted successfully")
        conn.close()
