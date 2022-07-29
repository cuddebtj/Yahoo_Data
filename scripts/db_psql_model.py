import psycopg2
import yaml
import pandas as pd
import logging
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import sqlalchemy
from io import StringIO


class DatabaseCursor(object):

    logging.basicConfig()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)

    def __init__(self, credential_file, **kwargs):
        """
        Import database credentials

        credential_file = path to private yaml file
        kwargs = {options: "-c search_path=dev"}
        """

        self.kwargs = kwargs

        try:
            with open(credential_file) as file:
                self.credentials = yaml.load(file, Loader=yaml.FullLoader)

        except Exception as error:
            print(error)

    def __enter__(self):
        """
        Set up connection and cursor
        """

        try:
            self.conn_string = f"postgresql+psycopg2://{self.credentials['psql_username']}:{self.credentials['psql_password']}@localhost/{self.credentials['psql_database']}"
            self.engine = sqlalchemy.create_engine(
                self.conn_string, connect_args=self.kwargs
            )
            self.conn = self.engine.raw_connection()
            self.cur = self.conn.cursor()

            return self.cur

        except (Exception, psycopg2.OperationalError) as error:
            print(error)

    def __exit__(self, exc_result):
        """
        Close connection and cursor

        exc_results = bool
        """

        if exc_result == True:
            self.conn.commit()

        self.cur.close()
        self.conn.close()

    def get_tables_metadata(self):
        """
        Get information from the Postgresql database
        about the tables and schemas within
        """
        try:
            cursor = self.__enter__()
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                SELECT *
                FROM information_schema.tables
                WHERE table_schema NOT LIKE 'pg_%' 
                AND table_schema NOT LIKE 'sql_%'
                AND table_schema NOT LIKE 'information_%';
                """
                )
                results = cur.fetchall()

            self.__exit__(exc_result=True)
            return results

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error: {error}")

    def create_schema(self, schema):
        """
        Create a schema within the Postgresql database

        schema = "test"
        """
        cursor = self.__enter__()

        sql_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(
            schema=sql.Identifier(schema)
        )

        try:
            cursor.execute(sql_query)
            self.__exit__(exc_result=True)
            print(f"{schema} schema created within MenOfMadison.")

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error: {error}")

    def drop_schema(self, schema):
        """
        Create a schema within the Postgresql database

        schema = "test"
        """
        cursor = self.__enter__()

        sql_query = sql.SQL("DROP SCHEMA IF EXISTS {schema};").format(
            schema=sql.Identifier(schema)
        )

        try:
            cursor.execute(sql_query)
            self.__exit__(exc_result=True)
            print(f"{schema} schema dropped within MenOfMadison.")

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error: {error}")

    def drop_table(self, schema, table):
        """
        Drop a table within the Postgresql database

        schema = "public"
        table = "test"
        """
        cursor = self.__enter__()

        sql_query = sql.SQL("DROP TABLE IF EXISTS {schema}.{table};").format(
            schema=sql.Identifier(schema), table=sql.Identifier(table)
        )

        try:
            cursor.execute(sql_query)
            self.__exit__(exc_result=True)
            print(f"{table} table dropped within MenOfMadison.{schema}")

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error: {error}")

    def copy_table_to_postgres_new(self, df, table, first_time="NO"):
        """
        Copy table to postgres from a pandas dataframe
        in memory using StringIO
        https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
        https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table

        table = "test"
        df = pd.DataFrame()
        first_time = "NO"
        """
        cursor = self.__enter__()

        if str(first_time).upper() == "YES":
            if "prod" in str(self.kwargs):
                df.head(0).to_sql(
                    table, self.engine, if_exists="replace", index=False, schema="prod"
                )

            else:
                df.head(0).to_sql(
                    table, self.engine, if_exists="replace", index=False, schema="dev"
                )

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        try:
            query = sql.SQL(
                "COPY {table} FROM STDIN (FORMAT 'csv', HEADER false);"
            ).format(
                table=sql.Identifier(table),
            )
            cursor.copy_expert(query, buffer)
            self.__exit__(exc_result=True)
            print(f"Upload successful: {table}")

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error {error}\nUpload unsuccessful: {table}")

    def copy_data_from_postgres(self, query):
        """
        Copy data from Postgresql Query into
        Pandas dataframe
        https://towardsdatascience.com/optimizing-pandas-read-sql-for-postgres-f31cd7f707ab

        query = "select * from dev.test"
        """
        cursor = self.__enter__()

        sql_query = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        buffer = StringIO()

        try:
            cursor.copy_expert(sql_query, buffer)
            buffer.seek(0)
            df = pd.read_csv(buffer)
            self.__exit__(exc_result=True)
            print(f"Successfully pulled: {query}")
            return df

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error {error}\n Query unsuccessful.")
