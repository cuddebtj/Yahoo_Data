import psycopg2
import yaml
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import AsIs
from io import StringIO

# from scripts.output_txt import log_print
from output_txt import log_print


class DatabaseCursor(object):
    def __init__(self, credential_file, **kwargs):
        """
        Import database credentials

        credential_file = path to private yaml file
        kwargs = {option_schema: "raw"}
        """

        self.kwargs = kwargs
        if "option_schema" not in self.kwargs.keys():
            self.kwargs["option_schema"] = "public"

        try:
            with open(credential_file) as file:
                self.credentials = yaml.load(file, Loader=yaml.SafeLoader)

        except Exception as e:
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="__init__",
                cred_file="Credential File",
            )
            # print(f"\n----ERROR db_psql_model.py: credential file\n----{e}\n")

    def __enter__(self):
        """
        Set up connection and cursor
        """

        try:
            self.conn = psycopg2.connect(
                dbname=self.credentials["psql_database"],
                user=self.credentials["psql_username"],
                password=self.credentials["psql_password"],
                options=f"-c search_path={self.kwargs['option_schema']}",
            )
            self.cur = self.conn.cursor()

            return self.cur

        except (Exception, psycopg2.OperationalError) as e:
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="__enter__",
                connection="Connection Error",
            )
            # print(f"\n----ERROR db_psql_model.py: __enter__\n----{e}\n")

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

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(error=e, module_="db_psql_model.py", func="get_tables_metadata")
            # print(f"\n----ERROR db_psql_model.py: get_tables_metadata\n----{e}\n")

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
            log_print(
                success="POSTGRESQL CREATE SCHEMA IF NOT EXISTS in MenOfMadison",
                module_="db_psql_model.py",
                func="create_schema",
                schema=schema,
            )
            # print(f"\n----{schema} schema created within MenOfMadison.\n")

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="get_tables_metadata",
                schema=schema,
            )
            # print(f"\n----ERROR db_psql_model.py: create_schema\n----{schema}\n----{e}\n")

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
            log_print(
                success="POSTGRESQL DROP SCHEMA IF EXISTS in MenOfMadison",
                module_="db_psql_model.py",
                func="drop_schema",
                schema=schema,
            )
            # print(f"\n----{schema} schema dropped within MenOfMadison.\n")

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e, module_="db_psql_model.py", func="drop_schema", schema=schema
            )
            # print(f"\n----ERROR db_psql_model.py: drop_schema\n----{schema}\n----{error}\n")

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
            log_print(
                success="POSTGRESQL DROP TABLE IF EXISTS in MenOfMadison",
                module_="db_psql_model.py",
                func="drop_table",
                schema=schema,
                table=table,
                query=sql_query
            )
            # print(f"\n----{table} table dropped within MenOfMadison {schema}\n")

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="drop_table",
                schema=schema,
                table=table,
            )
            # print(f"\n----ERROR db_psql_model.py: drop_table\n----{schema}.{table}\n----{error}\n")

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

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        columns = df.columns.to_list()
        fields = []
        for col in columns:
            fields.append(f'"{col}" TEXT')
        buffer.seek(0)

        try:
            if "YES" == str(first_time).upper():
                if "option_schema" in self.kwargs:
                    self.drop_table(schema=self.kwargs["option_schema"], table=table)
                cursor = self.__enter__()
                create_sql = (
                    f'CREATE TABLE IF NOT EXISTS "{self.kwargs["option_schema"]}"."{table}" ({", ".join(fields)})'
                )
                cursor.execute(create_sql)
                log_print(
                    success="CREATE TABLE IF NOT EXISTS in MenOfMadison",
                    module_="db_psql_model.py",
                    func="copy_table_to_postgres_new --> Create Table",
                    first_time=first_time,
                    schema=self.kwargs["option_schema"],
                    table=table,
                    query=create_sql,
                )
                self.__exit__(exc_result=True)
                
            cursor = self.__enter__()
            copy_to = sql.SQL(
                "COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE);"
            ).format(
                table=sql.Identifier(table),
            )
            cursor.copy_expert(copy_to, buffer)
            self.__exit__(exc_result=True)
            log_print(
                success="COPY EXPERT to MenOfMadison",
                module_="db_psql_model.py",
                func="copy_table_to_postgres_new",
                first_time=first_time,
                schema=self.kwargs["option_schema"],
                table=table,
                query=copy_to,
            )
            # print(f"\n----Upload successful: {table}\n")

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="copy_table_to_postgres_new",
                first_time=first_time,
                schema=self.kwargs["option_schema"],
                table=table,
                copy_query=copy_to,
                create_query=create_sql,
            )
            # print(f"\n----ERROR db_psql_model.py: copy_table_to_postgres_new\n----{table}\n----{e}\n")

    def copy_data_from_postgres(self, query):
        """
        Copy data from Postgresql Query into
        Pandas dataframe
        https://towardsdatascience.com/optimizing-pandas-read-sql-for-postgres-f31cd7f707ab

        query = "select * from raw.test"
        """
        cursor = self.__enter__()

        sql_query = "COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)".format(
            query=query
        )
        buffer = StringIO()

        try:
            cursor.copy_expert(sql_query, buffer)
            buffer.seek(0)
            df = pd.read_csv(buffer)
            self.__exit__(exc_result=True)
            log_print(
                success="COPY QUERY FROM MenOfMadison",
                module_="db_psql_model.py",
                func="copy_data_from_postgres",
                query=sql_query,
            )
            # print(f"\n----Successfully pulled: {query}\n")
            return df

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="copy_data_from_postgres",
                query=sql_query,
            )
            # print(f"\n----ERROR db_psql_model.py: copy_data_from_postgres\n----{query}\n----{e}\n")
