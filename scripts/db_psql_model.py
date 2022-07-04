import psycopg2
import yaml
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from io import StringIO


class DatabaseCursor(object):
    def __init__(self, credential_file, **kwargs):
        """
        Import database credentials
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

            conn_args_dict = {}
            for key, value in self.kwargs.items():
                conn_args_dict[key] = value

            self.engine = create_engine(self.conn_string, connect_args=conn_args_dict)
            self.conn = self.engine.raw_connection()
            self.cur = self.conn.cursor()

        except (Exception, psycopg2.OperationalError) as error:
            print(error)

    def __exit__(self, exc_result):
        """
        Close connection and cursor
        """

        if exc_result == True:
            self.cur.commit()

        else:
            self.cur.rollback()

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

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error: {error}")

    def create_schema(self, schema):
        """
        Create a schema within the Postgresql database

        schema = "test"
        """

        sql_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(
            schema=sql.Identifier(schema)
        )

        try:
            self.cur.execute(sql_query)
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

        sql_query = sql.SQL("DROP SCHEMA IF EXISTS {schema};").format(
            schema=sql.Identifier(schema)
        )

        try:
            self.cur.execute(sql_query)
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

        sql_query = sql.SQL("DROP TABLE IF EXISTS {schema}.{table};").format(
            schema=sql.Identifier(schema), table=sql.Identifier(table)
        )

        try:
            self.cur.execute(sql_query)
            self.__exit__(exc_result=True)
            print(f"{table} table dropped within MenOfMadison.{schema}")

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error: {error}")

    def copy_table_to_postgres(self, df, table):
        """
        Copy table to postgres from a pandas dataframe
        in memory using StringIO
        https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
        https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table

        table = "test"
        """

        df.head(0).to_sql(
            "test", self.engine, if_exists="replace", index=False, schema="dev"
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
            self.cur.copy_expert(query, buffer)
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

        query = "select * from players.test"
        """
        sql_query = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        buffer = StringIO()

        try:
            self.cur.copy_expert(sql_query, buffer)
            buffer.seek(0)
            df = pd.read_csv(buffer)
            self.__exit__(exc_result=True)
            print(f"Successfully pulled: {query}")
            return df

        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit__(exc_result=False)
            print(f"Error {error}\n Query unsuccessful.")
