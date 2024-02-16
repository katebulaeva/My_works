
import psycopg2

from sqlalchemy import create_engine

class Sql_Test:
    def __init__(self):
        self.mssql_engine = db.create_engine(
            f'mssql+pyodbc://{mssql_user}:{mssql_passowrd}@{host}:{mssql_port}/{database}?driver=ODBC Driver 17 for SQL Server')
        self.postgres_engine = db.create_engine(
            'postgresql+psycopg2://{}:{}@{}/{}'.format(psql_user, psql_password, host, database))
        self.mysql_engine = db.create_engine(
            'mysql+pymysql://{}:{}@{}/{}'.format(mysql_user, mysql_password, host, database))

    def connect_to_postgres_server(self):
        """
        For connect to Postgresql server
        :return:
        """
        try:
            self.postgres_engine.connect()
        except:
            raise Exception(
                "Ваш пароль {} или пароль {} и.т.др не верны. Пожалуйста насторойте конфигурационный файл ".format(
                    psql_user, psql_password))

        if self.postgres_engine.connect():
            print('Успешно соединено')


    def create_table_(self, max_row: int = None,
                            max_value: int = None,
                            unique_count_value: int = None,
                            table_name1: str = None):
        table = self.postgres_engine.execute(
            "select exists (select * from information_schema.tables where table_name = '{}' and table_schema = 'public');".format(
                table_name1))
        exists_table = list(table)[0][0]

        if exists_table is not True:
            mysql_result = self.postgres_engine.execute(
                "create table  {} (id  SERIAL PRIMARY KEY,number BIGINT not null);".format(table_name1))
            mysql_result = self.postgres_engine.execute(
                "create table {} (id  SERIAL PRIMARY KEY,number BIGINT not null);".format(table_name1 +'1'))
            print("Успешно созданы ваши таблицы  {} и {} ".format(table_name1, table_name1 + '1'))
            self.insert_psql(table_name1, max_value, max_row, unique_count_value)
        else:
            print('В Postgresql таблица {} уже существует. Пожалуйста выберите другое имя '.format(table_name1))



    def insert_psql(self, table_name_to_insert: int,
                     max_value: int,
                     max_row: int,
                     unique_count_value: int):
        """
        Insert to Postgresql SQL server a data
        :param table_name_to_insert:
        :param max_value:
        :param max_row:
        :param unique_count_value:
        :return:
        """

        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old
        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        second_query = self.postgres_engine.execute("select number from {}".format(table_name_to_insert))
        if not list(second_query):
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                self.postgres_engine.execute("insert into {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))
                self.postgres_engine.execute("insert into {} (number) values ('{}')".format(table_name_to_insert + '1',values_to_insert[0][count]))
            for count in range(len(values_to_insert[1])):
                self.postgres_engine.execute("insert into {} (number ) values ('{}')".format(table_name_to_insert + '1',values_to_insert[1][count]))
                self.postgres_engine.execute("insert into {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))

            after = time()
            self.postgres_engine.execute("CREATE INDEX number_index ON {} ( id, number);".format(table_name_to_insert))
            print("Время исполнения вставки в Postgresql ", after - before, "s")



    def select_from_to(self, value_from, value_to, table_name, table_column=None):
        """
        The table_column dont work!!!
        To get value from and to
        :param value_from:
        :param value_to:
        :param table_name:
        :return:
        """

        before = time()
        self.postgres_engine.execute("select * from {} where number between {} and {}".format(table_name, value_from, value_to))
        after = time()
        print("Время исполнения запроса между значениями {} и {} в Postgres  ".format(value_from, value_to), after - before, "s")










if __name__ == '__main__':
    TABLE_NAME = 'new_table'
    server = Sql_Test()
    server.create_table_for_all_server(sql_server='postgresql', table_name1=TABLE_NAME, max_value=10000, max_row=100, unique_count_value=10)

    # server.select_from_to(1221, 2000,TABLE_NAME)
    # server.select_max_table(TABLE_NAME)
    # server.select_number_where(TABLE_NAME,344)
    # server.in_both_table_number(TABLE_NAME, TABLE_NAME+"1")
