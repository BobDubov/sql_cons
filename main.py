import datetime
import psycopg2
import config
import csv
import os


def create_connection(db_name, db_user, db_password, db_host="localhost", db_port='5432'):
    """ Подклчючение к базе """
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Connection to PostgreSQL DB successful")

        return connection

    except Exception as e:
        print(f"[ERR] - {e}")
        finally_connect(connection)


def finally_connect(connection):
    """ Отключение от базы """
    if connection:
        connection.close()
        print('Connection closed')


def check_new_base(connection, csv_dir):
    """ Проверка CSV на новые базы
     если есть - добавляем в таблицу base """
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT b_short_name FROM base_name;"
        )
        base_bd = cursor.fetchall()
        base_name_list = list(map(lambda x: x[0].strip(), base_bd))

        for root, dirs, files in os.walk(csv_dir):
            for name in files:
                if name.lower().endswith('csv'):
                    csv_path = os.path.join(root, name)
                    with open(csv_path, encoding='cp1251') as file:
                        reader = csv.reader(file, delimiter=';')
                        new_base_list = []
                        for line in reader:
                            if line[0] != 'Дата пополнения' and line[3] not in base_name_list:
                                new_base_list.append((line[3], line[2]))

        if len(new_base_list) > 0:
            for query in new_base_list:
                try:
                    sql = "INSERT INTO base_name (b_short_name, b_full_name) VALUES (%s, %s)"
                    cursor.execute(sql, query)
                except Exception as e:
                    print(e.message)


def insert_data(connection, csv_dir):
    """ Читаем CSV с данными о поплнении
     и пишем в таблицу popoln """
    connection.autocommit = True
    five_days = []
    for x in range(5):
        day = (datetime.datetime.now() - datetime.timedelta(days=x)).strftime("%Y%m%d")
        five_days.append(f'{day}.CSV')

    for root, dirs, files in os.walk(csv_dir):
        new_base_list = []
        for name in files:
            if name.upper() in five_days:
                csv_path = os.path.join(root, name)
                with open(csv_path, encoding='cp1251') as file:
                    reader = csv.reader(file, delimiter=';')
                    for line in reader:
                        if line[0] != 'Дата пополнения':
                            date_line = f"{line[0][6:]}-{line[0][3:5]}-{line[0][:2]}"
                            new_base_list.append((line[3], date_line, line[4]))

    with connection.cursor() as cursor:
        if len(new_base_list) > 0:
            for query in new_base_list:
                try:
                    cursor.execute('''DELETE FROM popoln
                                   WHERE b_short_name = %s AND date_popoln = %s AND amount_docs = %s;''',
                                   query)
                    cursor.execute('''INSERT INTO popoln (b_short_name, date_popoln, amount_docs) 
                                   VALUES (%s, %s, %s)''',
                                   query)
                except Exception as e:
                    print(e.message)


def main():
    print()
    cons_bd_data = {
        "name_db": "cons",
        "user_db": config.root_user,
        "pass_db": config.password,
        "host_db": "localhost",
        "port_db": "5432"
    }
    connect = create_connection(cons_bd_data["name_db"], cons_bd_data["user_db"], cons_bd_data["pass_db"])

    check_new_base(connect, config.csv_dir_temp)

    insert_data(connect, config.csv_dir)

    finally_connect(connect)


if __name__ == '__main__':
    main()






# with connection.cursor() as cursor:
#     cursor.execute(
#         '''CREATE TABLE base(
#         id serial PRIMARY KEY,
#         short_name varchar(10) NOT NULL,
#         full_name varchar NOT NULL);'''
#     )


# cursor.executemany(sql, base_list)
# connection.commit()
# with connection.cursor() as cursor:
#     cursor.execute(
#         'DELETE FROM base WHERE "id" = 1;'
#     )
#     print(cursor.fetchone())

# with connection.cursor() as cursor:
#     cursor.execute(
#         '''INSERT INTO base(full_name, short_name) VALUES
#         ('Версия Проф', 'LAW');'''
#     )
