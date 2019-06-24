import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def creating_db(data, password):

    db_name = data[0]
    con = psycopg2.connect(
        database="postgres",
        user="postgres",
        password=password,
        host="127.0.0.1",
        port="5432"
    )
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = con.cursor()
    cur.execute(f"CREATE DATABASE {db_name}")
    cur.close()
    con.close()


def creating_tables(data, password):

    field_list = []
    type_list = []

    db_name = data.pop(0).lower()

    for item in data:
        table_name = item[0]
        type_list = item[2::2]
        field_list = item[1::2]
        string = ''
        list_of_parametres = []

        for element in range(len(field_list)):
            string = field_list[element] + " " + type_list[element]
            list_of_parametres.append(string)

        newstr = ", ".join(list_of_parametres)

        con = psycopg2.connect(
            database=db_name,
            user="postgres",
            password=password,
            host="127.0.0.1",
            port="5432"
        )

        print("Database opened successfully")

        cur = con.cursor()
        cur.execute(f"CREATE TABLE {table_name} ({newstr});")

        print("Table created successfully")

        con.commit()
        con.close()


def getting_data():

    list_of_data = []
    file = open('db.txt')  # openning file for reading

    for line in file:
        data = line.strip()

    splitted_data = data.split(';')
    db_name = splitted_data.pop(0)

    for item in splitted_data:
        list_of_splitted_data = item.split(', ')
        list_of_data.append(list_of_splitted_data)

    list_of_data.insert(0, db_name)

    return list_of_data


if __name__ == '__main__':
    password = "SAMIYSLOZHNIYPAROL"  # Write the password to the DB here
    data = getting_data()
    db = creating_db(data, password)
    tables = creating_tables(data, password)
