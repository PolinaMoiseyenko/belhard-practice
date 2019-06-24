import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def creating_db(data, password):
    """This function gets the list of data and then opens
    postgre database and creates a new database."""
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
    """This function gets the list of data and creates
    tables with fields and relations"""
    db_name = data.pop(0).lower()
    string = ''
    for item in data:
        table_name = item.pop(0)

        newstr = ", ".join(item)

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
    """This function gets data from the file and returns
    it as a list of data."""
    raw_data = []
    list_of_data = []

    file = open('db.txt')
    for line in file:
        data = line.strip()
        raw_data.append(data)

    db_name = raw_data.pop(0)

    for item in raw_data:
        list_of_splitted_data = item.split(', ')
        list_of_data.append(list_of_splitted_data)

    list_of_data.insert(0, db_name)

    return list_of_data


if __name__ == '__main__':
    password = "PASSWORD"  # Write the password to the DB here
    data = getting_data()
    db = creating_db(data, password)
    tables = creating_tables(data, password)
