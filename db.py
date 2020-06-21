import sqlite3
import os
from typing import Union


class db_Error(Exception):
    pass


conn: sqlite3.Connection
c: sqlite3.Cursor


def bar(i: int, count: int, hop: bool = True):
    d = int(50 / count * (i + 1))
    text = '\r' + "\u2589" * d + '-' * (50 - d)
    print(text, f" {i}/{count}", end='')
    if hop:
        if i >= count - 1:
            print()


def open_db() -> int:
    """
        Открытие БД.
        Если БД не существует, будет создана новая БД.
    """
    global conn
    global c

    try:
        conn = sqlite3.connect('primes/_numbers.db')
        c = conn.cursor()
        return 0
    except sqlite3.Error as e:
        raise db_Error("Error in db.open_db(): " + str(e))


def close_db() -> int:
    global conn
    try:
        conn.close()
        return 0
    except sqlite3.Error as e:
        raise db_Error("Error in db.close_db(): " + str(e))


def create_table(limit: int) -> int:
    """
        Создание таблицы, если она не существует.
        Заполнение таблицы
    """
    if not isinstance(limit, int):
        raise db_Error("Error_1 in db.create_table(): Invalid number type")
    if limit < 1:
        raise db_Error("Error_2 in db.create_table(): Invalid number type")
    global conn
    global c

    try:
        with conn:
            c.execute("""DROP TABLE IF EXISTS numbers""")
            c.execute("""
                CREATE TABLE numbers (
                number      INTEGER     PRIMARY KEY AUTOINCREMENT, \
                status      INTEGER     DEFAULT 0)""")
    except sqlite3.Error as e:
        raise db_Error("Error_3 in db.create_table(): " + str(e))
    print("Table created.")

    print("Table filling...")
    sql = """INSERT INTO numbers (number) VALUES (?)"""
    try:
        with conn:
            for i in range(limit):
                c.execute(sql, (i,))
                bar(i, limit, hop=False)
            c.execute(sql, (limit,))
            bar(i + 1, limit)
        return 0
    except sqlite3.IntegrityError as e:
        raise db_Error("Error_4 in db.create_table(): " + str(e))


def update(number: int, status: bool):
    global conn
    global c

    if not isinstance(number, int) or not isinstance(status, bool):
        raise db_Error("Error in db.update(): Invalid data type")

    if status:
        status = 1
    else:
        status = 0

    try:
        with conn:
            c.execute("""
            UPDATE numbers SET status=? WHERE number=?""",
                      (status, number))
        return 0
    except sqlite3.IntegrityError as e:
        raise db_Error("Error in db.update(): " + str(e))


def info(number: int) -> Union[bool, None]:
    global conn
    global c

    if not isinstance(number, int):
        raise db_Error("Error in db.info(): Invalid input type")

    try:
        with conn:
            c.execute("""SELECT status FROM numbers WHERE number=?""",
                      (number, ))
            status_int = c.fetchone()
            if status_int is None:
                return None
            elif status_int[0] == 1:
                status = True
            elif status_int[0] == 0:
                status = False
            else:
                raise db_Error("Error in db.info(): Invalid output type")
    except sqlite3.IntegrityError as e:
        raise db_Error("Error in db.info(): " + str(e))
    if not isinstance(status, bool):
        raise db_Error("Error in db.info(): Invalid output type")
    return status


open_db()
close_db()
