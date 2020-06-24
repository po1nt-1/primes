import sqlite3
import inspect
import os
import sys


def get_script_dir(follow_symlinks: bool = True) -> str:
    # https://clck.ru/P8NUA
    if getattr(sys, 'frozen', False):
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)


def bar(i: int, limit: int, per=False, hop: bool = True) -> None:
    d = int(50 / limit * (i + 1))
    text = '\r' + "\u2589" * d + '-' * (50 - d)
    if per:
        print(text, f" {int((100 * i) / limit)} %", end='')
    else:
        print(text, f" {i}/{limit}", end='')
    if hop:
        if i >= limit - 1:
            print()
    return None


class transaction:
    def __init__(self):
        self.conn: sqlite3.Connection
        self.c: sqlite3.Cursor

    def open_db(self) -> int:
        """
            Открытие БД.
            Если БД не существует, будет создана новая БД.
        """
        self.conn = sqlite3.connect(
            get_script_dir() + "/numbers.db")
        self.c = self.conn.cursor()

        return self.conn

    def close_db(self) -> int:
        self.conn.close()

    def create_table(self, limit: int) -> int:
        """
            Создание таблицы с нуля.
            Заполнение таблицы
        """
        self.c.execute("""DROP TABLE IF EXISTS numbers""")
        self.c.execute("""
            CREATE TABLE numbers (
            number      INTEGER     PRIMARY KEY AUTOINCREMENT, \
            status      INTEGER     DEFAULT 0)""")
        print("Table created.")

        sql = """INSERT INTO numbers (number) VALUES (?)"""
        print("Table creation...")
        for i in range(limit):
            self.c.execute(sql, (i,))
            bar(i, limit, hop=False)
        self.c.execute(sql, (limit,))
        bar(i + 1, limit)
        print("Table created.")
        return 0

    def update(self, number: int, status: bool):
        if status:
            status = 1
        else:
            status = 0

        self.c.execute(
            """UPDATE numbers SET status=? WHERE number=?""",
            (status, number))

    def info(self, number: int) -> bool:
        self.c.execute("""SELECT status FROM numbers WHERE number=?""",
                       (number, ))
        status_int = self.c.fetchone()
        if status_int[0] == 1:
            status = True
        elif status_int[0] == 0:
            status = False
        return status
