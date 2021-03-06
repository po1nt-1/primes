import sqlite3
import inspect
import os
import sys
from typing import Union, Tuple


def get_script_dir(follow_symlinks: bool = True) -> str:
    # https://clck.ru/P8NUA
    if getattr(sys, 'frozen', False):  # type: ignore
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)


def bar(i: int, limit: int,
        per: bool = False, hop: bool = True) -> None:
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


class session:
    def __init__(self) -> None:
        self.conn: sqlite3.Connection
        self.c: sqlite3.Cursor

    def open_db(self) -> sqlite3.Connection:
        """
            Открытие БД.
            Если БД не существует, будет создана новая БД.
        """
        try:
            self.conn = sqlite3.connect(
                get_script_dir() + "/numbers.db",
                timeout=1800)
            self.c = self.conn.cursor()

            return self.conn
        except KeyboardInterrupt:
            raise KeyboardInterrupt("\nThe program is stopped during " +
                                    "the operation with the database.")

    def close_db(self) -> None:
        try:
            self.conn.close()

            return None
        except KeyboardInterrupt:
            raise KeyboardInterrupt("\nThe program is stopped during " +
                                    "the operation with the database.")

    def create_table(self, limit: int, conn: sqlite3.Connection) -> None:
        """
            Создание таблицы с нуля.
            Заполнение таблицы
        """
        try:
            with conn:
                self.c.execute("""DROP TABLE IF EXISTS numbers""")
                self.c.execute("""
                    CREATE TABLE numbers (
                    number      INTEGER     PRIMARY KEY AUTOINCREMENT, \
                    status      INTEGER     DEFAULT 0 NOT NULL)""")

            sql = """INSERT INTO numbers (number) VALUES (?)"""
            print("Table creation...")
            with conn:
                for i in range(limit):
                    self.c.execute(sql, (i,))
                    if limit >= 10000:
                        bar(i, limit, hop=False)
                self.c.execute(sql, (limit,))
                bar(i + 1, limit)
                print("Table created.")
            self.conn.commit()

            return None
        except KeyboardInterrupt:
            raise KeyboardInterrupt("\nThe program is stopped during " +
                                    "the operation with the database.")

    def set(self, number: int, status: Union[bool, str]) -> Union[None, str]:
        try:
            if status is True:
                status_int = 1
            elif status is False:
                status_int = 0
            else:
                return "err"
            self.c.execute(
                """UPDATE numbers SET status=? WHERE number=?""",
                (status_int, number))
            return None
        except KeyboardInterrupt:
            raise KeyboardInterrupt("\nThe program is stopped during " +
                                    "the operation with the database.")

    def get(self, number: int) -> Union[bool, str]:
        try:
            self.c.execute("""SELECT status FROM numbers WHERE number=?""",
                           (number, ))
            status_int: Tuple[int] = self.c.fetchone()
            if status_int[0] == 1:
                status = True
            elif status_int[0] == 0:
                status = False
            else:
                return "err"
            return status
        except KeyboardInterrupt:
            raise KeyboardInterrupt("\nThe program is stopped during " +
                                    "the operation with the database.")

    def primes_count(self) -> int:
        try:
            self.c.execute(
                """SELECT COUNT(status) FROM numbers WHERE status=1""")
            result: Tuple[int] = self.c.fetchone()
            return result[0]
        except KeyboardInterrupt:
            raise KeyboardInterrupt("\nThe program is stopped during " +
                                    "the operation with the database.")
