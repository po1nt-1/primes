import db
import multiprocessing as mp
from sqlite3 import OperationalError
from typing import Union, Tuple, Callable, List


class key_error(Exception):
    pass


def process_count() -> int:
    try:
        cpu_cores = mp.cpu_count()
        if cpu_cores >= 6:
            processes_num = cpu_cores - 2
        elif cpu_cores > 2 and cpu_cores < 6:
            processes_num = cpu_cores - 1
        else:
            processes_num = 2
        return processes_num
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program stopped while calculating the number of processes.")


def print_result() -> None:
    try:
        session = db.session()
        conn = session.open_db()
        print(f"Prime numbers found: {session.primes_count()}")
        session.close_db()
        del session
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program stopped when summing up.")
    finally:
        if conn:
            conn.close()
        return None


def pre_calc(limit: int) -> None:
    try:
        session = db.session()
        conn = session.open_db()

        session.create_table(limit, conn)
        with conn:
            if (limit >= 2):
                if session.set(2, True) == "err":
                    raise Exception("\nError in set method.")
            if (limit >= 3):
                if session.set(3, True) == "err":
                    raise Exception("\nError in set method.")

        session.close_db()
        del session
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program stopped while creating the table.")
    finally:
        if conn:
            conn.close()
        return None


def calc(data: Tuple[int, int, int]) -> None:
    try:
        end, start, step = data
        x = start
        limit = end

        session = db.session()
        conn = session.open_db()
        s = 0
        with conn:
            while(x * x <= limit):
                y = 1
                while(y * y <= limit):
                    n = (4 * x * x) + (y * y)
                    if (n <= limit and (n % 12 == 1 or n % 12 == 5)):
                        try:
                            status = session.get(n)
                            if status == "err":
                                raise Exception("\nError in get method.")
                            if session.set(n, not status) == "err":
                                raise Exception("\nError in set method.")
                        except OperationalError:
                            continue

                    n = (3 * x * x) + (y * y)
                    if (n <= limit and n % 12 == 7):
                        try:
                            status = session.get(n)
                            if status == "err":
                                raise Exception("\nError in get method.")
                            if session.set(n, not status) == "err":
                                raise Exception("\nError in set method.")
                        except OperationalError:
                            continue

                    n = (3 * x * x) - (y * y)
                    if (x > 0 and x > y and n <= limit and n % 12 == 11):
                        try:
                            status = session.get(n)
                            if status == "err":
                                raise Exception("\nError in get method.")
                            if session.set(n, not status) == "err":
                                raise Exception("\nError in set method.")
                        except OperationalError:
                            continue
                    y += 1
                x += step
                if s >= 2500:
                    conn.commit()
                    s = 0
                s += 1
                if limit >= 1500:
                    db.bar(x * x, limit, per=True, hop=False)

        db.bar(limit, limit, per=True)
        session.close_db()
        del session
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program is stopped during the main stage of calculations.")
    finally:
        if conn:
            conn.close()
        return None


def post_calc(data: Tuple[int, int, int]) -> None:
    try:
        end, start, step = data
        i = start
        if i < 5:
            i += step
        limit = end

        session = db.session()
        conn = session.open_db()

        s = 0
        with conn:
            while(i * i <= limit):
                try:
                    status = session.get(i)
                    if status == "err":
                        raise Exception("\nError in get method.")
                    elif status is True:
                        n = i * i
                        for j in range(n, limit+1, n):
                            if session.set(j, False) == "err":
                                raise Exception("\nError in set method.")
                except OperationalError:
                    continue

                if s >= 2500:
                    conn.commit()
                    s = 0
                s += 1
                i += step
                if limit >= 1500:
                    db.bar(i * i, limit, per=True, hop=False)

        db.bar(limit, limit, per=True)
        session.close_db()
        del session
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program is stopped during the final " +
            "stage of the calculation.")
    finally:
        if conn:
            conn.close()
        return None


def multi(func: Callable[[Tuple[int, int, int]], None],
          limit: int, step: int) -> None:
    try:
        if func is calc:
            print("The main stage is in progress...")
        elif func is post_calc:
            print("The final stage is in progress...")
        if limit < step or step < 2:
            func((limit, 1, 1))
        else:
            steps_in_process = limit // step
            increased_proc_number = limit % step

            args = list()
            for start in range(1, step + 1):
                end = start + step * (steps_in_process - 1)
                if start <= increased_proc_number:
                    end += step
                data = (end, start, step)
                args.append(data)
            with mp.Pool(step) as p:
                p.map(func, args)
            p.close()
        if func is calc:
            print("\nThe main stage is completed.")
        elif func is post_calc:
            print("\nThe final stage is completed.")
        return None
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program is stopped during the final " +
            "stage of the calculation")


def SieveOfAtkin(limit: int) -> None:
    try:
        step = process_count()
        if step > 3:
            step = 3
        # step = 1    # можно жестко установить количество процессов

        pre_calc(limit)

        multi(calc, limit, step)

        multi(post_calc, limit, step)

        print_result()

        return None
    except KeyboardInterrupt as e:
        raise key_error(
            f"\n{str(e)}" +
            "\nThe program is stopped at the stage " +
            "of switching between stages.")
