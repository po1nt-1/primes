import db
from multiprocessing import cpu_count, Pool, Process, \
    Manager, Value, current_process


class key_error(Exception):
    pass


def process_count() -> int:
    try:
        cpu_cores = cpu_count()
        if cpu_cores >= 6:
            processes_num = cpu_cores - 2
        elif cpu_cores > 2 and cpu_cores < 6:
            processes_num = cpu_cores - 1
        else:
            processes_num = 2
        return processes_num
    except KeyboardInterrupt:
        raise key_error(
            "\nThe program stopped while calculating the number of processes.")


def print_result():
    try:
        session = db.session()
        conn = session.open_db()
        print(f"Prime numbers found: {session.primes_count()}")
        session.close_db()
        del session
    except KeyboardInterrupt:
        raise key_error(
            "\nThe program stopped when summing up.")


def pre_calc(limit):
    try:
        session = db.session()
        conn = session.open_db()

        with conn:
            session.create_table(limit)
            if (limit >= 2):
                session.set(2, True)
            if (limit >= 3):
                session.set(3, True)

        session.close_db()
        del session
    except KeyboardInterrupt:
        try:
            session.close_db()
        except db.sqlite3.OperationalError:
            pass
        del session
        raise key_error("\nThe program stopped while creating the table.")


def calc(data: tuple):
    try:
        end, start, step = data
        x = start
        limit = end
        db.bar(0, limit, per=True, hop=False)

        session = db.session()
        conn = session.open_db()
        with conn:
            while(x * x <= limit):
                y = 1
                while(y * y <= limit):
                    n = (4 * x * x) + (y * y)
                    if (n <= limit and (n % 12 == 1 or n % 12 == 5)):
                        status = session.get(n)
                        session.set(n, not status)

                    n = (3 * x * x) + (y * y)
                    if (n <= limit and n % 12 == 7):
                        status = session.get(n)
                        session.set(n, not status)

                    n = (3 * x * x) - (y * y)
                    if (x > 0 and x > y and n <= limit and n % 12 == 11):
                        status = session.get(n)
                        session.set(n, not status)
                    y += 1
                x += step
                if limit >= 1500:
                    db.bar(x * x, limit, per=True, hop=False)

        db.bar(limit, limit, per=True)
        session.close_db()
        del session
    except KeyboardInterrupt:
        try:
            session.close_db()
        except db.sqlite3.OperationalError:
            pass
        del session
        raise key_error(
            "\nThe program is stopped during the main stage of calculations.")


def post_calc(data: tuple):
    try:
        end, start, step = data
        i = start
        if i < 5:
            i += step
        limit = end
        db.bar(0, limit, per=True, hop=False)

        session = db.session()
        conn = session.open_db()
        with conn:
            while(i * i <= limit):
                if session.get(i):
                    n = i * i
                    for j in range(n, limit+1, n):
                        session.set(j, False)
                i += step
                if limit >= 1500:
                    db.bar(i * i, limit, per=True, hop=False)

        db.bar(limit, limit, per=True)
        session.close_db()
        del session
    except KeyboardInterrupt:
        try:
            session.close_db()
        except db.sqlite3.OperationalError:
            pass
        del session
        raise key_error(
            "\nThe program is stopped during the final " +
            "stage of the calculation.")


def multi(func, limit, step):
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
        with Pool(step) as p:
            p.imap(func, args)
    if func is calc:
        print("The main stage is completed.")
    elif func is post_calc:
        print("The final stage is completed.")


def SieveOfAtkin(limit):
    try:
        pre_calc(limit)

        step = process_count() - 1
        step = 1    # можно жестко установить количество процессов

        multi(calc, limit, step)

        multi(post_calc, limit, step)

        print_result()
    except KeyboardInterrupt:
        raise key_error(
            "\nThe program is stopped at the stage " +
            "of switching between stages.")
