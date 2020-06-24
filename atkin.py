import db
from multiprocessing import cpu_count, Pool, current_process


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


def pre_calc(limit):
    try:
        t_action = db.transaction()
        conn = t_action.open_db()

        with conn:
            t_action.create_table(limit)
            if (limit >= 2):
                t_action.update(2, True)
            if (limit >= 3):
                t_action.update(3, True)

        t_action.close_db()
        del t_action
    except KeyboardInterrupt:
        try:
            t_action.close_db()
        except db.sqlite3.OperationalError:
            pass
        del t_action
        raise key_error("\nThe program stopped while creating the table.")


global_value = 1


def calc(data: tuple):
    global global_value
    try:
        end, start, step = data
        x = start
        limit = end

        t_action = db.transaction()
        conn = t_action.open_db()
        with conn:
            k = 1
            while(x * x <= limit):
                y = 1
                while(y * y <= limit):
                    n = (4 * x * x) + (y * y)
                    if (n <= limit and (n % 12 == 1 or n % 12 == 5)):
                        t_action.update(n, not t_action.info(n))
                        k = max(n, k)

                    n = (3 * x * x) + (y * y)
                    if (n <= limit and n % 12 == 7):
                        t_action.update(n, not t_action.info(n))
                        k = max(n, k)

                    n = (3 * x * x) - (y * y)
                    if (x > 0 and x > y and n <= limit and n % 12 == 11):
                        t_action.update(n, not t_action.info(n))
                        k = max(n, k)
                    y += 1
                    if k < global_value:
                        global_value = k
                    db.bar(min(global_value, k), limit, per=True, hop=False)
                x += step

        # db.bar(limit, limit, per=True) # мониторинг прогресса вычислений
        print(f"{current_process().name} закончил calc()")
        t_action.close_db()
        del t_action
    except KeyboardInterrupt:
        try:
            t_action.close_db()
        except db.sqlite3.OperationalError:
            pass
        del t_action
        raise key_error(
            "\nThe program is stopped during the main stage of calculations.")


def post_calc(data: tuple):
    try:
        end, start, step = data
        i = start
        if i < 5:
            i += step
        limit = end

        t_action = db.transaction()
        conn = t_action.open_db()
        with conn:
            while(i * i <= limit):
                if t_action.info(i):
                    n = i * i
                    for j in range(n, limit+1, n):
                        t_action.update(j, False)
                i += step

        t_action.close_db()
        del t_action
    except KeyboardInterrupt:
        try:
            t_action.close_db()
        except db.sqlite3.OperationalError:
            pass
        del t_action
        raise key_error(
            "\nThe program is stopped during the final " +
            "stage of the calculation.")


def multic(func, limit):
    step = process_count() - 1
    # step = 1    # можно жестко установить количество процессов

    if limit < step or step < 2:
        func((limit, 1, 1))
    else:
        pool = Pool(step)

        steps_in_process = limit // step
        increased_proc_number = limit % step

        args = list()
        for start in range(1, step + 1):
            end = start + step * (steps_in_process - 1)
            if start <= increased_proc_number:
                end += step
            data = (end, start, step)
            args.append(data)

        pool.map(func, args)

        pool.close()
        pool.join()


def SieveOfAtkin(limit):
    try:
        pre_calc(limit)

        from time import time
        s = time()
        multic(calc, limit)

        multic(post_calc, limit)
        e = time()
        print(f"time: {e-s} seconds")

    except KeyboardInterrupt:
        raise key_error(
            "\nThe program is stopped at the stage " +
            "of switching between stages.")
