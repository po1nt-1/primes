import db
from multiprocessing import cpu_count, Pool, Lock


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
        obj = db.transaction()
        obj.open_db()
        obj.begin()

        obj.create_table(limit)
        if (limit >= 2):
            obj.update(2, True)
        if (limit >= 3):
            obj.update(3, True)

        obj.commit()
        obj.close_db()
        del obj
    except KeyboardInterrupt:
        try:
            obj.commit()
        except db.sqlite3.OperationalError:
            pass
        obj.close_db()
        del obj
        raise key_error("\nThe program stopped while creating the table.")


def calc(data: tuple):
    lock = Lock()
    try:
        obj = db.transaction()
        obj.open_db()
        obj.begin()

        end, start, step = data
        print(f"процесс в функции calc, start={start}")

        x = start
        limit = end
        while(x * x <= limit):
            if x % 1000 == 0:   # попытка разбить одну транзакцию на несколько
                print("транзакция перезагрузилась (", end='')
                obj.commit()
                del obj
                obj = db.transaction()
                obj.open_db()
                obj.begin()
                print(" )")

            print(
                f"\tfor(int x={start}; x <= {end}; " +
                f"x += {step}) // текущий x = {x}")
            y = 1
            while(y * y <= limit):
                n = (4 * x * x) + (y * y)
                if (n <= limit and (n % 12 == 1 or n % 12 == 5)):
                    obj.update(n, not obj.info(n))

                n = (3 * x * x) + (y * y)
                if (n <= limit and n % 12 == 7):
                    obj.update(n, not obj.info(n))

                n = (3 * x * x) - (y * y)
                if (x > 0 and x > y and n <= limit and n % 12 == 11):
                    obj.update(n, not obj.info(n))

                y += 1

            x += step

        obj.commit()
        obj.close_db()
        del obj
    except KeyboardInterrupt:
        try:
            obj.commit()
        except db.sqlite3.OperationalError:
            pass
        obj.close_db()
        del obj
        raise key_error(
            "\nThe program is stopped during the main stage of calculations.")


def post_calc(end: int, start: int = 5):
    print("in post_calc")
    try:
        obj = db.transaction()
        obj.open_db()
        obj.begin()

        i = start
        limit = end
        while(i * i <= limit):
            if obj.info(i):
                n = i * i
                for j in range(n, limit+1, n):
                    obj.update(j, False)
            i += 1

        obj.commit()
        obj.close_db()
        del obj
    except KeyboardInterrupt:
        try:
            obj.commit()
        except db.sqlite3.OperationalError:
            pass
        obj.close_db()
        del obj
        raise key_error(
            "\nThe program is stopped during the final " +
            "stage of the calculation.")


def SieveOfAtkin(limit):
    try:
        pre_calc(limit)

        step = process_count() - 1
        # step = 2    # можно жестко установить количество процессов
        print("process_count for program: ", step)

        if limit < 50 or step < 2:
            calc((limit, 1, 1))
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

            print("создание процессов (до b, от a, шаг c):\n", args)
            pool.map(calc, args)

            pool.close()
            pool.join()

        post_calc(limit)
    except KeyboardInterrupt:
        raise key_error(
            "\nThe program is stopped at the stage " +
            "of switching between stages.")
