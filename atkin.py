import db
import multiprocessing as mp
from multiprocessing import Pool


def process_count() -> int:
    cpu_count = mp.cpu_count()
    if cpu_count >= 6:
        processes_num = cpu_count - 2
    elif cpu_count > 2 and cpu_count < 6:
        processes_num = cpu_count - 1
    else:
        processes_num = 2
    return processes_num


def pre_calc(limit):
    db.begin()
    db.create_table(limit)
    if (limit >= 2):
        db.update(2, True)
    if (limit >= 3):
        db.update(3, True)
    db.commit()


def calc(data: tuple):
    end, start, step = data
    print(f"в функции calc, start={start}")

    x = start
    limit = end
    while(x * x <= limit):
        print(f"[{x}, {end}]")
        y = 1
        while(y * y <= limit):
            n = (4 * x * x) + (y * y)
            if (n <= limit and (n % 12 == 1 or n % 12 == 5)):
                checker1 = db.info(n)
                if checker1 is None:
                    raise Exception(
                        "Error1 in atkin.calc(): Number does not exist!")
                else:
                    db.update(n, not checker1)

            n = (3 * x * x) + (y * y)
            if (n <= limit and n % 12 == 7):
                checker2 = db.info(n)
                if checker2 is None:
                    raise Exception(
                        "Error2 in atkin.calc(): Number does not exist!")
                else:
                    db.update(n, not checker2)

            n = (3 * x * x) - (y * y)
            if (x > 0 and x > y and n <= limit and n % 12 == 11):
                checker3 = db.info(n)
                if checker3 is None:
                    raise Exception(
                        "Error3 in atkin.calc(): Number does not exist!")
                else:
                    db.update(n, not checker3)
            y += 1
        x += step


def post_calc(end: int, start: int = 5):
    db.begin()
    i = start
    limit = end
    while(i * i <= limit):
        if db.info(i):
            n = i * i
            for j in range(n, limit+1, n):
                db.update(j, False)
        i += 1
    db.commit()


def _kill():
    try:
        db.commit()
    except db.sqlite3.OperationalError:
        pass
    db.close_db()
    print("\nCalculations interrupted")


def SieveOfAtkin(limit):
    db.open_db()
    pre_calc(limit)

    db.begin()
    step = process_count() - 1
    step = 1
    print("process_count for program", step)

    if limit < step:
        calc((limit, 1, 1))
    elif step < 2:
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

        print(args)
        pool.map(calc, args)    # тут краш

        pool.close()
        pool.join()
    db.commit()

    post_calc(limit)
    db.close_db()
