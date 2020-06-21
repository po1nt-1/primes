import db
import multiprocessing as mp


def process_control():
    cpu_count = mp.cpu_count()
    if cpu_count >= 6:
        process_count = cpu_count - 2
    elif cpu_count > 2 and cpu_count < 6:
        process_count = cpu_count - 1
    else:
        process_count = 2


def pre_calc(limit):
    db.create_table(limit)

    if (limit >= 2):
        db.update(2, True)
    if (limit >= 3):
        db.update(3, True)


def calc(limit):
    x = 1
    while(x * x <= limit):
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
        x += 1


def post_calc(limit):
    i = 5
    while(i * i <= limit):
        if db.info(i):
            n = i * i
            for j in range(n, limit+1, n):
                db.update(j, False)
        i += 1


def SieveOfAtkin(limit):
    # pre_calc(limit)
    # calc(limit)
    # post_calc(limit)
    db.open_db()
    pre_calc(limit)
    calc(limit)
    post_calc(limit)
    db.close_db()
