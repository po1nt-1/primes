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
    primes = list()  # создать Т (совместить с нижним)

    if (limit >= 2):
        primes.append(2)    # записать в Т: 2 true
    if (limit >= 3):
        primes.append(3)    # записать в Т: 3 true

    sieve = [False] * (limit + 1)   # создать Т (совместить с верхним)

    return sieve    # убрать


def calc(limit, sieve):  # sieve убрать
    # прочитать всё из Т
    x = 1
    while(x * x <= limit):
        y = 1
        while(y * y <= limit):
            n = (4 * x * x) + (y * y)
            if (n <= limit and (n % 12 == 1 or n % 12 == 5)):
                sieve[n] = not sieve[n]
                # прочитать из Т: n status; записать в Т: n !status

            n = (3 * x * x) + (y * y)
            if (n <= limit and n % 12 == 7):
                sieve[n] = not sieve[n]
                # прочитать из Т: n status; записать в Т: n !status

            n = (3 * x * x) - (y * y)
            if (x > 0 and x > y and n <= limit and n % 12 == 11):
                sieve[n] = not sieve[n]
                # прочитать из Т: n status; записать в Т: n !status
            y += 1
        x += 1

    return sieve  # убрать


def post_calc(limit, sieve):    # sieve убрать
    # прочитать всё из Т
    i = 5
    while(i * i <= limit):
        if sieve[i]:
            n = i * i
            for j in range(n, limit+1, n):
                sieve[j] = False
                # записать в Т: j false
        i += 1

    primes = list()          # убрать (это здесь для старого вывода)

    if (limit >= 2):         # убрать (это здесь для старого вывода)
        primes.append(2)     # убрать (это здесь для старого вывода)
    if (limit >= 3):         # убрать (это здесь для старого вывода)
        primes.append(3)     # убрать (это здесь для старого вывода)

    for elem in range(len(sieve)):  # вывод убрать
        if (sieve[elem]):
            primes.append(elem)

    return primes   # вывод убрать


def SieveOfAtkin(limit):
    # pre_calc(limit)
    # calc(limit)
    # post_calc(limit)

    return post_calc(limit, calc(limit, pre_calc(limit)))   # убрать
