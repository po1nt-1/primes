import atkin
import sys
import time


if __name__ == "__main__":
    try:
        args = sys.argv
        if len(args) == 2:
            if args[1].isnumeric():
                if int(args[1]) > 0:
                    limit = int(args[1])
                else:
                    print(f"Error: The input parameter must " +
                          "be a natural number")
                    sys.exit()
            else:
                print(f"Error: The input parameter must be a natural number")
                sys.exit()
        else:
            print("Error: Need one input parameter")
            sys.exit()
    except KeyboardInterrupt:
        print("\nThe program stopped at the stage of recognition of " +
              "input parameters.")
        sys.exit()

    try:
        start = time.time()
        atkin.SieveOfAtkin(limit)
        total = int((time.time() - start) * 100) / 100
        if total > 60:
            print(f"\nTime spent: {total // 60} minutes.")
        else:
            print(f"\nTime spent: {total} seconds.")
    except atkin.key_error as e:
        print(str(e))
        sys.exit()
