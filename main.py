import sys
import atkin


if __name__ == "__main__":
    try:
        args = sys.argv
        if len(args) == 2:
            if args[1].isnumeric():
                if int(args[1]) > 0:
                    if int(args[1]) > 250000000:
                        print("Too many, wait...")
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
        atkin.SieveOfAtkin(limit)
    except atkin.key_error as e:
        print(str(e))
        sys.exit()
