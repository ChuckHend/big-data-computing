def factorial(val):
    if val < 0:
        return -1
    p = 1
    val_list = [x for x in range(1, val + 1)]
    for x in val_list:
        p *= x
    return p

def main():
    user_in = int(-1)
    while user_in < 0:
        user_in = int(input('Enter an integer: '))
    result = factorial(user_in)
    print(result)

if __name__ == "__main__":
    main()