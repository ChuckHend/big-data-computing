def main():
    # get user input
    lower, upper = parse_input()

    # find the prime numbers
    primes = find_primes(lower, upper)

    # format output
    output = format_output(primes)
    
    # print the output
    print(output)

def parse_input():
    first = int(input('Enter the first number: '))
    second = int(input('Enter the second number: '))

    inputs = [first, second]
    inputs.sort()

    return inputs[0], inputs[1]  


def find_primes(lower, upper):
    # the numbers we want to test if prime
    test_range = [x for x in range(lower + 1, upper)]

    # prime numbers must be > 1, so limit our list to those vals
    test_range = [x for x in test_range if x > 1]

    # we will assume they are prime
    # remove them from the test_range of they are NOT prime

    primes = test_range.copy()

    for x in test_range:
        # create the list of denominators, should be all ints <x, except for 1
        divs = [x for x in range(2,x)]

        for div in divs:
            if x % div == 0:
                primes.remove(x)
                break

    return primes

def format_output(primes):
    n = len(primes)

    if not primes:
        output = 'No Primes'

    elif n == 1:
        output = primes[0]

    else:
        seps = [':', '!', ',']
        
        # built sep list
        k = len(seps)
        sep_list = seps*(int(n/k))
        sep_list = sep_list + seps[:n - len(sep_list)]

        output = ''

        for i, prime in enumerate(primes[:-1]):
            # format each element til last element
            output += str(prime) + sep_list[i]

        else:
            # last element do not format
            output += str(primes[-1])

    return output

if __name__ == "__main__":
    main()
