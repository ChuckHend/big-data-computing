import sys

def main():

    for line in sys.stdin:
        ints = [int(x) for x in line.split()]

    sigma = 0
    n = len(ints)
    
    for x in ints:
        sigma += x
    
    print(sigma/n)
    
if __name__ == "__main__":
    main()
    
