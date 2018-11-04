#!/usr/bin/env python

import sys

def main(argv):
    results = {}

    for line in sys.stdin:
        line = line.strip()

        vowels, count = line.split('\t')
        count = int(count)

        if vowels in results.keys():
            results[vowels] += count
        else:
            results[vowels] = count

    for k,v in results.items():
        print('%s:%s' % (k.replace('_',''),v))

if __name__ == "__main__":
    main(sys.argv)