#!/usr/bin/env python

import sys

def main(argv):

    '''
    this dictionary will store total spend for each customer 
    for each month/country combination
    '''
    results = {}

    for line in sys.stdin:
        line = line.split('\t')

        vals = line[1].split(',')

        # key
        mo_co = line[0]

        # values
        custID = vals[0]
        spend = float(vals[1])

        if mo_co in results.keys():
            if custID in results[mo_co].keys():
                results[mo_co][custID] += spend
            else:
                results[mo_co][custID] = spend
        else:
            results[mo_co] = {custID : spend}

    '''
    now determine max customer(s) for each key in the results dictionary
    format and print results along the way
    '''
    key_maxes = {}
    for key, vals in results.items():
        # get max value
        maxVal = results[key][str(max(vals, key=vals.get))]
        key_maxes[key] = {k: v for (k,v) in vals.items() if v == maxVal}
        ids = ''.join(list(key_maxes[key].keys()))
        print('%s:%s' % (key, ids))


if __name__ == "__main__":
    main(sys.argv)