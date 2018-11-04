#!/usr/bin/env python

import sys

def main(argv):
    for line in sys.stdin:

        # split each row to a list using comma delimiter
        lineSplit = line.split(',')

        custID = lineSplit[6]
        invoiceID = lineSplit[0]

        # skip records where 'invoice starts w/ C' OR 'no custID' OR 'it is the header'
        if (invoiceID[0].upper() == 'C') or (custID in ['',' ', None]) or (invoiceID[-1].isalpha()):
            continue # move on to next line in file
        else:
            month = int(lineSplit[4].split('/')[0])
            country = lineSplit[7].strip()
            spend = float(lineSplit[3]) * float(lineSplit[5])

            # format to month,country \t customer, total spend

            printStr = '%02d,%s\t%s,%s' % (month, country, custID, spend)
            print(printStr)

if __name__ == "__main__":
    main(sys.argv)