#!/usr/bin/env python

import sys
import itertools

def main(argv):
    
    for line in sys.stdin:
        line = line.strip('\n')
        pers = line.split(':')[0].strip()
        conns = line.split(':')[1].split()

        # print all the direct connection pairs
        for con in conns:
            out = [int(pers), int(con)]
            out.sort(key=int)
            print('%s\t%s,-999999999' % (out[0],out[1]))
        
        # create pairs for all the shared connections and who they are connected to
        pairs = list(itertools.permutations(conns, 2 ))
        
        for pair in pairs:
            #pair = [int(x) for x in pair]
            #pair.sort()
            #pair = [str(x) for x in pair]

            print('%s\t%s,1' % (pair[0], pair[1]))

if __name__ == "__main__":
    main(sys.argv)