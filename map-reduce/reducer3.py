#!/usr/bin/env python



# Might = connection of 2-3 current connections
# Probably = connection of >= 4 current connections
# Not = connection < 2 current connections

import sys

def main(argv):

    shared_conns = {}
    for line in sys.stdin:
        person, common = line.strip('\n').split('\t')
        comm, val = common.split(',')
        if person in shared_conns.keys():
            if comm in shared_conns[person].keys():
                shared_conns[person][comm] += int(val)
            else:
                shared_conns[person][comm] = int(val)
        else:
            shared_conns[person] = {person : int(val)}

    for key, values in shared_conns.items():
        #print(key, values)
        p = []
        m = []
        for k, v in values.items():
            if v >= 4:
                p.append(k)
            elif v > 1 and v < 4:
                m.append(k)

        p.sort(key=int)

        m.sort(key=int)
        if len(p) > 0:
            p = "Probably(%s)" % ','.join(p)
        else:
            p = ''
        if len(m) > 0:
            m = "Might(%s)" % ','.join(m)
        else:
            m = ''
        print('%s:%s %s' % (key, m, p))
        

if __name__ == "__main__":
    main(sys.argv)