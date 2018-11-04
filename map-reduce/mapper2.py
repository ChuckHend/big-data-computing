#!/usr/bin/env python

import sys
import re


def vowel_count(word):
    vowels = ['a','e','i','o','u','y']
    vow_ct = {}
    for vowel in vowels:
        vow_ct[vowel] = word.lower().count(vowel)
    outstr = ''
    for k,v in vow_ct.items():
        outstr += k*v

    if outstr == '':
        outstr = '_'

    return outstr

def main(argv):
    for line in sys.stdin:

        line = re.split('\t| ', line.strip('\n'))

        for word in line:

            vow_str = vowel_count(word)
            print(vow_str+"\t"+"1")

if __name__ == "__main__":
    main(sys.argv)