#!/usr/bin/env python2
import sys
from Crypto.Cipher import AES
from binascii import b2a_hex,a2b_hex
length=16
def decry(password,number):
    key=password
    for line in sys.stdin:
        times=0
        decrypt_list=[]
        line=line.strip()
        line_list=line.split('\t')
        while int(times)<int(number):
            decrypt_column=line_list[times]
            decrypt_list.append(decrypt_column)
            times+=1
        other_column = line_list[times:]
        obj=AES.new(key,AES.MODE_CBC,'This is an IV456')
        decry_result=[]
        for decrypt_ele in decrypt_list:
            decrypt_ele=decrypt_ele.strip()
            decrypt_text=obj.decrypt(a2b_hex(decrypt_ele))
            decry_result.append(decrypt_text)
        final=decry_result+other_column
        print '\t'.join(final)

if __name__=='__main__':
    decry(sys.argv[1],sys.argv[2])
