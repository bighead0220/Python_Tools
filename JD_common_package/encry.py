#!/usr/bin/env python2
import sys
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
length=16
def encry(password,number):
    key=password
    for line in sys.stdin:
        times=0
        encrypt_list=[]
        line=line.strip()
        line_list=line.split('\t')
        while int(times) <  int(number):
            encrypt_column=line_list[times]
            count = len(encrypt_column)
            if count < length:
                encrypt_column = encrypt_column + (length - count) * '\0'  # \0=backspace
            if count > length:
                encrypt_column = encrypt_column + (length - (count % length)) * '\0'
            encrypt_list.append(encrypt_column)
            times+=1
        other_column = line_list[times:]
        obj=AES.new(key,AES.MODE_CBC,'This is an IV456')
        encry_result=[]
        for encrypt_ele in encrypt_list:
            encrypt_text=obj.encrypt(encrypt_ele)
            encrypt_text=b2a_hex(encrypt_text)
            encry_result.append(encrypt_text)
        final=encry_result+other_column
        print '\t'.join(final)

if __name__=='__main__':
    encry(sys.argv[1],sys.argv[2])
