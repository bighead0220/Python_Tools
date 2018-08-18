#!/usr/bin/env python3
#!coding: utf-8
import sys
import time
class COUNTING:
    def __init__(self):
        pass
    @staticmethod
    def counting_down():
        print ('每隔10秒显示倒计时...')
        minutes = float(sys.argv[5])
        seconds = int(minutes*60)
        i = 0
        while i < seconds:
            print (seconds-i)
            i += 10
            time.sleep(10)


