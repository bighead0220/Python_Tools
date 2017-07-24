#!/usr/bin/env python3

import calendar
from datetime import timedelta
import datetime

class Calendar:

    def __init__( self, dateTime ):
        self.dateTime = dateTime       #以下获取的时间点都以此时间为基准
        self.Year = dateTime.year
        self.Month = dateTime.month
        self.Quarter = [range(1, 4), range(4, 7), range(7, 10), range(10, 13)]

   
    def setMonth( self, Index=-1 ):
        start =  (int)((str)(self.Year) + (str)(self.Month))
        if Index < 0:
            if self.Month < abs(Index):
                self.Month = 12 + Index + self.Month
                self.Year -= 1
            elif self.Month == abs(Index):
                self.Month = 12
                self.Year -= 1
            else:
                self.Month += Index 
        elif Index >0:
            if self.Month + Index > 12:
                self.Month = self.Month + Index -12
                self.Year += 1
            else:
                self.Month += Index
        else:
            pass
        try:
            self.dateTime = datetime.datetime(self.Year, self.Month, self.dateTime.day)
        except ValueError:
            try:
                self.dateTime = datetime.datetime(self.Year, self.Month, self.dateTime.day-1)
            except ValueError:
                self.dateTime = datetime.datetime(self.Year, self.Month, self.dateTime.day-3) 
        days = []
        end = int((str)(self.Year) + (str)(self.Month))
        Count = range(min(start, end), max(start, end))
        Count = [(str(i)[:4], str(i)[4:]) for i in Count if str(i)[4:] <= '12' and str(i)[4:] > '00']
        for i in Count:
            days.append(len(calendar.month(int(i[0]), int(i[1])).split()[9:]))
        self.dateTime = self.dateTime + timedelta(days = sum(days) )

    
    def setWeek( self, Index=-1 ):
        days = Index*7
        self.dateTime = self.dateTime + timedelta(days = days )
        self.Year = self.dateTime.year
        self.Month = self.dateTime.month
 
    #def getYesterday( self, sep = '-' ):
    #    return ( self.dateTime - timedelta(days = 1 ) ).strftime( '%Y{0}%m{0}%d'.format( sep ) )

    def getYesterday( self, sep = '-' ):
        return self.dateTime.date().strftime( '%Y{0}%m{0}%d'.format( sep ) )
            
    def getMonthFirst( self, sep = '-' ):
        return sep.join( ( (str)(self.Year), '%02d' % self.Month, '01' ) )

    def getMonthLast( self, sep = '-' ):
        return sep.join( ( (str)(self.Year), '%02d' % self.Month, calendar.month( self.Year, self.Month ).split()[-1] ) )

    def getWeekLast( self, sep = '-', ws = 0 ):
        return ( self.dateTime + timedelta( days = ( 6 - ( self.dateTime.weekday() + ws ) ) ) ).strftime( '%Y{0}%m{0}%d'.format( sep ) )

    def getWeekFirst( self, sep = '-', ws=0 ):
        return ( self.dateTime - timedelta( days = ( self.dateTime.weekday() + ws ) ) ).strftime( '%Y{0}%m{0}%d'.format( sep ) )

    def getYearWeek(self):
        year=self.dateTime.year
        wd=calendar.weekday(year,1,1)
        days=(self.dateTime-datetime.date(year,1,1)).days
        nweek=0
        if wd:
            nweek=(days+wd)/7
        else:
            nweek=days/7+1
        if( nweek==0 ):
            nweek = 52
            year -= 1
        return int(nweek)+1

    def getYearMonth(self):
        return self.Month

    def getYearQuarter(self):
        for i in range(len(self.Quarter)):
            if self.getYearMonth() in self.Quarter[i]:
                return i+1

    def setNMonth(self, n): 
        if n not in range(1, 13):
            raise Exception('错误的Nth')
        self.dateTime = datetime.datetime(self.Year, n, self.dateTime.day)
        self.Year = self.dateTime.year
        self.Month = self.dateTime.month

    def setNWeek(self, n):
        if n <= 0:
            raise Exception('错误的Nth')
        days = (n-1)*7
        self.dateTime = datetime.datetime(self.Year, 1, 1) + timedelta(days = days )
        self.Year = self.dateTime.year
        self.Month = self.dateTime.month

    def setNQuarter(self, n):
        if n not in [1,2,3,4]:
            raise Exception('错误的Nth')
        self.dateTime = datetime.datetime(self.Year, self.Quarter[n-1][0], 1)
        self.Year = self.dateTime.year
        self.Month = self.dateTime.month         
    
    def getQuarterFirst(self, sep='-'):
        for i in range(len(self.Quarter)):
            if self.Month in self.Quarter[i]:
                return sep.join( ( (str)(self.Year), '%02d' % self.Quarter[i][0], '01' ) ) 

    def getQuarterLast(self, sep='-'):
        for i in range(len(self.Quarter)):
            if self.Month in self.Quarter[i]:
                return sep.join( ( (str)(self.Year), '%02d' % self.Quarter[i][-1], calendar.month( self.Year, self.Quarter[i][-1] ).split()[-1] ) )

if __name__ == "__main__":
    a = Calendar(datetime.datetime.now())
    a.setNQuarter(1)
    print (a.getQuarterFirst())
    print (a.getQuarterLast())
