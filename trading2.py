# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 16:48:54 2017

@author: Administrator
"""

from Queue import Queue  
  
import random  
  
import threading  
  
import time  

import tushare as ts

import pandas as pd

import pymysql

import datetime

from pyquery import PyQuery as pq

from sqlalchemy import create_engine 


#Producer thread  
  
class Producer(threading.Thread):  
  
    def __init__(self, t_name, queue):  
  
        threading.Thread.__init__(self, name=t_name)  
               
        self.codes=pd.read_table(u'E:\\work\\股票数据\\股票细分板块\\allboard.txt',usecols=[0],header=0,names=['code'],dtype=str)['code'].apply(lambda x: x < '600000' and 's_sz'+x or 's_sh'+x)   
        
        self.data=queue  
             
    def run(self):
        
        self.getInfo()       
  
        print "%s: %s finished!" %(time.ctime(), self.getName())  
                
    def getInfo(self):
        
        while 1:  
            
            codesum=len(self.codes)
            
            #分多少组去取
            tupleCapacity=60
            tuplenum= (float(codesum)/tupleCapacity)>(codesum/tupleCapacity) and (codesum/tupleCapacity)+1 or (codesum/tupleCapacity)
            
            header=['code','hq_name','hq_code','hq_close','chg','chgper','hq_vol','hq_amo','blank','mkt']
            
            df=pd.DataFrame()
            
            for i in xrange(tuplenum):
                if i == tuplenum-1:
                    tmpcodes=self.codes[i*tupleCapacity:]
                else:      
                    tmpcodes=self.codes[i*tupleCapacity:(i+1)*tupleCapacity]
                    
                codestr=''
                for code in tmpcodes:
                    if code == self.codes.iat[-1]:
                        codestr+=code       
                    else:
                        codestr+=code+','            
                
                df=df.append(pd.DataFrame(map(lambda x:x.split('~'),pq('http://qt.gtimg.cn/q='+codestr).text().split(';')),columns=header))
            
            df=df.drop(['hq_vol','hq_amo','blank','mkt','chg','chgper','code'],axis=1).dropna()
            
            df.set_index('hq_code',inplace=True)
            
            fmt='%Y-%m-%d %H:%M'
            now=datetime.datetime.now()
            nowstr=now.strftime(fmt)
            thetime=nowstr.split()[-1]
         
            df['date_time']=nowstr
                                            
            cond1=(thetime >= '09:30' and thetime <= '11:30')
            
            cond2=(thetime >= '13:00' and thetime <= '15:00')
            
            if cond1 or cond2:

                self.data.put(df)  
                
                print 'get '+str(now)  
                
  
class Consumer(threading.Thread):  
  
    def __init__(self,t_name, queue,sdate):  
  
        threading.Thread.__init__(self, name=t_name)  
  
        self.data=queue  
        
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        self.series15min=list(pd.read_csv(u'E:\\work\\股票数据\\时间序列\\15min.csv')['time'])
        
        self.series60min=list(pd.read_csv(u'E:\\work\\股票数据\\时间序列\\60min.csv')['time'])
        
        self.testFile='E:/test/buy.txt'
        
        self.stocBoardFile=u'E:\\work\\股票数据\\股票细分板块\\allboard.txt'
        
        self.sdate=sdate
        
                   
    def run(self):         
        self.putInfo()
        print "%s: %s finished!" %(time.ctime(), self.getName())
        
    def getPrice(self,df,df_mould):
             
        highindexs=df_mould[df.hq_close>df_mould.hq_high].index    
        for index in highindexs:    
            df_mould.loc[index,'hq_high']=df.loc[index,'hq_close']
            
        lowindexs=df_mould[df.hq_close<df_mould.hq_low].index
        for index in lowindexs:       
            df_mould.loc[index,'hq_low']=df.loc[index,'hq_close']  
            
        df_mould['hq_close']=df['hq_close']            
            
        return df_mould
        
    def getUint(self,df,df_mould):
        
        df_tmp=df_mould.copy()
        
        df_tmp['date_time']=df['date_time']
        
        df_tmp.reset_index(inplace=True)
        
        #df_tmp.date_time=pd.to_datetime(df_tmp.date_time)
      
#        df_tmp.set_index(df_tmp['date_time'],inplace=True,drop=False)
#        
#        df_tmp.drop('date_time',axis=1,inplace=True)
        
        return df_tmp
        

    def getMacd(self,df,times=1):
        
        ema_short=df.groupby('hq_code').apply(lambda x :x['hq_close'].ewm(span=12*times).mean())
        
        ema_long=df.groupby('hq_code').apply(lambda x :x['hq_close'].ewm(span=26*times).mean())
        
        dif=ema_short-ema_long
        
        dea=dif.groupby(level=0).apply(lambda x :x.ewm(span=9).mean())
        
        macd=((dif-dea)*2).reset_index(level=0)
        
        return macd['hq_close']
            
        
    def updateDf60Macd(self,df_60):
        
        df_60.reset_index(inplace=True,drop=True)
        
        df_60['macd_60']=self.getMacd(df_60)
        
        df_60['macd_day']=self.getMacd(df_60,4)
        
        df_60['macd_week']=self.getMacd(df_60,20)
              
        return df_60
        
        
    def initializeDf60(self):
        
        df_60Sql='SELECT hstockquotationsixty.index,hq_code,hq_open,hq_high,hq_low,hq_close FROM hstockquotationsixty where hstockquotationsixty.index >= "'+self.sdate+'"'
        
        df_60=pd.read_sql(df_60Sql,con=self.engine,index_col='hq_code')
        
        print df_60Sql  
        
        df_60.rename(columns={'index':'date_time'},inplace=True)
        
        df_60['date_time']=df_60['date_time'].astype(str)        
        
        stockNames=pd.read_table(self.stocBoardFile,usecols=[0,1],index_col=0,encoding='utf-8')
        
        df_60['hq_name']=stockNames[u'名称']       
        
        df_60.reset_index(inplace=True)
        
        print 'df_60初始化完毕...'
                     
        return df_60        
        

    def macdLastBarTest(self,df_60):
        
        def macdStrategy(df_60_tmp):
            
            f=open(self.testFile,'a')
       
            lastRow=df_60_tmp.iloc[-1]      
            
            code=lastRow.loc['hq_code']
            
            date_time=lastRow.loc['date_time']
                  
            if lastRow.loc['macd_week']>0:
            
                if lastRow.loc['macd_day']>0: 
                    
                    if True in ((df_60_tmp.iloc[-4:]['macd_60']<0).tolist()):
                        
                        if lastRow.loc['macd_60']>0:
                            
                            buy='买'+str(code)+' '+date_time+'\n'
    
                            f.write(buy)
        
                            print buy      
    
            f.close()
        
        t1=time.time()
        df_60.groupby('hq_code').apply(macdStrategy)
        print time.time()-t1

     
    def putInfo(self):    
        
        df_60=self.initializeDf60()
       
        df1=pd.DataFrame()
        
        df15=pd.DataFrame()

        df60=pd.DataFrame()        

        df= self.data.get(timeout=40)
        
        startTime=df.iloc[0]['date_time']
        
        df_mould=df
        
        df_mould['hq_high']=df_mould['hq_low']=df_mould['hq_open']=df_mould['hq_close']
        
        df_mould15=df_mould.copy()
        
        df_mould60=df_mould15.copy()
                
        while 1:
            
            df= self.data.get(timeout=6000)
            
            theTime=df.iloc[0]['date_time']
            
            df_mould=self.getPrice(df,df_mould)
            
            df_mould15=self.getPrice(df,df_mould15)
            
            df_mould60=self.getPrice(df,df_mould60)
            
            if theTime != startTime:
                
                df_tmp=self.getUint(df,df_mould)
                  
                df1=df1.append(df_tmp)
                
                df1.to_csv(r'E:\test\1min.csv',encoding='gbk',index=None)
                
                print 'create 1min '+theTime
                
                df_mould['hq_open']=df_mould['hq_close']                
                
                startTime=theTime    
                
                theTime=theTime.split()[-1]
            
                if  theTime== '09:45':
                    theTime = '9:45'
                    
                if  theTime in self.series15min:
                    
                    t1=time.time()
                    
                    df_tmp=self.getUint(df,df_mould15)            
                          
                    df15=df15.append(df_tmp)
                                            
                    df15.to_csv(r'E:\test\15min.csv',encoding='gbk',index=None)   
                    
                    print 'create 15min '+theTime+' '+str(time.time()-t1)
                    
                    df_mould15['hq_open']=df_mould15['hq_close']                    
                    
                if  theTime in self.series60min:
                    
                    t1=time.time()
                    
                    df_tmp=self.getUint(df,df_mould60)
                          
                    df60=df60.append(df_tmp)
                    
                    df_60=df_60.append(df1)
                    
                    df_60=self.updateDf60Macd(df_60)
                    
                    self.macdLastBarTest(df_60)                    
                                    
                    df60.to_csv(r'E:\test\60min.csv',encoding='gbk',index=None)   
#                    
                    print 'create 60min '+theTime+' '+str(time.time()-t1)                
                    
                    df_mould60['hq_open']=df_mould60['hq_close']
                                   
            time.sleep(random.randrange(4))  
            
            
if __name__ == '__main__':  
  
    queue = Queue()  
  
    producer = Producer('Pro.', queue)  
  
    consumer = Consumer('Con.', queue,'2017-06-01')  
  
    producer.start()  
  
    consumer.start()  
  
    producer.join()  
  
    consumer.join()  
  
    print 'All threads terminate!'  