# -*- coding: utf-8 -*-
"""
Created on Thu Jun 01 20:20:18 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys) 

from DataHanle.MktDataHandle import MktIndexHandle

from sqlalchemy import create_engine 

import datetime

import pandas as pd

import numpy as np
 


from Pic import picZf
from Pic import picZqZf

class ZF():
    
    def __init__(self):
        
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        #得到200标的股票代码    
        self.code200=self.getCode()
        
        #定义时间周期        
        self.tstartdate=datetime.datetime(2017,6,9)

        self.tenddate=datetime.datetime(2017,6,12)       
        
               
    def getCode(self):
  
        #代码文件目录
        fname=r'E:\test\code\code.txt'
        
        s1=pd.read_table(fname,usecols=[0],names=['code'],dtype=str)
                
        codelist=s1['code'].astype('int')
        
        return codelist    
        
    def calculate(self):
            
        def xdChg(x,indexchg):   
            try:
                indexchg.index=x.index
                x=x-indexchg
                return x    
            except:
                print x.name
                print '数据长度有误'
                
        def zhouqi(x):
            closeList=x['hq_close'].tolist()
            _open=closeList[0]
            _close=closeList[-1]
            diff=_close-_open
            try:
                chgper=diff/_open*100
                x['chgper']=chgper
                return x[-2:-1]
            except:
                print x
        
        def paixu(x):
            x=x.sort_values(by=['chgper'], ascending=[0])
            index=np.arange(1,len(x)+1)
            x['paiming']=index
            return x
                
        mktindex = MktIndexHandle()
        
        mktstock = mktindex.mktstock
        
        sdate=self.tstartdate+datetime.timedelta(1)
        sdate=sdate.strftime("%Y-%m-%d")
        edate=self.tenddate.strftime("%Y-%m-%d")
        
        period=sdate+'至'+edate
        
        #获取板块股票关联信息
        
        stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        stockrelated['cFlag']=stockrelated['board_name'].str.contains(u'通达信行业-')
        
        stockrelated=stockrelated[stockrelated['cFlag']]
        
        stockrelated['board_name']=stockrelated['board_name'].apply(lambda x:x.replace(u'通达信行业-',''))
        
        
        #获取代码对应股票名称
        stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name']) 
        
        #indexNames=pd.read_sql_table('boardindexbaseinfo',con=self.engine,schema='stocksystem',index_col='board_code',columns=['board_code','board_name']) 
        
        
        #获取每日数据
        df_stock = mktstock.MkStockBarHistOneDayGet('',self.tstartdate,self.tenddate)
        
        df_index = mktindex.MktIndexBarHistDataGet('>399317',self.tstartdate,self.tenddate,"D")
        
        df_dapan = mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        df_dapan.index=df_dapan['index']
        
        #拼接板块的代码与名称
#        df_index.index=df_index['hq_code']
#        df_index['hq_name']=indexNames['board_name']
#        df_index['hq_name']=df_index['hq_name'].apply(lambda x:x[6:])
        
        #计算股票每日涨幅
        df_stock['chg'] = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
        df_stock['preclose']=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
        df_stock['chgper']=df_stock['chg']*100/df_stock['preclose']
        df_stock.index=df_stock['index']
               
        #计算指数每日涨幅
        df_index['chg'] = df_index['hq_close'].groupby(df_index['hq_code']).diff()
        df_index['preclose']=df_index['hq_close'].groupby(df_index['hq_code']).shift()
        df_index['chgper']=df_index['chg']*100/df_index['preclose']
        #df_index.index=np.arange(len())
         
        #计算大盘每日涨幅   
        daPanChg = df_dapan['hq_close'].diff()
        daPanPreCLose=df_dapan['hq_close'].shift()
        daPanChgper=daPanChg*100/daPanPreCLose
        
        #计算股票周期涨幅
        zq_stock=df_stock.groupby('hq_code').apply(zhouqi)
        
        #计算指数周期涨幅
        zq_index = df_index.groupby('hq_code').apply(zhouqi)
        
        #计算大盘周期涨幅
        daPanCloseList=df_dapan['hq_close'].tolist()
        daPanDiff=daPanCloseList[-1]-daPanCloseList[0]
        daPanZqChgper=daPanDiff/daPanCloseList[0]*100
        
        length=len(daPanChg)
        
        #按交易日期补全数据，停牌涨幅设为-100
        stockGrouped=df_stock[['hq_code','chgper','hq_date']].groupby(df_stock['hq_code'])
        
        df_stock=pd.DataFrame()
        
        for code,data in stockGrouped:
            if len(data)!=length:       
                data=data.reindex(df_dapan.index)
                data['hq_code'].fillna(method='bfill',inplace=True)     
                data['hq_code'].fillna(method='ffill',inplace=True)        
                data['chgper'].fillna(-100,inplace=True)       
            df_stock=df_stock.append(data)
        
        #计算每日相对涨幅
        df_stock['xdchgper']=df_stock['chgper'].groupby(df_stock['hq_code']).apply(lambda x: xdChg(x,daPanChgper))
        
        #按股票涨幅排序
        df_stock=df_stock.groupby(df_stock['hq_date']).apply(paixu)
        
        df_index['xdchgper']=df_index['chgper'].groupby(df_index['hq_code']).apply(lambda x: xdChg(x,daPanChgper))
        
        #计算周期相对涨幅
        zq_stock['xdchgper']=zq_stock['chgper']-daPanZqChgper
        
        zq_index['xdchgper']=zq_index['chgper']-daPanZqChgper    
             
        #拼接股票的代码与名称
        df_stock.index=df_stock['hq_code']
        df_stock['hq_name']=stockNames['stock_name']
        zq_stock.index=zq_stock.hq_code
        zq_stock['hq_name']=stockNames['stock_name']
        
        #得到股票所属板块
        df_stock.index=df_stock.hq_code
        stockrelated.index=stockrelated.stock_id
        df_stock['board_name']=stockrelated['board_name']
        zq_stock['board_name']=stockrelated['board_name']
            
        #计算周期涨幅排序，并筛选数据 
        zq_stock=zq_stock.sort_values(by=['chgper'], ascending=[0])
        index=np.arange(1,len(zq_stock)+1)
        zq_stock['paiming']=index        
        
        
        #找到200标的
        df_stock['cFlag']=df_stock['hq_code'].isin(self.code200)        
        df_stock200=df_stock[df_stock['cFlag']] 
        
        zq_stock['cFlag']=zq_stock['hq_code'].isin(self.code200)  
        zq_stock200=zq_stock[zq_stock['cFlag']]     
              
        #筛选数据
        df_stock=df_stock[['hq_name','hq_date','chgper','xdchgper','board_name','paiming']].dropna()
        df_stock200=df_stock200[['hq_name','hq_date','chgper','xdchgper','board_name','paiming']].dropna()
                
        df_index=df_index[['hq_name','hq_date','chgper','xdchgper']].dropna()
  
        #定义涨幅与相对涨幅列表
        indexChg=[]
        stockChg=[]
        stock200Chg=[]
        timeList=[]
                
        #计算每日排名，加入数据列表
        indexGrouped=df_index.groupby('hq_date')
        for date,data in indexGrouped:
            timeList.append(date.strftime('%Y-%m-%d'))
            chg=data.sort_values(by=['chgper'], ascending=[0])
            indexChg.append(chg)
#            xdchg=data.sort_values(by=['xdchgper'], ascending=[0])
#            indexChg[1].append(xdchg)
                       
        stockGrouped=df_stock.groupby('hq_date')
        for date,data in stockGrouped:
            #stockchg=data.sort_values(by=['chgper'], ascending=[0])
            stockChg.append(data)
#            stockxdchg=data.sort_values(by=['xdchgper'], ascending=[0])
#            stockChg[1].append(stockxdchg)
        
        stock200Grouped=df_stock200.groupby('hq_date')
        for date,data in stock200Grouped:
            #stock200chg=data.sort_values(by=['chgper'], ascending=[0])    
            stock200Chg.append(data)
#            stock200xdchg=data.sort_values(by=['xdchgper'], ascending=[0])
#            stock200Chg[1].append(stock200xdchg)
            
        
        #计算周期排名
        zq_stock=zq_stock[['hq_name','chgper','xdchgper','board_name','paiming']].dropna()
        #zqSXdChg=zq_stock[['hq_name','xdchgper','board_name']].sort_values(by=['xdchgper'], ascending=[0])
        #zqStockChg.append(zqSChg)
       # zqStockChg.append(zqSXdChg)
        
        
        zq_stock200=zq_stock200[['hq_name','chgper','xdchgper','board_name','paiming']].dropna()  
        #zqS200XdChg=zq_stock200[['hq_name','xdchgper','board_name']].sort_values(by=['xdchgper'], ascending=[0])
        #zqStock200Chg.append(zqS200Chg)
       # zqStock200Chg.append(zqS200XdChg)
          
        zq_index=zq_index[['hq_name','chgper','xdchgper']].sort_values(by=['chgper'], ascending=[0]) 
       # zqIXdChg=zq_index[['hq_name','xdchgper']].sort_values(by=['xdchgper'], ascending=[0])
        #zqIndexChg.append(zqIChg)
        #zqIndexChg.append(zqIXdChg)
  
        #画图
        p=picZf.picExcel(period,timeList,indexChg,stockChg,stock200Chg,stockrelated)
        p.picModel()
        #p.update()
        
        p2=picZqZf.picExcel(period,zq_index,zq_stock,zq_stock200)
        p2.picModel()
        p2.update()


if __name__=='__main__':
    T=ZF()
    T.calculate()


        
        