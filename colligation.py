# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 10:28:00 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys)      
sys.setdefaultencoding('utf-8')

from DataHanle.MktDataHandle import MktIndexHandle

from DataHanle.MktDataHandle import MktStockHandle

from sqlalchemy import create_engine 

import datetime

import pandas as pd

import numpy as np

import time

import glob

from PlotData.plotpic import picZH
from PlotData.plotpic import picZqZH
from PlotData.plotexcel import plotToExcel
import ZJ
import rftrading
import tushare as ts


class ZH():
    
    def __init__(self,sdate,edate):
        
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
      
        #定义时间周期   
#        if ' ' not in sdate:     
#            self.tstartdate=datetime.datetime.strptime(sdate, "%Y-%m-%d")  
#        else:
#            self.tstartdate=datetime.datetime.strptime(sdate, "%Y-%m-%d %H:%M")
#
#        if ' 'not in edate:   
#            self.tenddate=datetime.datetime.strptime(edate, "%Y-%m-%d")  
#        else:
#            self.tenddate=datetime.datetime.strptime(edate, "%Y-%m-%d %H:%M")
      
        if ' ' in sdate: 
            self.tstartdate=sdate
            self.tenddate=edate
        else:
            self.tstartdate=sdate+' 00:00'
            self.tenddate=edate+' 15:00'
        
        self.mktindex = MktIndexHandle()
        
        self.mktstock = MktStockHandle()
        
        self.rf30=self.getCode(u'E:\\work\\标的\\30.txt')

        self.rf200=self.getCode(u'E:\\work\\标的\\200.txt')
        
        self.comb30=self.getCode(u'E:\\work\\标的\\combination30.txt')
        
        self.comb200=self.getCode(u'E:\\work\\标的\\combination200.txt')
        
        self.holdings=self.getCode(u'E:\\work\\标的\\holdings.txt')
        
        self.alternation=u'E:\\work\\标的\\30alternative.txt'
              
        self.stocBoardFile=u'E:\\work\\股票数据\\股票细分板块\\allboard.txt'
        
        self.classifyFile=u'E:\\work\\标的\\classify.xlsx'
        
        self.index56=u'E:/work/标的/index56.txt'

        #self.stockNames=pd.read_sql_table('stockinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])   
        self.stockNames=pd.read_table(self.stocBoardFile,usecols=[0,1],index_col=0)
        self.stockNames.columns=['stock_name']
        
        self.stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        self.indexNames=pd.read_sql_table('boardindexbaseinfo',con=self.engine,schema='stocksystem',index_col='board_code',columns=['board_code','board_name'])
        
        
                          
    #apply函数，计算相对涨跌幅
    def xdChg(self,x,indexchg):   
        try:
            indexchg.index=x.index
            x=x-indexchg
            return x    
        except:
            print x.name
            print '数据长度有误'   
            
    #特立独行处理函数，用于apply
    def tldx(self,x,series):
        if not x.any():
            xishu=np.nan
        else:
            nx=x.shift()
            bl=nx/x
            x=np.log(bl).dropna() 
            x.index = np.arange(len(x))  
            series.index=np.arange(len(series))
            try:
                xishu=(np.mean(series*x)-np.mean(series)*np.mean(x))/(np.std(series)*np.std(x))
            except:
                xishu=10
        return np.abs(xishu)  
    
    #计算聪明因子,apply
    def jsS(self,x):
        
        x['smartS']=np.abs(x['chgper'])/np.sqrt(x['hq_vol'])*100000
        x.sort_values(by=['smartS'], ascending=[0],inplace=True) 
        x['accumVolPct']=x['hq_vol'].cumsum()/x['hq_vol'].sum()
        
        return x   
           
    #计算Q因子
    def jsQIndex(self,x):
        
        totalvol=x['hq_vol'].sum()
        try:           
            VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
            x=x[x.accumVolPct<=0.2]
            smartvol=x['hq_vol'].sum()
            VWAPsmart=(x['hq_close']*x['hq_vol']).sum()/smartvol 
            x['Q']=VWAPsmart/VWAPall
            x=x[['hq_name','Q','hq_code']].head(1)
            return x                   
        except ZeroDivisionError:
            pass     
        
    def jsQStock(self,x):
        totalvol=x['hq_vol'].sum()
        try:           
           # VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
            VWAPall=(x['hq_amo']).sum()/totalvol       
            x=x[x.accumVolPct<=0.2]
            smartvol=x['hq_vol'].sum()
            VWAPsmart=(x['hq_amo']).sum()/smartvol 
            x['Q']=VWAPsmart/VWAPall
            x=x[['hq_name','Q','hq_code','board_name']].head(1)
            return x                   
        except ZeroDivisionError:
            pass               
             
    #获取大盘涨幅和收盘价
    def getDapanChg(self,df_dapan):
        #df_dapan = self.mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        df_dapan.index=df_dapan['index']   
        #计算大盘每日涨幅   
        daPanChg = df_dapan['hq_close'].diff()
        daPanPreCLose=df_dapan['hq_close'].shift()
        daPanChgper=daPanChg/daPanPreCLose
        return daPanChgper,df_dapan['hq_close']    
    
    #获取指数代码对应名称
    def getIndexNames(self,df_index,indexNames):
        df_index.index=df_index['hq_code']
        df_index['hq_name']=indexNames['board_name']
        df_index['hq_name']=df_index['hq_name'].apply(lambda x:x.replace(u'通达信行业-',''))  
        df_index.index=np.arange(len(df_index))
        return df_index['hq_name']
    
    #获取股票代码对应名称
    def getStockNames(self,df_stock,stockNames):
        
        if 'hq_code' in df_stock.columns:     
            df_stock.index=df_stock['hq_code']
        df_stock['hq_name']=stockNames['stock_name']
        return df_stock['hq_name']
    
    #获取股票关联板块
    def getStockRelated(self,df_stock,stockrelated):
        #stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        stockrelated['cFlag']=stockrelated['board_name'].str.contains(u'通达信行业-')
        
        stockrelated=stockrelated[stockrelated['cFlag']]
        
        stockrelated['board_name']=stockrelated['board_name'].apply(lambda x:x.replace(u'通达信行业-',''))     
        
        df_stock.index=df_stock.hq_code
        stockrelated.index=stockrelated.stock_id
        stockrelated['board_name']
        return  stockrelated['board_name']
    
    #得到标的代码
    def getCode(self,fname):
        try:
            s1=pd.read_table(fname,usecols=[0],dtype=str,encoding='utf-8')
        except:
            s1=pd.read_table(fname,usecols=[0],dtype=str,encoding='gbk')
        
        s1.columns=['code']
        
        try:
            codelist=s1['code'].astype('int')
        except:
            codelist=s1['code']
            codelist.drop(codelist[codelist==u'数据来源:通达信'].index,inplace=True)
            codelist=codelist.astype('int')
        
        return codelist 
    
    #得到标的细分行业
    def getBoard(self,rfname,df_stock):
        if rfname==200:
            try:
                s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0)
            except:
                s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0,encoding='gbk')
        elif rfname==30:
            try:
                s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0)
            except:
                s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0,encoding='gbk')
        elif rfname==3000:
            try:
                s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0)
            except:
                s1=pd.read_table(self.stocBoardFile,usecols=[0,18],dtype={'代码':int,'细分行业':str},index_col=0,encoding='gbk')

        s1.columns=['board_name']
        
        #如果数据含有列'hq_code',就把code设为索引，否则需要索引为code
        if 'hq_code' in df_stock.columns:
            df_stock.index=df_stock.hq_code
        
        df_stock['board_name']=s1['board_name']
        
        return df_stock
        
    #得到标的数据,要穿一个hq_code列
    def getRF(self,name,df_stock,comb=False,boardFlag=0):
        
        if comb==False:              
            if name==30:      
                codelist=self.rf30
            elif name==200:
                codelist=self.rf200
            elif name == 'holdings':
                codelist=self.holdings
        else:
            if name==30:      
                codelist=self.comb30
            else:
                codelist=self.comb200            
        
        if 'hq_code' in df_stock.columns:        
            df_stock['cFlag']=df_stock['hq_code'].isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]  
        else:
            df_stock['cFlag']=df_stock.index.isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]             
  
        if boardFlag!=0:
            if 'hq_code' in df_stock.columns: 
                df_stock200.set_index('hq_code',inplace=True)

            df_stock200=self.getBoard(name,df_stock200)
            
        del df_stock200['cFlag'],df_stock['cFlag']   
        
        return df_stock200       
    
    def getAlternation(self,df_stock):
        
        codelist=self.getCode(self.alternation)
        
        if 'hq_code' in df_stock.columns:        
            df_stock['cFlag']=df_stock['hq_code'].isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]  
        else:
            df_stock['cFlag']=df_stock.index.isin(codelist)        
            df_stock200=df_stock[df_stock['cFlag']]  
            
        del df_stock200['cFlag'],df_stock['cFlag']   
        
        return df_stock200     
    
    #复权
    def FQ(self,df_stock):
        sdate=self.tstartdate.strftime('%Y-%m-%d')
        edate=self.tenddate.strftime('%Y-%m-%d')
        #对股票进行复权
        #获得股票除权信息
        cqsql="select * from stockratio where date>='"+sdate+"' and date<='"+edate+"'"      
        stockRatio=pd.read_sql(cqsql,con=self.engine)   
        for code in stockRatio['code']:
            #取出对应code的股票，进行复权
            stockData=df_stock[df_stock.hq_code==code]
            ratioData=stockRatio[stockRatio.code==code]
            ratioDates=ratioData['date'][::-1]
            for date in ratioDates:
                ratio=(ratioData[ratioData.date==date])['ratio'].iat[0]
                tmpStock=stockData[stockData.hq_date<date]
                tmpStock['hq_close']=tmpStock['hq_close']*ratio
                stockData[stockData.hq_date<date]=tmpStock
            #用复权后的数据覆盖原数据             
            df_stock[df_stock.hq_code==code]=stockData  
        return df_stock

    def getZqChg(self,x):
        _open=x['hq_close'].iat[0]
        _close=x['hq_close'].iat[-1]
        diff=_close-_open
        try:
            chgper=diff/_open
            x['chgper']=chgper
            return x.tail(1)
        except Exception as e:
            print e       
            print x.name        
    
    def getDayData(self):
        #获取每日数据
        df_stock = self.mktstock.MkStockBarHistOneDayGet('',self.tstartdate,self.tenddate)
                      
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>=880301 and hq_code <= 880497',self.tstartdate,self.tenddate,"D")
        
        index56=self.getCode(self.index56)
        
        df_index=df_index[df_index['hq_code'].isin(index56)]
        
        df_dapan = self.mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        
        df_dapan.index=df_dapan['index']
        df_stock.index=df_stock['index']
        length=len(df_dapan)
        #按交易日期补全数据，停牌涨幅设为-100
        stockGrouped=df_stock[['hq_code','hq_close','hq_open','hq_vol','hq_date']].groupby(df_stock['hq_code'])        
        df_stock=pd.DataFrame()
        datelist=df_dapan.hq_date.tolist()
        
        for code,data in stockGrouped:
            if len(data)!=length:       
                firstopen=data['hq_open'].iat[0]
                data=data.reindex(df_dapan.index)
                data['hq_code']=code     
                data['hq_close'].fillna(method='ffill',inplace=True)
                data['hq_close'].fillna(firstopen,inplace=True)
                data['hq_vol'].fillna(0,inplace=True)  
                data['hq_date']=datelist
            df_stock=df_stock.append(data)        
            
        df_stock.index=np.arange(len(df_stock))
        df_dapan.index=np.arange(length)
        
        return df_stock,df_index,df_dapan
        
    
    def get5minData(self):
        
        tstartdate=self.tstartdate
        
        #取得5分钟级别数据
        df_dapan =self.mktindex.MktIndexBarHistDataGet('=399317',sDate_Start=tstartdate,sDate_End=self.tenddate,speriod='5M') 
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',tstartdate,self.tenddate,"5M")
        
        index56=self.getCode(self.index56)
        
        df_index=df_index[df_index['hq_code'].isin(index56)]        
        
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',tstartdate,self.tenddate,"5M")
        
        return df_stock,df_index,df_dapan
        
        
    def get1minData(self):
        
        df_dapan =self.mktindex.MktIndexBarHistDataGet('=399317',sDate_Start=self.tstartdate,sDate_End=self.tenddate,speriod='1M') 
        
        df_index = self.mktindex.MktIndexBarHistDataGet('>400000',self.tstartdate,self.tenddate,"1M")
        
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',self.tstartdate,self.tenddate,"1M")
        
        return df_stock,df_index,df_dapan        
            
            
    #计算指数每个最小周期涨幅
    def indexEveryChg(self,df_index):
        #计算指数每日涨幅
        df_index['chg'] = df_index['hq_close'].groupby(df_index['hq_code']).diff()
        df_index['preclose']=df_index['hq_close'].groupby(df_index['hq_code']).shift()
        df_index['chgper']=df_index['chg']/df_index['preclose']
        #df_index.index=df_index['index']           
   
        return df_index
      
    #计算股票每个最小周期涨幅
    def stockEveryChg(self,df_stock):
        #计算股票每个周期涨幅
        df_stock['chg'] = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
        df_stock['preclose']=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
        df_stock['chgper']=df_stock['chg']/df_stock['preclose']                
        #修复索引，防止出错
        df_stock.index=np.arange(len(df_stock)) 

        return df_stock        
        
            
    #计算每日涨幅,相对涨幅，排名
    def calculateChg(self,df_stock,df_index,df_dapan):
             
        
        def paixu(x):
            x.sort_values(by=['chgper'], ascending=[0],inplace=True)
            index=np.arange(1,len(x)+1)
            x['paiming']=index
            return x

        #计算大盘每日涨幅   
        df_dapan.index=df_dapan['index']
        daPanChg = df_dapan['hq_close'].diff()
        daPanPreCLose=df_dapan['hq_close'].shift()
        daPanChgper=daPanChg/daPanPreCLose
        dateList=df_dapan['hq_date'].tolist()
        del dateList[0]                               
        #length=len(daPanChg)
        
#        #按交易日期补全数据，停牌涨幅设为-100
#        stockGrouped=df_stock[['hq_code','chgper','hq_date']].groupby(df_stock['hq_code'])
#        
#        df_stock=pd.DataFrame()
#        
##        for code,data in stockGrouped:
##            if len(data)!=length:       
##                data=data.reindex(df_dapan.index)
##                data['hq_code'].fillna(method='bfill',inplace=True)     
##                data['hq_code'].fillna(method='ffill',inplace=True)   
##                data['chgper'].fillna(0,inplace=True)       
##            df_stock=df_stock.append(data)
#       
        #得到指数每日涨幅
        df_index=self.indexEveryChg(df_index)
        #拼接板块的代码与名称              
        df_index['hq_name']=self.getIndexNames(df_index,self.indexNames) 
        #修复索引，防止出错
        #df_index.index=np.arange(len(df_index))    
        df_index['xdchgper']=df_index['chgper'].groupby(df_index['hq_code']).apply(lambda x: self.xdChg(x,daPanChgper))        
        
        df_index=df_index[['hq_name','hq_date','chgper','xdchgper']].dropna() 

        indexChg=[]        
               
        #计算每日排名，加入数据列表
        for date in dateList:
            data=df_index[df_index.hq_date==date]    
            chg=data.sort_values(by=['chgper'], ascending=[0])
            indexChg.append(chg)        
           
        #计算股票每日涨幅           
        df_stock=self.stockEveryChg(df_stock)
        
        #计算每日相对涨幅
        df_stock['xdchgper']=df_stock['chgper'].groupby(df_stock['hq_code']).apply(lambda x: self.xdChg(x,daPanChgper))
        
        #按股票涨幅排序
        df_stock=df_stock.groupby(df_stock['hq_date']).apply(paixu)

        #获取代码对应股票名称
        df_stock['hq_name']=self.getStockNames(df_stock,self.stockNames)
        
        #得到股票所属板块
        df_stock['board_name']=self.getStockRelated(df_stock,self.stockrelated)    
                                         
        #筛选数据
        df_stock=df_stock[['hq_name','hq_date','chgper','xdchgper','board_name','paiming','hq_code']].dropna()
        df_stock.drop(df_stock[np.abs(df_stock.chgper)>0.105].index,inplace=True)
        
        daPanChgper.dropna(inplace=True)        
        
        #找到RF200   
        df_stock200=self.getRF(200,df_stock)               

        #定义涨幅与相对涨幅列表
        stockChg=[]
        stock200Chg=[]
        
        for date in dateList:
            #涨幅的添加
            stockData=df_stock[df_stock.hq_date==date]          
            stock200Data=df_stock200[df_stock200.hq_date==date]
            stockChg.append(stockData)
            stock200Chg.append(stock200Data)   
        
        return indexChg,stockChg,stock200Chg
    


    def stockZqChg(self,df_stock):
        
        zq_stock=df_stock.groupby('hq_code').apply(self.getZqChg)   
        return zq_stock      
        
    def indexZqChg(self,df_index):
        zq_index = df_index.groupby('hq_code').apply(self.getZqChg)
        return zq_index
        
    
    def dapanZqChg(self,df_dapan):
        daPanCloseList=df_dapan['hq_close'].tolist()
        daPanDiff=daPanCloseList[-1]-daPanCloseList[0]
        daPanZqChgper=daPanDiff/daPanCloseList[0] 
        return daPanZqChgper
        
    #计算累计涨幅
    def allChg(self,data,bdFlag=0):
        if bdFlag==0:
            close0=data['hq_close'].iat[0]
            allChg=data['hq_close']/close0-1      
            return allChg 
        else:
            close0=data['hq_close'].iat[0]
            allChg=data['hq_close']/close0-1    
            data['allchg']=allChg
            
            return data          
            
    #将板块内股票的累计涨幅平均，得到板块累计涨幅
    def allChgAvg(self,df,dates):
        meanChgList=[]
        for day in dates:      
            df_tmp=df[df.hq_date==day]
            tmpachg=df_tmp['allchg'].mean()
            meanChgList.append(tmpachg)     
        
        return meanChgList        
      
    def calculateZqChg(self,df_stock,df_index,df_dapan):

        #计算大盘周期涨幅
        daPanZqChgper=self.dapanZqChg(df_dapan)
       
        
        #计算股票周期涨幅
        zq_stock=self.stockZqChg(df_stock)
        #计算周期相对涨幅
        zq_stock['xdchgper']=zq_stock['chgper']-daPanZqChgper
        
        zq_stock['hq_name']=self.getStockNames(zq_stock,self.stockNames) 
        
        #得到股票所属板块        
        zq_stock['board_name']=self.getStockRelated(zq_stock,self.stockrelated)      
        
        #计算周期涨幅排序，并筛选数据 
        zq_stock=zq_stock.sort_values(by=['chgper'], ascending=[0])
        index=np.arange(1,len(zq_stock)+1)
        zq_stock['paiming']=index         

        zq_stock200=self.getRF(200,zq_stock)        
        
        zq_stock30=self.getRF(30,zq_stock)
                        
        #计算周期排名
        zq_stock=zq_stock.loc[:,['hq_name','chgper','xdchgper','board_name','paiming']].dropna() 
        
        zq_stock200=zq_stock200.loc[:,['hq_name','chgper','xdchgper','board_name','paiming']].dropna()        
        
        zq_stock30=zq_stock30.loc[:,['hq_name','chgper','xdchgper','board_name','paiming']]
                
        #计算指数周期涨幅
        zq_index = self.indexZqChg(df_index)
        zq_index['xdchgper']=zq_index['chgper']-daPanZqChgper         
        zq_index['hq_name']=self.getIndexNames(zq_index,self.indexNames)                            
        zq_index=zq_index[['hq_name','chgper','xdchgper']].sort_values(by=['chgper'], ascending=[0])             
        zq_index=zq_index.loc[:,['hq_name','chgper','xdchgper']].sort_values(by=['chgper'], ascending=[0]) 

        return zq_index,zq_stock,zq_stock200,zq_stock30
        
    def calculateMinZqChg(self):
        df_stock= self.mktstock.MktStockBarHistDataGet('>=000001',self.tstartdate,self.tenddate,"5M")
        df_stock=df_stock.groupby('hq_code').apply(self.getZqChg)
        df_stock.set_index('hq_code',inplace=True)
        return df_stock['chgper']
        
        
    #计算Q因子，特立独行
    def calculateFactor(self,df_stock,df_index,df_dapan):
                          
        #拼接板块的代码与名称              
        df_index['hq_name']=self.getIndexNames(df_index,self.indexNames)
        
        #拼接股票的代码与名称
        df_stock['hq_name']=self.getStockNames(df_stock,self.stockNames)
        
        #拼接股票关联板块
        df_stock['board_name']=self.getStockRelated(df_stock,self.stockrelated)
        
        #设置索引，防止groupby出错
        df_stock.index=np.arange(len(df_stock))
               
        #定义存储序列
        iTLDXList=[]
        sTLDXList=[[],[],[]]
        iQList=[]
        sQList=[[],[],[]]
        #sQList200=[]
                      
        dates=df_dapan['hq_date'].drop_duplicates()#.drop(0)
        for tstartdate in dates:
        #for tstartdate in df_index['hq_date']:            
            #tstartdate=datetime.datetime.strptime(tstartdate,'%Y-%m-%d').date()
            
            dapan_price=df_dapan[df_dapan.hq_date==tstartdate]['hq_close']
            
            indexDay=df_index[df_index.hq_date==tstartdate]#[['hq_name','hq_close','hq_vol','hq_code']]
            
            stockDay=df_stock[df_stock.hq_date==tstartdate]#[['hq_name','hq_close','hq_vol','board_name','hq_code','hq_amo']]
                            
            indexTLDX=indexDay.loc[:,['hq_name','hq_close']].groupby(indexDay['hq_name']).agg({'hq_name':'first','hq_close':lambda x:self.tldx(x,dapan_price)}).sort_values('hq_close')                   
            iTLDXList.append(indexTLDX)
            
            stockTLDX=stockDay.loc[:,['hq_name','hq_close','board_name','hq_code']].groupby(stockDay['hq_name']).agg({'hq_name':'first','hq_close':lambda x:self.tldx(x,dapan_price),'board_name':'first','hq_code':'first'}).sort_values('hq_close')
            sTLDXList[0].append(stockTLDX)
            
            #筛选RF200
            stockTLDX200=self.getRF(200,stockTLDX)        
            stockTLDX30=self.getRF(30,stockTLDX)        
            sTLDXList[1].append(stockTLDX200)
            sTLDXList[2].append(stockTLDX30)
                            
            #计算指数当日涨幅
#            chg = indexDay['hq_close'].groupby(indexDay['hq_name']).diff()
#            preclose=indexDay['hq_close'].groupby(indexDay['hq_name']).shift()
#            indexDay['chgper']=chg/preclose            
            indexDay=self.indexEveryChg(indexDay)
                    
            #计算股票当日涨幅  
#            chg = stockDay['hq_close'].groupby(stockDay['hq_name']).diff()
#            preclose=stockDay['hq_close'].groupby(stockDay['hq_name']).shift()
#            stockDay['chgper']=chg/preclose    
            stockDay=self.stockEveryChg(stockDay)
            
            indexDay   = indexDay.groupby(indexDay['hq_code']).apply(self.jsS)
            stockDay   = stockDay.groupby(stockDay['hq_code']).apply(self.jsS)
            
            indexQ = indexDay.groupby(indexDay['hq_code']).apply(self.jsQIndex).sort_values('Q')
            stockQ = stockDay.groupby(stockDay['hq_code']).apply(self.jsQStock).sort_values('Q')
            
            #筛选RF200    
            stockQ200=self.getRF(200,stockQ)      
            stockQ30=self.getRF(30,stockQ)              
            iQList.append(indexQ)
            sQList[0].append(stockQ)
            sQList[1].append(stockQ200)
            sQList[2].append(stockQ30)
            
            print tstartdate
            
        return iTLDXList,sTLDXList,iQList,sQList
            
            
            
    def indexZqTldx(self,df_index,df_dapan):
        indexTLDX=df_index[['hq_code','hq_close']].groupby(df_index['hq_code']).agg({'hq_code':'first','hq_close':lambda x:self.tldx(x,df_dapan['hq_close'])}).sort_values('hq_close')
        return indexTLDX
        
        
    def indexZqQ(self,df_index):
        def jsQindex(x):
            totalvol=x['hq_vol'].sum()
            try:           
                VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
                x=x[x.accumVolPct<=0.2]
                smartvol=x['hq_vol'].sum()
                VWAPsmart=(x['hq_close']*x['hq_vol']).sum()/smartvol 
                x['Q']=VWAPsmart/VWAPall
                x=x.loc[:,['Q','hq_code']].head(1)
                return x                   
            except ZeroDivisionError:
                pass           
     
        #计算指数涨幅
        df_index=self.indexEveryChg(df_index)
        #计算指数聪明因子                 
        df_index   = df_index.groupby(df_index['hq_code']).apply(self.jsS)                                  
        #指数Q因子
        indexQ = df_index.groupby(df_index['hq_code']).apply(jsQindex).sort_values('Q') 

        return indexQ      
    
    def stockZqTldx(self,df_stock,df_dapan):
        stockTLDX=df_stock[['hq_close','hq_code']].groupby(df_stock['hq_code']).agg({'hq_close':lambda x:self.tldx(x,df_dapan['hq_close']),'hq_code':'first'}).sort_values('hq_close')        
        return stockTLDX        
        
    def stockZqQ(self,df_stock):
        def jsQstock(x):
            totalvol=x['hq_vol'].sum()
            try:           
                VWAPall=(x['hq_amo']).sum()/totalvol
                x=x[x.accumVolPct<=0.2]
                smartvol=x['hq_vol'].sum()
                VWAPsmart=(x['hq_amo']).sum()/smartvol 
                x['Q']=VWAPsmart/VWAPall
                x=x.loc[:,['Q','hq_code']].head(1)
                return x                   
            except ZeroDivisionError:
                pass          
        #计算股票涨幅  
        df_stock=self.stockEveryChg(df_stock)
        #先计算聪明因子
        df_stock   = df_stock.groupby(df_stock['hq_code']).apply(self.jsS)    
        #计算Q因子
        stockQ = df_stock.groupby(df_stock['hq_code']).apply(jsQstock).sort_values('Q')     
        
        return stockQ
        
    def calculateZqFactor(self,df_stock,df_index,df_dapan): 
                     
#        chg = df_index['hq_close'].groupby(df_index['hq_code']).diff()
#        preclose=df_index['hq_close'].groupby(df_index['hq_code']).shift()
#        df_index['chgper']=chg/preclose    
                

#        chg = df_stock['hq_close'].groupby(df_stock['hq_code']).diff()
#        preclose=df_stock['hq_close'].groupby(df_stock['hq_code']).shift()
#        df_stock['chgper']=chg/preclose   
                    
        #df_stock.fillna(0,inplace=True)
        
        #股票特立独行
        stockTLDX=self.stockZqTldx(df_stock,df_dapan)
       #计算股票Q因子
        stockQ=self.stockZqQ(df_stock)        
                  
        #计算指数特立独行
        indexTLDX=self.indexZqTldx(df_index,df_dapan)           
        #指数Q因子
        indexQ=self.indexZqQ(df_index)
        

        #拼接股票的代码与名称
        stockTLDX['hq_name']=self.getStockNames(stockTLDX,self.stockNames) 
        stockQ['hq_name']=self.getStockNames(stockQ,self.stockNames)
          
        #拼接股票关联板块
        stockTLDX['board_name']=self.getStockRelated(stockTLDX,self.stockrelated)  
        stockQ['board_name']=self.getStockRelated(stockQ,self.stockrelated)
        
        #删除未录入名字的新股
        stockTLDX.dropna(inplace=True)
        stockQ.dropna(inplace=True)
        
        #拼接板块的代码与名称              
        indexTLDX['hq_name']=self.getIndexNames(indexTLDX,self.indexNames)
        indexQ['hq_name']=self.getIndexNames(indexQ,self.indexNames)     
        
        #筛选RF200   
        stockQ200=self.getRF(200,stockQ)
        
        stockQ30=self.getRF(30,stockQ)
        
        stockTLDX200=self.getRF(200,stockTLDX)
        
        stockTLDX30=self.getRF(30,stockTLDX)
        
        return indexTLDX,stockTLDX,indexQ,stockQ,stockQ200,stockTLDX200,stockQ30,stockTLDX30
    
    def all_chg(self,code):
        
        df=self.mktindex.MktIndexBarHistDataGet('='+code,self.tstartdate,self.tenddate,"D")
        
        allChg=self.allChg(df)
        
        return allChg    
    
    
    def rfAllchg(self,df_stock,df_dapan):
                          
          #沪深300
        df_hs300  = self.mktindex.MktIndexBarHistDataGet('=399300',self.tstartdate,self.tenddate,"D")       
        
        #找到RF200，用于计算累计涨幅   
        df_stock200=self.getRF(200,df_stock)[['hq_code','hq_date','hq_close','hq_open']] 
     
        #找到RF30  
        df_stock30=self.getRF(30,df_stock)[['hq_code','hq_date','hq_close','hq_open']]            
  
        #计算累计涨幅
        allChgdapan=self.all_chg('399317')
        allChgcybz=self.all_chg('399102')
        allChgcx=self.all_chg('399678')
        allChgsz50=self.all_chg('000016')
        allChgzz500=self.all_chg('399905')
        allChgsz=self.all_chg('000001')
        allChgzz1000=self.all_chg('000852')
        allChghs300=self.allChg(df_hs300)
        allChghsi=self.all_chg('100001')
        allChghz5014=self.all_chg('105014')
        allChghz5044=self.all_chg('105044')
#        allChgsz=self.allChg(df_sz)
#        allChgzz1000=self.allChg(df_zz1000)        
        
        
        achg200=df_stock200[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:self.allChg(x,1))
        achg30=df_stock30[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:self.allChg(x,1))  
        
        timeList=[]
        allChg200=[]
        allChg30=[]
        
        dates=df_dapan['hq_date']
        for date in dates:
            timeList.append(date.strftime("%Y-%m-%d") )
            tmp200=achg200[achg200.hq_date==date]
            tmpachg=tmp200['allchg'].mean()
            allChg200.append(tmpachg)
            tmp30=achg30[achg30.hq_date==date]
            tmpachg=tmp30['allchg'].mean()
            allChg30.append(tmpachg)            
              
        allChg200=pd.Series(allChg200)
        allChg30=pd.Series(allChg30)
        avgChg={'399317':[],'200':[],'30':[],'300':[]}   

        chg200to30=allChg200-allChg30
        chg200tohs300=allChg200-allChghs300        
        chg30tohs300=allChg30-allChghs300
        chg50to300=allChgsz50-allChghs300
        chg500to300=allChgzz500-allChghs300 
        chg1000to300=allChgzz1000-allChghs300 
        chgcybto300=allChgcybz-allChghs300
        chgcxto300=allChgcx-allChghs300       
        
        #得到净值数据
        avgDic={'日期':timeList,'RF200净值':allChg200,'RF30净值':allChg30,'国证A指':allChgdapan,'RF200相对RF30':chg200to30,'RF30相对沪深300':chg30tohs300,'上证50相对沪深300':chg50to300,'中证500相对沪深300':chg500to300,'创业板综相对沪深300':chgcybto300,'次新股相对沪深300':chgcxto300,'创业板综':allChgcybz,'次新股':allChgcx,'上证50':allChgsz50,'中证500':allChgzz500,'沪深300':allChghs300,'沪深300指数':df_hs300['hq_close'],'RF200相对沪深300':chg200tohs300,'上证指数':allChgsz,'中证1000':allChgzz1000,'中证1000相对沪深300':chg1000to300,'恒生指数':allChghsi,'恒生国企指数':allChghz5014,'恒生AH股溢价指数':allChghz5044}
        avg=pd.DataFrame(avgDic)   
        #计算周期净值变化
        chg200=allChg200.iat[-1]
        chg30=allChg30.iat[-1]
        chgi=allChgdapan.iat[-1]
        chg30to200=chg30-chg200
        chgito200=chgi-chg200
        chg30toi=chg30-chgi
        chg200toi=chg200-chgi
        chg30tohs300=chg30-allChghs300.iat[-1]
        chg200tohs300=chg200-allChghs300.iat[-1]
        chgitohs300=chgi-allChghs300.iat[-1]
           
        avgChg['200'].append(chg200)
        avgChg['200'].append(chg200toi)
        avgChg['200'].append(0)
        avgChg['200'].append(chg200tohs300)
        
        avgChg['30'].append(chg30)        
        avgChg['30'].append(chg30toi)
        avgChg['30'].append(chg30to200)
        avgChg['30'].append(chg30tohs300)
        
        avgChg['399317'].append(chgi)       
        avgChg['399317'].append(0)         
        avgChg['399317'].append(chgito200) 
        avgChg['399317'].append(chgitohs300)
        
        avgChg['300'].append(allChghs300.iat[-1])
        avgChg['300'].append(allChghs300.iat[-1]-chgi)
        avgChg['300'].append(allChghs300.iat[-1]-chg200)
        avgChg['300'].append(0) 
        
        return avg,avgChg  
        

    def combAllchg(self):
                
        def getSharpeRatio(series):
            
            yieldRate=series.iat[-1]/len(series)*252
             
            annualizedStd=np.std(series)*np.sqrt(252)
            
            sharpeRatio=(yieldRate-3.642/100)/annualizedStd
            
            series+=1
            max_draw_down = 0
            temp_max_value = 0
            for i in range(1, len(series)):
                temp_max_value = max(temp_max_value, series.iat[i-1])
                #temp_max_value=max(series[:i])
                max_draw_down = min(max_draw_down, (series.iat[i]/temp_max_value)-1)
      
            return sharpeRatio,max_draw_down
        
        df_stock = self.mktstock.MkStockBarHistOneDayGet('',self.tstartdate,self.tenddate)[['hq_code','hq_date','hq_close','hq_open']] 
        
        df_stock=df_stock.groupby('hq_code').apply(self.allChg,1)
        
        df_stock['hq_name']=self.getStockNames(df_stock,self.stockNames)
        
        df_stock['hq_name']=df_stock['hq_name'].astype(unicode)
        
        df_dapan = self.mktindex.MktIndexBarHistDataGet('=399317',self.tstartdate,self.tenddate,"D")
        
        df_hs300  = self.mktindex.MktIndexBarHistDataGet('=399300',self.tstartdate,self.tenddate,"D") 
            
        df_rf200=self.getRF(200,df_stock,comb=False)#[['hq_code','hq_date','hq_close','hq_open']] 
        
        df_rf30=self.getRF(30,df_stock,comb=False)#[['hq_code','hq_date','hq_close','hq_open']] 
     
        df_comb=self.getRF(30,df_stock,comb=True)#[['hq_code','hq_date','hq_close','hq_open']]   
        
        df_holdings=self.getRF('holdings',df_stock,comb=False)
        
        df_alternation=self.getAlternation(df_stock)
        
        allChghs300=self.allChg(df_hs300)
              
        allChgdapan=self.allChg(df_dapan)       
    
#        achg200=df_rf200[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:self.allChg(x,1))  
#        
#        achg30=df_rf30[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:self.allChg(x,1))   
#        
#        achgcomb=df_comb[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:self.allChg(x,1))

        #achgalternation=df_alternation[['hq_code','hq_open','hq_close','hq_date']].groupby('hq_code').apply(lambda x:self.allChg(x,1))        
        
        allChg200=[]
        allChg30=[]
        allChgcomb=[]
        allChgAlternation=[]
        allChgHoldings=[]
        
        dates=df_hs300['hq_date']
        for date in dates:
                     
            tmp200=df_rf200[df_rf200.hq_date==date]
            
            allChg200.append(tmp200['allchg'].mean())
            
            tmp30=df_rf30[df_rf30.hq_date==date]
            
            allChg30.append(tmp30['allchg'].mean())
            
            tmpcomb=df_comb[df_comb.hq_date==date]
            
            allChgcomb.append(tmpcomb['allchg'].mean())
            
            tmpalternation=df_alternation[df_alternation.hq_date==date]
            
            allChgAlternation.append(tmpalternation['allchg'].mean())
            
            tmpHoldings=df_holdings[df_holdings.hq_date==date]
            
            allChgHoldings.append(tmpHoldings['allchg'].mean())
            
        allChg200=pd.Series(allChg200)
        allChg30=pd.Series(allChg30)
        allChgcomb=pd.Series(allChgcomb)
        allChgAlternation=pd.Series(allChgAlternation)
        allChgHoldings=pd.Series(allChgHoldings)
        
        datelist=df_hs300['hq_date'].astype(str)
        period=datelist.iat[1]+'--'+datelist.iat[-1]
        
        avgDic={'日期':datelist,'组合':allChgcomb,'RF200':allChg200,'RF30':allChg30,'沪深300':allChghs300,'备选':allChgAlternation,'持仓':allChgHoldings}
        df_avg=pd.DataFrame(avgDic)   
              
        #avgChg={'200':[],'30':[],'300':[],'comb':[]}
        avgChg={}
        
        chgdapan=allChgdapan.iat[-1]
           
        chg200=allChg200.iat[-1]
        chg30=allChg30.iat[-1]
        chghs300=allChghs300.iat[-1]
        
        chg200to30=chg200-chg30       
        chg200tohs300=chg200-chghs300
                      
        chg30tohs300=chg30-chghs300   
        chg30to200=-chg200to30
          
        chghs300to200=-chg200tohs300        
       # chghs300to30=-chg30tohs300
        
        avgChg['RF200']=['RF200',chg200,chg200-chgdapan,0,chg200tohs300]        
        
        avgChg['RF30']=['RF30',chg30,chg30-chgdapan,chg30to200,chg30tohs300]
        
        avgChg['沪深300']=['沪深300',allChghs300.iat[-1],allChghs300.iat[-1]-chgdapan,chghs300to200,0]
        
        avgChg['备选']=['备选',allChgAlternation.iat[-1],allChgAlternation.iat[-1]-chgdapan,allChgAlternation.iat[-1]-chg200,allChgAlternation.iat[-1]-allChghs300.iat[-1]]
        
        avgChg['组合']=['组合',allChgcomb.iat[-1],allChgcomb.iat[-1]-chgdapan,allChgcomb.iat[-1]-chg200,allChgcomb.iat[-1]-allChghs300.iat[-1]]
        
        avgChg['持仓']=['持仓',allChgHoldings.iat[-1],allChgHoldings.iat[-1]-chgdapan,allChgHoldings.iat[-1]-chg200,allChgHoldings.iat[-1]-allChghs300.iat[-1]]
        
        
        #从这里开始计算股票池细分板块数据
        if len(dates)>10:
            df_classify=pd.read_excel(self.classifyFile,header=None,encoding='gbk',index_col=0)
            
            boards=df_classify.index
            
            lowDic={'board':[],'close':[]}
            highDic={'board':[],'close':[]}
            meanChgDic={}
            
            for i in xrange(len(df_classify)):
                board=boards[i]
                theStocks=df_classify.iloc[i].dropna()
    #            names=list(df_stock['hq_name'])
    #            df_stock['cflag']=np.where(names in theStocks,True,False )
    #            df_tmp=df_stock[df_stock['cflag']]
                df_tmp=df_stock[df_stock['hq_name'].isin(theStocks)]
                meanChgList=self.allChgAvg(df_tmp,dates)
                
                close=meanChgList[-1]
                
                if close>allChgcomb.iat[-1]:   
                    highDic['board'].append(board)
                    highDic['close'].append(close)
                    meanChgDic[board]=meanChgList   
                            
                    
#                    avgChg[board]=[board,meanChgList[-1],meanChgList[-1]-chgdapan,meanChgList[-1]-chg200,meanChgList[-1]-allChghs300.iat[-1]]
#                    df_avg[board]=meanChgList     
                
                if close<chg200:
                    lowDic['board'].append(board)
                    lowDic['close'].append(close)
                    meanChgDic[board]=meanChgList
                    
            df_high=pd.DataFrame(highDic).sort_values('close',ascending=False).iloc[0:3]
                
            for board in df_high['board']:
                meanChgList=meanChgDic[board]
                avgChg[board]=[board,meanChgList[-1],meanChgList[-1]-chgdapan,meanChgList[-1]-chg200,meanChgList[-1]-allChghs300.iat[-1]]
                df_avg[board]=meanChgList                       
                    
                
            df_low=pd.DataFrame(lowDic).sort_values('close').iloc[0:3]
            
            for board in df_low['board']:
                meanChgList=meanChgDic[board]
                avgChg[board]=[board,meanChgList[-1],meanChgList[-1]-chgdapan,meanChgList[-1]-chg200,meanChgList[-1]-allChghs300.iat[-1]]
                df_avg[board]=meanChgList   
                
        
        #从这里开始计算几大线条的数据
        #orientation缩写ORI
        df_rf20=pd.read_excel(u'E:\\work\\标的\\rfOri.xlsx',header=0)
        
        df_rf20[df_rf20.columns[1]].fillna(method='ffill',inplace=True)
        
        rfBoards=df_rf20[df_rf20.columns[1]].drop_duplicates()
        
        df_rf20.set_index(df_rf20.columns[1],inplace=True)
        
        df_rf20.drop([df_rf20.columns[0],'代码',df_rf20.columns[3]],axis=1,inplace=True)
        
        for board in rfBoards:
            
            theStocks=[]
            
            df_tmp=df_rf20.loc[board]
            
            for i in xrange(len(df_tmp)):
                
                if type(df_tmp) == type(df_rf20):
                    
                    tmpSeries=df_tmp.iloc[i].dropna()
                    
                else:
                    
                    tmpSeries=df_tmp.dropna()
                
                if not tmpSeries.empty:   
                    
                    #theStocks=theStocks.append(tmpSeries)
                       
                    theStocks.extend(tmpSeries)     
            
            #在这里isin股票名称无法识别全，是因为df_stock里的股票名称不是unicode类型的，需要转换成统一类型的才行
            df_tmp=df_stock[df_stock['hq_name'].isin(theStocks)]
            
            meanChgList=self.allChgAvg(df_tmp,dates)        
            
            avgChg[board]=[board,meanChgList[-1],meanChgList[-1]-chgdapan,meanChgList[-1]-chg200,meanChgList[-1]-allChghs300.iat[-1]] 
            
            df_avg[board]=meanChgList
            
            
        #计算夏普比率
        for key in df_avg:
            if key != u'日期':
                tmp_series=df_avg[key][1:]
                sharpeRatio,maxDrawdown=getSharpeRatio(tmp_series)
                avgChg[key].append(sharpeRatio)
                avgChg[key].append(maxDrawdown)
                
        
            
        p=plotToExcel.PlotToExcel()
        p.combAvg(df_avg,avgChg,period)
        
        return df_avg,avgChg


    def getRFTop(self,stock200ZqChg,stock30ZqChg): 
        #规定数据长度为RF30的长度
        stock200ZqChg=stock200ZqChg.iloc[:,0:2].head(len(stock30ZqChg))
        stock30ZqChg=stock30ZqChg.iloc[:,0:2]
        
        stock200ZqChg=stock200ZqChg.reset_index()
        stock30ZqChg=stock30ZqChg.reset_index()        
      
#        stock200ZqChg.index=np.arange(len(stock200ZqChg))   
#        stock30ZqChg.index=np.arange(len(stock30ZqChg))   
#        RFtopdata=pd.concat([stock200ZqChg,stock30ZqChg],axis=1).head(15)
#        RFtopdata.columns=['RFTop30','RFTop30Chg','RF30','RF30Chg']  
        return stock200ZqChg,stock30ZqChg 
    
    def buildForms(self):
        
        stockd,indexd,dapand=self.getDayData()
        stock5,index5,dapan5=self.get5minData()        

        zfTimes=dapand['hq_date'].drop_duplicates().drop(0).astype(str)
        
        #生成每日报表
        sdate=dapand['hq_date'].iat[-2]
        indexChg,stockChg,stock200Chg=self.calculateChg(stockd[stockd['hq_date']>=sdate],indexd[indexd['hq_date']>=sdate],dapand[dapand['hq_date']>=sdate])
        sdate=dapand['hq_date'].iat[-1]
        (iTLDX,sTLDX,iQList,sQList)=self.calculateFactor(stock5[stock5['hq_date']==sdate],index5[index5['hq_date']==sdate],dapan5[dapan5['hq_date']==sdate])
       
        num=0
        #for t in zfTimes:
        for t in [zfTimes.iat[-1]]:
            p=picZH.picExcel(t,indexChg[num],stockChg[num],stock200Chg[num],iTLDX[num],sTLDX[0][num],sTLDX[1][num],sTLDX[2][num],iQList[num],sQList[0][num],sQList[1][num],sQList[2][num])
            p.picModel()
            #p.update()
            num+=1
            print num
#        
        if len(zfTimes)>1:
            #生成周期报表
            indexZqTLDX,stockZqTLDX,indexQ,stockQ,stockQ200,stockTLDX200,stockQ30,stockTLDX30=self.calculateZqFactor(stock5,index5,dapan5)  
            indexZqChg,stockZqChg,stock200ZqChg,stock30ZqChg=self.calculateZqChg(stockd,indexd,dapand)      
            sdate=zfTimes.iat[0]
            edate=zfTimes.iat[-1]   
            period=sdate+'-'+edate       
            p=picZqZH.picExcel(period,indexZqChg,stockZqChg,stock200ZqChg,stock30ZqChg,indexZqTLDX,stockZqTLDX,stockTLDX200,stockTLDX30,indexQ,stockQ,stockQ200,stockQ30)
            p.picModel()
            print '综合报表生成'   
        

    def factorCount(self):
        tldxdir=u'E:/work/数据备份/tldx200/*.csv'
        qdir=u'E:/work/数据备份/Q200/*.csv'
        
        tldxdata=pd.DataFrame()
        qdata=pd.DataFrame()
        tldxflist=glob.glob(tldxdir)
        qflist=glob.glob(qdir)
        
        for f in tldxflist:
            s1=pd.read_csv(f,encoding='gbk').head(10)
            s1['date']=f.split('\\')[-1][:-4]
            tldxdata=tldxdata.append(s1)
            
        for f in qflist:
            s1=pd.read_csv(f,encoding='gbk').head(10)
            s1['date']=f.split('\\')[-1][:-4]
            qdata=qdata.append(s1)         
        
        tldxdata=tldxdata.loc[:,['hq_code','hq_name','hq_close','date']]
        qdata=qdata.loc[:,['hq_code','hq_name','Q','date']]
        
        zh=tldxdata.append(qdata)
        
        #统计个股在综合中出现的次数
        tj=zh.groupby('hq_code').agg({'hq_name':'first','hq_code':'count'})
        #找到总出现次数大于1的
        tj=tj[tj.hq_code>1]
        
        #筛选出现次数大于1的票
        zh['cFlag']=zh['hq_name'].isin(tj.hq_name)
        zh=zh[zh['cFlag']].sort_values('hq_name')
       
        date=datetime.datetime.now().strftime('%Y-%m-%d')
 
        zh.to_csv(u'E:/work/数据备份/因子统计结果/'+date+'.csv',encoding='gbk')
        
        return zh,tj
    
    #为数据排序
    def rank(self,data,gradename,maxgrade=5):
        
        #计算长度均值，然后依据这个值把数据划分为5等分
        
        dataLen=len(data)
        
        #获取列名，如果code列不存在，直接reindex，否则重新赋值给index
        
        columns=list(data.columns)
        if 'hq_code' not in columns:
            data.reset_index(inplace=True)
        else:         
            data.index=np.arange(dataLen)
        avg=float(dataLen)/maxgrade
                 
        n=0
        
        grade=pd.Series()
        
        gradename=gradename+'grade'
        #不知道为什么，这里用np.arange排序后不能用这个排序把数据筛选出来，只能用索引排序筛选了
        for i in xrange(maxgrade):
            tmp=data[(data.index>=n)&(data.index<(n+avg))] 
 
            tmp[gradename]=(maxgrade-i)

            grade=grade.append(tmp[gradename])

            n+=avg
            
        data[gradename]=grade
    
        data.set_index('hq_code',inplace=True)
    
        return data

    def zjRank(self,sdate,edate,allFlag,networth): 
        
        sdate=sdate+' 00:00'
        edate=edate+" 15:00"
        z=ZJ.ZJ()
        amosum,zj=z.zqAmoMin(sdate,edate)
        zj200=self.getRF(200,zj)
        zj30=self.getRF(30,zj)
        if allFlag==0:     
            zj200=self.rank(zj200,'zj')
            zj30=self.rank(zj30,'zj')
            return zj200,zj30
        else:
            zj=self.rank(zj,'zj')        

#        zj200,zj30,zj=z.zqAmo(sdate,edate)        
#        if allFlag==0:
#            zj200=zj200.loc[:,['hq_code','vbigper']]
#            zj30=zj30.loc[:,['hq_code','vbigper']]     
#            zj200=self.rank(zj200,'zj')
#            zj30=self.rank(zj30,'zj')
#            return zj200,zj30
#        else:
#            zj=self.rank(zj.loc[:,['hq_code','vbigper','bigD','vbigD','lbigD']] ,'zj')
            return zj
               
    def factorRank(self,sdate,edate,allFlag=0):
        sdate=sdate.split()[0]
        edate=edate.split()[0]
        date=sdate+'-'+edate
        tldxfile200=u'E:/work/数据备份/tldx200'+'/'+date+'.csv'
        tldxfile30=u'E:/work/数据备份/tldx30'+'/'+date+'.csv'
        tldxfile=u'E:/work/数据备份/tldx'+'/'+date+'.csv'
        qfile200=u'E:/work/数据备份/Q200'+'/'+date+'.csv'
        qfile30=u'E:/work/数据备份/Q30'+'/'+date+'.csv'
        qfile=u'E:/work/数据备份/Q'+'/'+date+'.csv'
        
        print qfile,qfile30
        
        if allFlag==0:
            tldx200=pd.read_csv(tldxfile200,encoding='gbk').loc[:,['hq_code','hq_name','hq_close']]
            tldx30=pd.read_csv(tldxfile30,encoding='gbk').loc[:,['hq_code','hq_name','hq_close']]
            q200=pd.read_csv(qfile200,encoding='gbk').loc[:,['hq_code','hq_name','Q']]
            q30=pd.read_csv(qfile30,encoding='gbk').loc[:,['hq_code','hq_name','Q']]
            
            tldx200=self.rank(tldx200,'tldx')
            tldx30=self.rank(tldx30,'tldx')
            q200=self.rank(q200,'q')
            q30=self.rank(q30,'q')
            
            q200['fgrade']=0.5*tldx200['tldxgrade']+0.5*q200['qgrade']
            q30['fgrade']=0.5*tldx30['tldxgrade']+0.5*q30['qgrade']
            
            return q200['fgrade'],q30['fgrade']
        else:
            tldx=pd.read_csv(tldxfile,encoding='gbk').loc[:,['hq_code','hq_name','hq_close']]
            q=pd.read_csv(qfile,encoding='gbk').loc[:,['hq_code','hq_name','Q']]
            tldx=self.rank(tldx,'tldx')
            q=self.rank(q,'q')    

            q['fgrade']=0.5*tldx['tldxgrade']+0.5*q['qgrade']   
            
            return q['fgrade']
    
    
    def chgRank(self,stock200chg,stock30chg,stockchg=pd.DataFrame(),maxgrade=5):
        
        if stockchg.empty:          
            stock200chg=self.rank(stock200chg,'chg',maxgrade)
            stock30chg=self.rank(stock30chg,'chg',maxgrade)
             
            return stock200chg.loc[:,['hq_name','chgper','chggrade']],stock30chg.loc[:,['hq_name','chgper','chggrade']]
        else:
            stockchg=self.rank(stockchg,'chg',maxgrade)
            return stockchg.loc[:,['hq_name','chgper','chggrade']]
            
            
    def tradingRank(self,sdate,edate,allFlag,update):
        
        r=rftrading.RF_TradingMonitor()
        rzye200_df,rzye30_df,atr_200,atr_30,rzye_df,atr_all=r.getTrading(sdate,edate,update)   
        
        if allFlag==0:
            rzye200_df=self.rank(rzye200_df,'rz')
            rzye30_df=self.rank(rzye30_df,'rz')
            atr_200=self.rank(atr_200,'atr')
            atr_30=self.rank(atr_30,'atr')
            
            return rzye200_df,rzye30_df,atr_200,atr_30
        else:
            rzye_df=self.rank(rzye_df,'rz')
            atr_all=self.rank(atr_all,'atr')        
            return rzye_df,atr_all
    
    
    def getRank(self,stock200ZqChg,stock30ZqChg,stockZqChg,sdate,edate,allFlag,update,networth):
        
        if allFlag==0:
            stock200ZqChg,stock30ZqChg=self.chgRank(stock200ZqChg,stock30ZqChg)        
            frank200,frank30=self.factorRank(sdate,edate)
            zjdata200,zjdata30=self.zjRank(sdate,edate,allFlag,networth)   
            rzye200_df,rzye30_df,atr_200,atr_30=self.tradingRank(sdate,edate,allFlag,update)   
            
            df_rank=[]
            
            df_rank1=pd.concat([frank200,zjdata200,stock200ZqChg,rzye200_df,atr_200],axis=1)
            df_rank1.loc[:,['mt_rzye','rzgrade']]=df_rank1.loc[:,['mt_rzye','rzgrade']].fillna(0)
            df_rank1['grade']=df_rank1['chggrade']*0.3+df_rank1['zjgrade']*0.3+df_rank1['rzgrade']*0.1+df_rank1['fgrade']*0.3#df_rank1['atrgrade']*0.1+
            df_rank1=df_rank1.sort_values('grade',ascending=False).dropna() 
            df_rank1=self.getBoard(200,df_rank1)
            df_rank1.index=np.arange(len(df_rank1))
            
            df_rank2=pd.concat([frank30,zjdata30,stock30ZqChg,rzye30_df,atr_30],axis=1)
            df_rank2.loc[:,['mt_rzye','rzgrade']]=df_rank2.loc[:,['mt_rzye','rzgrade']].fillna(0)
            df_rank2['grade']=df_rank2['chggrade']*0.3+df_rank2['zjgrade']*0.3+df_rank2['rzgrade']*0.1+df_rank2['fgrade']*0.3 #+df_rank2['atrgrade']*0.1
            df_rank2.sort_values('grade',ascending=False,inplace=True)   
            df_rank2=self.getBoard(30,df_rank2)
            df_rank2.index=np.arange(len(df_rank2))
            
            #标记200中的30，后面用不同的颜色区分开            
            df_rank1['cFlag']=df_rank1['hq_name'].isin(df_rank2['hq_name'])
            
            df_rank.append(df_rank1)
            df_rank.append(df_rank2)
            
            return df_rank1,df_rank2
        else:
            stockZqChg=self.chgRank(stock200ZqChg,stock30ZqChg,stockZqChg)
            frank=self.factorRank(sdate,edate,1)
            zjdata=self.zjRank(sdate,edate,allFlag,networth)
            rzye_df,atr=self.tradingRank(sdate,edate,allFlag,update)
            
            df_rank=pd.concat([stockZqChg,frank,zjdata,rzye_df,atr],axis=1)
            df_rank.loc[:,['mt_rzye','rzgrade']]=df_rank.loc[:,['mt_rzye','rzgrade']].fillna(0)
            df_rank['grade']=df_rank['chggrade']*0.3+df_rank['zjgrade']*0.3+df_rank['atrgrade']*0.1+df_rank['rzgrade']*0.1+df_rank['fgrade']*0.2
            if 'cFlag' in df_rank:       
                del df_rank['cFlag']
            df_rank=df_rank.sort_values('grade',ascending=False).dropna() 
            df_rank.index=np.arange(len(df_rank))       
            return df_rank


    def buildJzRankForm(self,allFlag,update,rankFlag,networth):
        
        stockd,indexd,dapand=self.getDayData() 
        indexZqChg,stockZqChg,stock200ZqChg,stock30ZqChg=self.calculateZqChg(stockd,indexd,dapand) 
        
        sdate=dapand['hq_date'].iat[1].strftime("%Y-%m-%d")
        edate=dapand['hq_date'].iat[-1].strftime("%Y-%m-%d") 
        period=sdate+'至'+edate        
        
        #allFlag=0，则只取公司标的，否则取全市场       
        if rankFlag==0:
            df_rank=[]
        else:
            df_rank=self.getRank(stock200ZqChg,stock30ZqChg,stockZqChg,sdate,edate,allFlag=allFlag,update=update,networth=networth)
        
        if allFlag==0:
            rf200zqchg,rf30zqchg=self.getRFTop(stock200ZqChg,stock30ZqChg)
            
            avg,avgChg=self.rfAllchg(stockd,dapand)
            
            pte = plotToExcel.PlotToExcel()
            pte.buildAvg(avg,avgChg,stock200ZqChg,stock30ZqChg,df_rank,period)        
        else:
            df_rank=self.getBoard(3000,df_rank)
            df_rank=df_rank.loc[:,['hq_name','board_name','chgper','amopower','mt_rzye','atr','fgrade','grade']]
            df_rank30=self.getRF(30,df_rank)
            df_rank.columns=['名称','板块','涨幅','大单效能','融资余额','ATR','异动级别','总评']
            df_rank.to_csv(u'E:\\work\\数据备份\\全市场选股\\'+period+'.csv',encoding='gbk',float_format='%.2f')
            df_rank30.to_csv(u'E:\\work\\数据备份\\全市场选股\\RF'+period+'.csv',encoding='gbk',float_format='%.2f')
        
        
    def buildMinRankForm(self,factor=False):

        def writeDf(boardNames,df_rank,left,sheet):
            
            top=0
                        
            header=df_rank.columns
            
            for board in boardNames:
                
                df_rank_tmp=df_rank[df_rank[u'板块']==board]
                
                if len(df_rank_tmp)>1:                 
                
                    sheet.write_row(top,left,header)  
                    
                    for i in xrange(len(df_rank_tmp)):    
                        
                        sheet.write_row(top+i+1,left,df_rank_tmp.iloc[i])   
                    
                    top+=len(df_rank_tmp)+2
                            
            return sheet
                
        z=ZJ.ZJ()
        
        amosum,amonet=z.zqAmoMin(self.tstartdate,self.tenddate) 

        zjRanknet=self.rank(amonet,'zj',10)
        
        zjRanksum=self.rank(amosum,'zj',10)
        
        sdatetime=self.tstartdate.split()
        sdate=sdatetime[0]
        stime=sdatetime[1]
        
        edatetime=self.tenddate.split()    
        edate=edatetime[0]
        etime=edatetime[1]
            
        if factor==True:
            
            if stime == '00:00' and etime == '15:00':

                try:
                    if sdate == edate:
                        stockQ=pd.read_csv(u'E:\\work\\数据备份\\Q\\'+sdate+'.csv',encoding='gbk')
                    
                    else:
                        stockQ=pd.read_csv(u'E:\\work\\数据备份\\Q\\'+sdate+'-'+edate+'.csv',encoding='gbk')
                                    
                    print u'\n读取本地Q因子文件...'
                
                except:  
                    stock5,index5,dapan5=self.get5minData()            
                    stockQ=self.stockZqQ(stock5)
        #            stockZqChg=self.stockZqChg(stock5).loc[:,['hq_code','chgper']]
        #            stockZqChg.sort_values('chgper',inplace=True,ascending=False)
        #            chgRank=self.rank(stockZqChg,'chg',10)            
        #            stockZqTLDX=self.stockZqTldx(stock5,dapan5)
        #            stockZqTLDX.sort_values('hq_close',inplace=True)                    
                    
            
            else:
                stock5,index5,dapan5=self.get5minData()
                stockQ=self.stockZqQ(stock5)
                               
            stockQ.sort_values('Q',inplace=True)
            
            #tldxRank=self.rank(stockZqTLDX,'tldx',10)
            qRank=self.rank(stockQ,'q',10)
            #qRank['fgrade']=qRank['qgrade']*0.5+tldxRank['tldxgrade']*0.5          
            #df_rank=pd.concat([chgRank,zjRank,qRank],axis=1).dropna()
            
            #净额打分
            df_rank=pd.concat([zjRanknet,qRank],axis=1).dropna()
            df_rank['grade']=df_rank['qgrade']*0.3+df_rank['zjgrade']*0.7
            df_rank=self.getBoard(3000,df_rank)
            df_rank['hq_name']=self.getStockNames(df_rank,self.stockNames)
            #df_rank=df_rank.loc[:,['hq_name','board_name','chgper','bigD','bigper','fgrade','grade']]
            df_rank=df_rank.loc[:,['hq_name','board_name','amo','amopower','Q','grade']]
            
            #总量打分
            df_rank2=pd.concat([zjRanksum,qRank],axis=1).dropna()
            df_rank2['grade']=df_rank2['qgrade']*0.3+df_rank2['zjgrade']*0.7
            df_rank2=self.getBoard(3000,df_rank2)
            df_rank2['hq_name']=self.getStockNames(df_rank2,self.stockNames)
            df_rank2=df_rank2.loc[:,['hq_name','board_name','amo','amopower','Q','grade']]
                   
        else:   
            stock5,index5,dapan5=self.get5minData()
            
            stockZqChg=self.stockZqChg(stock5).loc[:,['hq_code','chgper']]
            stockZqChg.sort_values('chgper',inplace=True,ascending=False)
            chgRank=self.rank(stockZqChg,'chg',10)            
            df_rank=pd.concat([chgRank,zjRanksum],axis=1).dropna()
            df_rank['grade']=df_rank['chggrade']*0.5+df_rank['zjgrade']*0.5
            df_rank=self.getBoard(3000,df_rank)
            df_rank['hq_name']=self.getStockNames(df_rank,self.stockNames)
            df_rank=df_rank.loc[:,['hq_name','board_name','chgper','amo','amopower','grade']]
            df_rank['chgper']=df_rank['chgper']*100
            #df_rank['bigper']=df_rank['bigper']*100
               

        df_rank.sort_values(['grade','board_name'],inplace=True,ascending=False)    
        df_rank200=self.getRF(200,df_rank,boardFlag=1)
        df_rank30=self.getRF(30,df_rank,boardFlag=1)
         
        df_rank2.sort_values(['grade','board_name'],inplace=True,ascending=False)     
        df_rank2002=self.getRF(200,df_rank2,boardFlag=1)
        df_rank302=self.getRF(30,df_rank2,boardFlag=1)        
        
        df_rankI=df_rank.groupby('board_name').agg({'grade':'mean'})
        df_rankI2=df_rank2.groupby('board_name').agg({'grade':'mean'})
        df_rankI=df_rankI+df_rankI2
        df_rankI.reset_index(inplace=True)
        df_rankI.sort_values('grade',ascending=False,inplace=True)
        df_rankI.index=np.arange(len(df_rankI))
        
#        df_rank200.reset_index(inplace=True)    
#        df_rank30.reset_index(inplace=True)
        
        if factor==True:
            #df_rank200.columns=df_rank30.columns=df_rank.columns=['名称','板块','涨幅','大单净额','净额占比','异动级别','综合评分']
            df_rank200.columns=df_rank30.columns=df_rank.columns=['名称','板块','大单净额','大单效能','聪明钱','综合评分']
            df_rank2.columns=df_rank2002.columns=df_rank302.columns=['名称','板块','大单总量','大单效能','聪明钱','综合评分']
        else:
            df_rank200.columns=df_rank30.columns=df_rank.columns=['名称','板块','涨幅','大单净额','大单效能','综合评分']
            

#        df_rank['']=''
#        df_rank2['']=''
#        df_rankI['']=''
#        
#        df_rank.index=df_rank2.index=np.arange(len(df_rank))
#  
#        df_rank200['']=''
#        #df_rank200[' ']=''
#        
#        df_rank2002['']=''
#        #df_rank2002[' ']=''
#        
#        df_rank30['']=''
#        #df_rank30[' ']=''       
#        
#        df_rank200.index=np.arange(len(df_rank200))
#        df_rank30.index=np.arange(len(df_rank30))
#        
#        df_rank2002.index=np.arange(len(df_rank2002))
#        df_rank302.index=np.arange(len(df_rank302))        
#              
#        df_rank=pd.concat([df_rankI,df_rank,df_rank2,df_rank200,df_rank2002,df_rank30,df_rank302],axis=1)
#        
#        if factor==False: 
#            df_rank.to_csv(u'E:\\work\\报表\\全市场选股\\'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.3f')
#            #df_rankRF.to_csv(u'E:\\work\\报表\\全市场选股\\RF'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.3f')
#        else:
#            df_rank.to_csv(u'E:\\work\\报表\\全市场选股\\F'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.3f')
#            #df_rankRF.to_csv(u'E:\\work\\报表\\全市场选股\\FRF'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.csv',encoding='gbk',float_format='%.3f')            


        import xlsxwriter
        
        wbk=xlsxwriter.Workbook(u'E:\\work\\报表\\全市场选股\\F'+self.tstartdate.replace(':','时')+'至'+self.tenddate.replace(':','时')+'.xlsx')
        
        sheet1   = wbk.add_worksheet(u'data')
        
        boardNames=df_rankI['board_name'].drop_duplicates()
        
        width=len(df_rank.columns)+1
        
        left=0
        
        sheet1.write_row(0,left,df_rankI.columns)
        for i in xrange(len(df_rankI)):
            
            sheet1.write_row(i+1,left,df_rankI.iloc[i])
            
        left+=len(df_rankI.columns)+1
        sheet1=writeDf(boardNames,df_rank,left,sheet1)
        
        left+=width
        sheet1=writeDf(boardNames,df_rank2,left,sheet1)
        
        left+=width
        sheet1=writeDf(boardNames,df_rank200,left,sheet1)
        
        left+=width
        sheet1=writeDf(boardNames,df_rank2002,left,sheet1)
            
        left+=width
        sheet1=writeDf(boardNames,df_rank30,left,sheet1)       
        
        left+=width
        sheet1=writeDf(boardNames,df_rank302,left,sheet1)  

        wbk.close()          
        
if __name__=='__main__':
    
#    t1=time.time()
    #注意 纯日期不要打空格
#    c=ZH('2017-11-10','2017-11-17')
#    df_avg,avgChg=c.combAllchg()
#    c.buildJzRankForm(update=True,rankFlag=1,allFlag=0,networth=1)
#    
    c=ZH('2017-10-16','2017-11-17')
    #df_avg,avgChg=c.combAllchg()    
    c.buildJzRankForm(update=False,rankFlag=0,allFlag=0,networth=1)    
    #a,b=c.zjRank(c.tstartdate,c.tenddate)
    
    #c.buildForms()
    
    #amo=c.indexCashflow('2017-07-17 14:30','2017-07-17 15:00')
    #c.buildMinRankForm(factor=False)
    
#    stock,index,dapan=c.getDayData()
#    a,b=c.getAllChg(stock,dapan)
