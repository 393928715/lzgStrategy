# -*- coding: utf-8 -*-
"""
Created on Mon Jun 05 13:27:20 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys)

from DataHanle.MktDataHandle import MktIndexHandle

from DataHanle.MktDataHandle import MktStockHandle

from sqlalchemy import create_engine 

import datetime

import time

import pandas as pd

import numpy as np

from Pic import cmq240

from Pic.tldxday import picExcel



class tldxcmq():
    def __init__(self):
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        self.tstartdate=datetime.datetime(2017,5,10)      
        self.tenddate=datetime.datetime(2017,6,2)        
        
    
    def calculate(self):
            
    #    apply函数，计算smartS
        def jsS(x):
            x['smartS']=np.abs(x['chgper'])/np.sqrt(x['hq_vol'])*100000
            x.sort_values(by=['smartS'], ascending=[0],inplace=True) 
            x['accumVolPct']=x['hq_vol'].cumsum()/x['hq_vol'].sum()
            return x         

        #计算Q因子
        def jsQ(x):
            totalvol=x['hq_vol'].sum()
            try:           
                VWAPall=(x['hq_close']*x['hq_vol']).sum()/totalvol
                y=x[x.accumVolPct<=0.2]
                smartvol=y['hq_vol'].sum()
                VWAPsmart=(y['hq_close']*y['hq_vol']).sum()/smartvol 
                x['Q']=VWAPsmart/VWAPall
                x=x[['hq_name','Q','hq_code']].head(1)
                return x                   
            except ZeroDivisionError:
                pass    
        
        #特立独行处理函数，用于apply
        def tldx(x,series):
            if not x.any():
                xishu=np.nan
            else:
                nx=x.shift()
                bl=nx/x
                y=np.log(bl).dropna() 
                y.index = np.arange(len(y))   
                try:
                    xishu=(np.mean(series*y)-np.mean(series)*np.mean(y))/(np.std(series)*np.std(y))
                except:
                    xishu=10
            return np.abs(xishu)      
    
        def getCode():
    #        def inttostr(x):
    #            if len(x)<6:
    #                if 6-len(x)==3:
    #                    x='000'+x
    #                elif 6-len(x)==2:
    #                    x='00'+x
    #                elif 6-len(x)==1:
    #                    x='0'+x
    #                elif 6-len(x)==4:
    #                    x='0000'+x
    #                else:
    #                    x='00000'+x
    #            return x
            #代码文件目录
            fname=r'E:\test\code\code.txt'
            
            s1=pd.read_table(fname,usecols=[0],names=['code'],dtype=str)
                    
            codelist=s1['code'].astype('int')
            
            return codelist
               
        #定义apply函数，获得版块每天的涨幅比
        def zf(x):
            nx=x.shift()
            bl=nx/x
            index_list=np.log(bl).dropna().tolist()             
            return index_list
        
        def xdchg(x,indexchg):
            if len(x)!=240:  
                print x
                return x
            else:
                indexchg.index=x.index
                x=x-indexchg
                return x
        
        t=time.time()
        
        
        mkstock = MktStockHandle()
        
        mkindex = MktIndexHandle()
               
        #定义起始终止日期
        
        tstartdate=self.tstartdate
        
        tenddate=self.tenddate
        
        #取全市场股票的where条件
        allcode='>=000001'
        
        #获取板块股票关联信息
        
        stockrelated=pd.read_sql_table('boardstock_related',con=self.engine,columns=['board_name','stock_id'],schema='stocksystem')
        
        stockrelated['cFlag']=stockrelated['board_name'].str.contains(u'通达信行业-')
        
        stockrelated=stockrelated[stockrelated['cFlag']]
        
        stockrelated['board_name']=stockrelated['board_name'].apply(lambda x:x[6:])        
        
        #获得股票名称
        #stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])
        
        #取200支标的股票代码列表，用于在dataframe中筛选
        code200=getCode()
        
        #取得起始日至终止日的所有版块数据
        index =mkindex.MktIndexBarHistDataGet('399317',sDate_Start=tstartdate,sDate_End=tenddate,speriod='1M')
        
        #按日期groupby,计算每天的涨幅比
        index['chg'] = index['hq_close'].groupby(index['hq_date']).diff()
        index['preclose']=index['hq_close'].groupby(index['hq_date']).shift()
        index['chgper']=index['chg']*100/index['preclose']
        index=index[['hq_date','hq_close','chgper']].groupby(index['hq_date']).agg({'hq_date':'first','chgper':lambda x:x.tolist(),'hq_close':lambda x:zf(x)})
        
        #得到日期序列，用于EXCEL作图
        dateList=index['hq_date'].apply(lambda x:x.strftime( '%Y-%m-%d' ))
            
        #遍历每一天，处理数据   
        dataList=[[],[]]
        QList=[[],[]]
        for tstartdate in index['hq_date']:     
            t1=time.time()             
            SList=[[],[]]
            chgList=[[],[]]
            timeList=[]
            #获取当天板块的涨幅序列
            index_price=index[index.hq_date==tstartdate]['hq_close']
            index_price=pd.Series(index_price[0])                      
            #得到全市场股票的dataframe
            stockList        = mkstock.MktStockBarHistDataGet(allcode,tstartdate,tstartdate,"1M")
                    
            #拼接股票代码与名称
    #        stockList['hq_code']=stockList['hq_code'].astype('int')
    #        stockList.index=stockList.hq_code
    #        stockList['hq_name']=stockNames['stock_name']
            
            #stockList.dropna(inplace=True)
            stockList['chg'] = stockList['hq_close'].groupby(stockList['hq_code']).diff()
            stockList['preclose']=stockList['hq_close'].groupby(stockList['hq_code']).shift()
            stockList['chgper']=stockList['chg']*100/stockList['preclose']
            index_chgper=index[index.hq_date==tstartdate]['chgper']
            index_chgper=pd.Series(index_chgper[0])  
            stockList['xdchgper']=stockList['chgper'].groupby(stockList['hq_code']).apply(lambda x:xdchg(x,index_chgper))
            
            stockList        = (stockList.groupby(stockList['hq_code']).apply(jsS)) 
            
            #分钟级别监控部分
            
#            sdata=stockList[(stockList.hq_vol>1000)&(stockList.smartS!=0)]
#            #(stockList.hq_vol>1000)&
#            
#            #筛选出200支标的
#            cFlag=sdata['hq_code'].isin(code200)        
#            sdata['cFlag']=cFlag
#            data200=sdata[sdata['cFlag']]        
#            
#            data200=data200[['hq_name','smartS','chgper','xdchgper']].groupby(data200['hq_time'])
#            sdata=sdata[['hq_name','smartS','chgper','xdchgper']].groupby(sdata['hq_time'])
#            
#            #添加全市场数据
#            num=0
#            for time1,data in sdata:
#                #因为通达信数据延迟一分钟，此处平移数据对齐正确时间
#                num+=1
#                if num==1:
#                    timeList.append(str(time1).split()[-1])                
#                    continue
#                if num==len(sdata):
#                    chgPer=data.sort_values(by=['chgper'], ascending=[0])   
#                    chgList[0].append(chgPer)           
#                    data.sort_values(by=['smartS'], ascending=[0],inplace=True)           
#                    SList[0].append(data)
#                    break
#                timeList.append(str(time1).split()[-1]) 
#                chgPer=data.sort_values(by=['chgper'], ascending=[0])   
#                chgList[0].append(chgPer)           
#                data.sort_values(by=['smartS'], ascending=[0],inplace=True)           
#                SList[0].append(data)
#                if num==238:
#                    print num
#                
#                
#            #添加200标的数据
#            num=0
#            for time1,data in data200:
#                num+=1
#                if num==1:
#                    continue            
#                chgper200=data.sort_values(by=['chgper'], ascending=[0])   
#                chgList[1].append(chgper200)         
#                data.sort_values(by=['smartS'], ascending=[0],inplace=True)
#                SList[1].append(data)
#      
#            
#            tstartdate=tstartdate.strftime( '%Y-%m-%d' )
#            p=cmq240.picExcel(timeList,SList,tstartdate,chgList)#,chgList
#            p.picModel()
            #p.update()
    
    
            #日级别监控部分
            #计算Q因子
            
            
            Qdata = stockList.groupby(stockList['hq_code']).apply(jsQ)
            
            Qdata.sort_values(by=['Q'],inplace=True)
            
            #得到股票所属板块
            stockrelated.index=stockrelated.stock_id
            
            stockList.index=stockList.hq_code
            stockList['board_name']=stockrelated['board_name']   
            
            Qdata.index=Qdata.hq_code
            Qdata['board_name']=stockrelated['board_name']               
            
            
            
            Qdata['cFlag']=Qdata.hq_code.isin(code200)       

            Qdata200=Qdata[Qdata['cFlag']]  
            
            QList[0].append(Qdata)
            QList[1].append(Qdata200)
                 
            #处理全市场数据                     
            dayData=stockList[['hq_name','hq_close','hq_code','board_name']].groupby(stockList['hq_code']).agg({'hq_name':'first','hq_close':lambda x:tldx(x,index_price),'hq_code':'first','board_name':'first'})
            
            dayData=dayData.sort('hq_close').dropna()
                      
            dayData['cFlag']=dayData.index.isin(code200)       

            dayData200=dayData[dayData['cFlag']]
    
            #dayData=dayData.head(300)
            dataList[0].append(dayData)
            dataList[1].append(dayData200)
            
            print '一个周期'
            print time.time()-t1
         
        #画EXCEL

      
        p=picExcel(dataList,dateList,QList)
        p.picModel()
        p.update()
        
        print 'OVER'
        print time.time()-t

if __name__=='__main__':
    S=tldxcmq()
    S.calculate()