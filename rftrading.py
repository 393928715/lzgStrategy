# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:41:04 2017

@author: Administrator
"""

from sqlalchemy import create_engine 

from datetime import datetime
from dateutil.parser import parse
import matplotlib.pyplot as plt
import os

import pandas as pd
import numpy as np

import colligation

import sys
sys.path.append("..")
reload(sys)
          
sys.setdefaultencoding('utf-8')

from DataHanle.MktDataHandle import MktStockHandle

import tushare as ts

#计算趋同度的类
class RF_TradingMonitor:
    
    def __init__(self):
        
        self.rf30  = u'E:/工作/标的/30.txt'
                
        self.rf200 = u'E:/工作/标的/200.txt'
        
        self.tdir  = u'E:/工作/PySAP-Master/Data/History/Stock/股票时间数据/股票交易日期/交易时间.txt'
        
        self.mktstock = MktStockHandle()    
        
        self.magin_sh = 'margintrading_sh'
        
        self.magin_sz = 'margintrading_sz'
        
        self.engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
    #获取所有交易时间   
    def getAllTrdingDate(self,startDate,endDate):
        
        #startDate=datetime.strptime(startDate, "%Y-%m-%d").date()
        #endDate=datetime.strptime(endDate, "%Y-%m-%d") .date()
        
        #生成目录字典
        sh_data=ts.get_hist_data('sh')
        
        sh_data=sh_data[(sh_data.index>startDate)&(sh_data.index<endDate)]
        
        sh_data.reset_index(inplace=True)

        allTimedf =sh_data['date']
        
        print allTimedf
        
        return allTimedf
             
    def getNewDate(self,start_date,dayoffset):
        
        #startDate = datetime.strptime(start_date, "%Y-%m-%d")  
        sh_data=ts.get_hist_data('sh')
        
        sh_data=sh_data[(sh_data.index<=start_date)]
        
        sh_data.reset_index(inplace=True)

        timelist =sh_data['date']     
        
        newdate   = start_date
        
        if len(timelist)>dayoffset:
           
           newdate =  timelist[dayoffset] 
           
           #newdate = datetime.strptime(start_date, "%Y-%m-%d")
        
        return newdate
               
        
    def getCompanySocklist(self):
                
        rf30dir  = self.rf30
        
        rf200dir = self.rf200
                     
        fheader = ['scode','sname']
        
        #生成目录字典
        rf30df =pd.read_table(rf30dir.decode('utf-8'),usecols=[0,1],header=0,names=fheader)
                
        rf200df =pd.read_table(rf200dir.decode('utf-8'),usecols=[0,1],header=0,names=fheader)
                              
        return rf30df,rf200df   
     
    def getStockData(self,scodes,startDate,endDate,klineType):
          
        mktstock =  self.mktstock
        
        mktdf = pd.DataFrame()
        
        mktdf = mktstock.MktStockBarHistDataGet(scodes,startDate,endDate,klineType)
                                
        mktdf['HL'] = abs(mktdf['hq_high'] - mktdf['hq_low'])
        
        mktdf['HCL'] = abs(mktdf['hq_high'] - mktdf['hq_close'])
        
        mktdf['CLL'] = abs(mktdf['hq_close'] - mktdf['hq_low'])
        
        mkdatagroup = mktdf.groupby('hq_code')
        
        mkdatadict  = dict(list(mkdatagroup))
        
        atrdf   = pd.DataFrame()
        
        for mkdict in mkdatadict:
            
            mkitemdf = mkdatadict[mkdict]
            
            tmpdf    = mkitemdf[['HL','HCL','CLL']]
            
            mkitemdf['atr'] = tmpdf.max(axis=1)
                        
            mkitemdf['atr'] =  pd.DataFrame(pd.rolling_mean(tmpdf.max(axis=1),window=6))
                      
    
            mkAtrdf  = mkitemdf[['hq_code','hq_date','atr']]
                        
            atr_ret   = mkAtrdf.set_index(['hq_code','hq_date'])
            
            uatr_ret = atr_ret.unstack()['atr'] # 获取个股历史收益率
            
            atrdf = atrdf.append(uatr_ret)
            
            #mkdata = pd.DataFrame(pd.rolling_mean(mkdata.max(axis=1),window=6))

        atrdf = atrdf.fillna(0)
                        
        return atrdf
    
    #处理融资融券数据    
    def dealMarginData(self,startDate,endDate):
        
        dbtable_sh = self.magin_sh
        
        dbtable_sz = self.magin_sz
        
        engine     = self.engine
        
        #生成目录字典
        
        allTimedf  = self.getAllTrdingDate(startDate,endDate)
        
        if len(allTimedf)>0:
            
            allTimelist = list(allTimedf)
            
            #获取每天融资融券数据
            for atl in  allTimelist:
                    
              #上海融资融券数据
              marginSh_df = pd.DataFrame()
                
              #深圳融资融券数据       
              marginSz_df = pd.DataFrame()
                
              #深圳融资融券数据       
              marginTmp_df = pd.DataFrame()
        
              #获取沪市融资融券汇总数据
              allsh_df= ts.sh_margins(start=atl, end=atl)
                            
              marginTmp_df['mt_rzye']  = allsh_df['rzye']
              marginTmp_df['mt_rzmre'] = allsh_df['rzmre']
              
              marginTmp_df['mt_rzche'] = 0
              marginTmp_df['mt_rqyl']  = allsh_df['rqyl']
              
              marginTmp_df['mt_rqylje']= allsh_df['rqylje']
              marginTmp_df['mt_rqmcl'] = allsh_df['rqmcl']
              
              marginTmp_df['mt_rqchl'] = 0
              marginTmp_df['mt_rzrqjyzl'] =allsh_df['rzrqjyzl']
              
              marginTmp_df['mt_code']  = '000002' 
              marginTmp_df['mt_name']  = u'上海A股' 
              marginTmp_df['mt_date']  = atl
              
              marginSh_df = marginSh_df.append(marginTmp_df)
              
              #获取沪市融资融券个股数据
              allsh_df= ts.sh_margin_details(start=atl, end=atl) 
              
              marginTmp_df = pd.DataFrame()
                           
              marginTmp_df['mt_code']  = allsh_df['stockCode'] 
              marginTmp_df['mt_name']  = allsh_df['securityAbbr'] 
              marginTmp_df['mt_date']  = allsh_df['opDate']
              
              marginTmp_df['mt_rzye']  = allsh_df['rzye']
              marginTmp_df['mt_rzmre'] = allsh_df['rzmre']
              
              marginTmp_df['mt_rzche'] = allsh_df['rzche']
              marginTmp_df['mt_rqyl']  = allsh_df['rqyl']
              
              marginTmp_df['mt_rqylje']= 0
              marginTmp_df['mt_rqmcl'] = allsh_df['rqmcl']
              
              marginTmp_df['mt_rqchl'] = allsh_df['rqchl']
              marginTmp_df['mt_rzrqjyzl'] =0
              
              marginSh_df = marginSh_df.append(marginTmp_df)
              
              marginSh_series =  marginSh_df['mt_date'].astype('str')
              
              marginSh_index  =  marginSh_series.values
              
              marginSh_df = marginSh_df.set_index(marginSh_index)
              
              #获取深市融资融券汇总数据
              allsz_df= ts.sz_margins(start=atl, end=atl)
              
              
              marginTmp_df = pd.DataFrame()
             
              marginTmp_df['mt_rzmre'] = allsz_df['rzmre'] 
              marginTmp_df['mt_rzye']  = allsz_df['rzye']
              
              
              marginTmp_df['mt_rqmcl'] = allsz_df['rqmcl']              
              marginTmp_df['mt_rqyl']  = allsz_df['rqyl']
              
              marginTmp_df['mt_rqye']  = allsz_df['rqye'] 
              marginTmp_df['mt_rzrqye']= allsz_df['rzrqye'] 
              
              marginTmp_df['mt_code']  = '399107' 
              marginTmp_df['mt_name']  = u'深圳A股' 
              marginTmp_df['mt_date']  = atl
              
              marginSz_df = marginSz_df.append(marginTmp_df)
              
              #获取深市融资融券个股数据
              allsz_df= ts.sz_margin_details(atl)
              
              #清空tmp表              
              marginTmp_df = pd.DataFrame()
              
              
              marginTmp_df['mt_code']  = allsz_df['stockCode'] 
              marginTmp_df['mt_name']  = allsz_df['securityAbbr'] 
              marginTmp_df['mt_date']  = allsz_df['opDate']
              
              marginTmp_df['mt_rzmre'] = allsz_df['rzmre'] 
              marginTmp_df['mt_rzye']  = allsz_df['rzye']
              
              
              marginTmp_df['mt_rqmcl'] = allsz_df['rqmcl']              
              marginTmp_df['mt_rqyl']  = allsz_df['rqyl']
              
              marginTmp_df['mt_rqye']  = allsz_df['rqye'] 
              marginTmp_df['mt_rzrqye']= allsz_df['rzrqye'] 
                            
              
              marginSz_df = marginSz_df.append(marginTmp_df)
                            
              marginSz_series = marginSz_df['mt_date'].astype('str')
              
              marginSz_index  =  marginSz_series.values
              
              marginSz_df = marginSz_df.set_index(marginSz_index)
              
              
              marginSh_df.to_sql(dbtable_sh,con=engine,if_exists='append')
              
              marginSz_df.to_sql(dbtable_sz,con=engine,if_exists='append')
              
              print atl
        
        m =1 
        
    def get_SH_Margins(self,startDate,endDate):
        
        margin_engine  = self.engine
                
        sTime_Start = '00:00:00'
        
        sTime_End   = '00:00:00'
        
        strstart = ' UNIX_TIMESTAMP(\''+startDate +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+endDate +' '+sTime_End+'\')'
        
        sqlstr = "select * from margintrading_sh where UNIX_TIMESTAMP(mt_date)>="+strstart +" and UNIX_TIMESTAMP(mt_date)<=" +strend 
            
        print sqlstr        
        
        with margin_engine.connect() as conn, conn.begin():
            rzye_df = pd.read_sql_query(sqlstr, conn)
                
        return rzye_df
   
    def get_SZ_Margins(self,startDate,endDate):
                
        margin_engine  = self.engine
        
        sTime_Start = '00:00:00'
        
        sTime_End   = '00:00:00'
        
        strstart = ' UNIX_TIMESTAMP(\''+startDate +' ' + sTime_Start+'\')'
        
        strend = 'UNIX_TIMESTAMP(\''+endDate +' '+sTime_End+'\')'
        
        sqlstr = "select * from margintrading_sz where UNIX_TIMESTAMP(mt_date)>="+strstart +" and UNIX_TIMESTAMP(mt_date)<=" +strend 
            
        print sqlstr   
        
        with margin_engine.connect() as conn, conn.begin():
            rzye_df = pd.read_sql_query(sqlstr, conn)
                
        return rzye_df
   
        
#        
#    def getIndex_ATR(self,clist,startDate,endDate,klineTyep):
#        
#        MktStockBarHistDataGet
#        
#
#data = DataAPI.MktEqudAdjGet(ticker=u"000001",beginDate=u"20150101",endDate=u"20160701",field=u"tradeDate,highestPrice,lowestPrice,preClosePrice",pandas="1")
#2
#data.set_index('tradeDate',inplace=True)
#3
#data['HL'] = abs(data['highestPrice'] - data['lowestPrice'])
#4
#data['HCL'] = abs(data['highestPrice'] - data['preClosePrice'])
#5
#data['CLL'] = abs(data['preClosePrice'] - data['lowestPrice'])
#6
#data = data[['HL','HCL','CLL']]
#7
#data = pd.DataFrame(pd.rolling_mean(data.max(axis=1),window=6))
#8
#data.columns = ['MAX']
#9
#​
#10
#final_data = data.merge(data_atr, left_index = True,right_index = True,how='outer')
#11
#final_data.plot()


    #最终调用 atr_30，atr_200用于atr指标
    #最终调用 rzye30_df，rzye200_df用于融资融券指标
    def getTrading(self,sdate,edate,marginflag):
          
        #ATR指标
         
        newdate   = self.getNewDate(sdate,6)
        
            
        #获取所有股票数据
        rfcodestr = '>=000001 '
        
        rfall_atr = self.getStockData(rfcodestr,newdate,edate,'D')
        
        atrall_colunms = list(rfall_atr.columns)
        
        #获取最后一个数据
        atr_all  = rfall_atr[[atrall_colunms[-1]]]
                
        atr_all['hq_code']  = atr_all.index
               
        atr_allindexlist  = range(len(atr_all))
        
        atr_all.rename(columns={atrall_colunms[-1]: 'atr'}, inplace=True)        
        atr_all.sort_values(by=['atr'],ascending=False, inplace=True)
        
        atr_all.index  = atr_allindexlist
               
        # 处理融资余额
        
#        startDate = start_date.strftime('%Y-%m-%d')
#        
#        endDate   = end_date.strftime('%Y-%m-%d')
        
        #self.get_SH_Margins(startDate,endDate,rf30codestr)
        
        if marginflag==True:
           self.dealMarginData(sdate,edate)
            
        #处理上海融资融券数据
            
        shrzye_df  = self.get_SH_Margins(sdate,edate)
             
       #处理深圳融资融券数据
        
        szrzye_df  = self.get_SZ_Margins(sdate,edate)
    
        #计算所有融资余额
        szrzye_group  = szrzye_df.groupby('mt_code')
        
        shrzye_group  = shrzye_df.groupby('mt_code')
                
        szrzye_dict   = dict(list(szrzye_group))
        
        shrzye_dict  =  dict(list(shrzye_group))
        
        rzye_dict = dict( szrzye_dict, **shrzye_dict )
         
        rzye_series    = pd.Series()
        
        for rdict  in  rzye_dict:
            
            dictItem  =  rzye_dict[rdict]
            
            if len(dictItem)>=1:
                
                headItem =  dictItem.head(1)
                
                tailItem =  dictItem.tail(1)
                
                headItem.set_index(headItem['mt_code'],inplace=True)
                
                tailItem.set_index(tailItem['mt_code'],inplace=True)
                
                tmpItem =  (tailItem['mt_rzye']-headItem['mt_rzye']) /10000
                
                rzye_series =  rzye_series.append(tmpItem)
                
        rzye_df    = rzye_series.to_frame('mt_rzye')
         
        rzye_df['mt_code'] = rzye_df.index
            
        rzye_df.sort_values(by=['mt_rzye'],ascending=False, inplace=True)
        
        rzyeindexlist  = range(len(rzye_df))
        
        rzye_df.index  = rzyeindexlist
        
        rzye_df.rename(columns={'mt_code': 'hq_code'}, inplace=True)
        
        #获取30,200标的的数据
        c=colligation.ZH(sdate,edate)
        
        rzye30_df=c.getRF(30,rzye_df)   
        rzye200_df=c.getRF(200,rzye_df)    
        
        atr_30=c.getRF(30,atr_all)   
        atr_200=c.getRF(200,atr_all)  
        
        return rzye200_df,rzye30_df,atr_200,atr_30,rzye_df,atr_all

if '__main__'==__name__:  
    c=RF_TradingMonitor()
    c.getTrading('2017-07-07','2017-07-14',0)


    

     
     
     
     
     
     
     
     
     
     
     