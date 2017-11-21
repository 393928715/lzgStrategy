# -*- coding: utf-8 -*-
"""
Created on Mon May 22 11:20:51 2017

@author: rf_hs
"""
import datetime
from dateutil.parser import parse
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

import sys
sys.path.append("..")
reload(sys)
          
sys.setdefaultencoding('utf-8')

from DataHanle.MktDataHandle import MktIndexHandle

from DataHanle.MktDataHandle import MktStockHandle

from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei'] # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题
mpl.rcParams['font.size'] =16



#计算趋同度的类
class ConvergenceStrategy:
    
    def __init__(self):
        pass;
    

    def MktIndexComponent(self,index_symbol,start_date,end_date):
    
        """获取动态股票池行情"""
    
#        trade_calendar = get_trade_calendar(start_date,end_date) 

        mktindex = MktIndexHandle()
    
        mktstock = mktindex.mktstock    
     
        trade_calendar = mktindex.MkIndexAllTradingTime()
    
        df_eq = pd.DataFrame()
        
        #先不考虑时间的，做完之后再扩展
        
        #取出指数关联的股票
        universe = mktindex.MktIndexToStocksClassify(index_symbol)
        
        tcalendarlist = trade_calendar['Time'].tolist()
        
        ustockcodelist =universe['stock_id'].tolist()

        def getOneMinutesChg(df):   
            
            df['hq_pclose'] = df['hq_close'].shift()
            
            df['hq_chg']    = (df['hq_close']/df['hq_pclose'] -1)*100
            
            return df[['hq_code','index','hq_chg']]
                
        #计算拟合度  
        def rsquare(x,y):
            ##计算拟合优度R2
            meanofx=x.mean()
            meanofy=y.mean()
            difofxx=x - meanofx
            difofyy=y - meanofy 
            beta = sum(difofxx * difofyy) / sum(difofxx ** 2)
            r2 = beta**2 * sum(difofxx**2) / sum(difofyy**2)
            return r2
            
        #计算趋同度       
        def window_convergence(x,data):
            # 计算趋同度
            test = map(int,x)
            
            window_data =  data.iloc[map(int,x)]
            #window_data = window_data.dropna(axis=1)
            r2_list = []
            
            print x
            
            # print list(window_data.columns)
            for col in window_data.columns:
                if col != 'hq_chg':
                    r2_list.append(rsquare(window_data['hq_chg'],window_data[col]))
            #print np.nanmean(r2_list)
            return np.nanmean(r2_list) 
            #取出指定时间每分钟数据
            
            
    #    #画趋势图与指数行情图    
        def convergence_index_plot(convergence_data,df_index,start_date,end_date):
            
        
            #画趋势图与指数行情图
        
#            start_date = start_date if isinstance(start_date,datetime.datetime) else parse(start_date)
#        
#            end_date = end_date if isinstance(end_date,datetime.datetime) else parse(end_date)
        
            convergence_data_sample = convergence_data[(convergence_data['tradeDate']>=start_date) & (convergence_data['tradeDate']<=end_date)]
            
            start_date=start_date.date()
            
            end_date=datetime.datetime.strptime(end_date,'%Y-%m-%d').date()
        
            df_index_sample = df_index[(df_index['hq_date']>=start_date) & (df_index['hq_date']<=end_date)]
        
            fig = plt.figure(figsize=(18,7))
        
            ax = fig.add_subplot(111)
        
            ax2=ax.twinx()
        
            ax.plot(df_index_sample['hq_date'].values,df_index_sample['hq_close'].values,'darkgoldenrod',linewidth=2.5)
        
            ax2.plot(convergence_data_sample['tradeDate'].values,convergence_data_sample['convergence_30'].values,'crimson',linewidth=2.5)
        
            ax.legend([u"国证A指"],loc='upper left',bbox_to_anchor=(0.01,0.99),frameon= False,handlelength = 3,handletextpad=0.1,borderpad = 0.1)
        
            ax2.legend([u"国证A指趋同度"],loc='upper left',bbox_to_anchor=(0.01,0.92),frameon= False,handlelength = 3,handletextpad=0.1,borderpad = 0.1)
        
            ax.spines['top'].set_color('none')
        
            ax2.spines['top'].set_color('none')
        
            ax.xaxis.set_ticks_position('bottom')
        
            # plt.ylim(df_index_sample['closeIndex'].min(),df_index_sample['closeIndex'].max())
        
            ax.set_title(u"图1 国证A指趋同度")              
            
        
              
            
        #获取每天数据
        df_tmp = mktstock.MkStockBarHistOneDayGet('',start_date,end_date)
           
          # df_tmp.index=df_tmp.hq_date
           
        df_index = mktindex.MktIndexBarHistOneDayGet(index_symbol,start_date,end_date)
       
       #df_index.index=df_index.hq_date               
       
#               df_index['hq_pclose'] = df_index['hq_close'].shift(1)
#               
#               df_index['hq_chg']    = (df_index['hq_close']/df_index['hq_pclose'] -1)*100
#               
#               df_indexret = df_index[['hq_code','index','hq_chg']]

        df_indexret=df_index.groupby('hq_code').apply(getOneMinutesChg)       
      
        df_ret   = df_tmp.groupby('hq_code').apply(getOneMinutesChg)
       
        df_ret   = df_ret.set_index(['index','hq_code'])
        
#        print df_ret
          
        eq_ret = df_ret.unstack()['hq_chg'] # 获取个股历史收益率
       
        df_data = pd.concat([eq_ret,df_indexret.set_index(['index'])['hq_chg']],axis=1)
       
        df_data['ii'] = range(len(df_data))
        
        df_data.fillna(0,inplace=True)
       
        convergence_oneday = pd.rolling_apply(df_data.ii, 30, lambda x: window_convergence(x, df_data))
        
        convergence_oneday=convergence_oneday.to_frame('convergence_30')
        
        convergence_oneday['tradeDate']=convergence_oneday.index
        
        start_date=datetime.datetime.strptime(start_date,'%Y-%m-%d')++datetime.timedelta(30)
        
        convergence_index_plot(convergence_oneday,df_index,start_date,end_date)
             
        m =1
               
#                 
#               df_dict = dict(list(df_group))
#               
#               
#               for dfdict in  df_dict:
#                   
#                   df_ditem= df_dict[dfdict]
                   
                   
               
#               
#               #遍历所属板块的股票
#               for ustocks in ustockcodelist:
#                   #如果是成分股则处理
#                   if df_dict.has_key(ustocks):
#                       
#                       df_ditem= df_dict[ustocks]
#                       
#                       
#                       pass;
                      
               
#        print df_group.describe()
#           
#        df_tmp['hq_chg']  = df_tmp['hq_close'].diff()
               

        # 按天来取出计算
        
#                
#        for date in trade_calendar:
#    
#            universe = self.MktIndexToStocksClassify(index_symbol,date)
#    
#            df_tmp = DataAPI.MktEqudAdjAfGet(secID=universe,tradeDate = date,
#    
#                                    field=u"secID,tradeDate,preClosePrice,closePrice,openPrice",pandas="1")
#    
#            df_eq = pd.concat([df_eq,df_tmp],axis=0)
    
       # return df_eq


if '__main__'==__name__:    

#    startyear  = 2017
#    
#    startmonth = 6
#    
#    startday   = 1
#    
#    tstartdate=(datetime.datetime(startyear,startmonth,startday)) 
    tstartdate='2017-06-01'
#    endyear  = 2017
#    
#    endmonth = 8
#    
#    endday   = 4
#    
#    tenddate=(datetime.datetime(endyear,endmonth,endday)) 
    tenddate='2017-09-01'
    
    cStrategy = ConvergenceStrategy()
    
    df_eq = cStrategy.MktIndexComponent('399317',tstartdate,tenddate)
    
   
    #取出指数中所有股票数据，需动态股票数据，主要指数可以实现，暂时按最新股票计算
    #主要指数均为半年变一次成分
    
    
#def get_trade_calendar(start_date,end_date):
#
#    """获取交易日历列表"""
#
#    trade_date = DataAPI.TradeCalGet(exchangeCD=u"XSHG",beginDate=start_date,endDate=end_date,field=u"isOpen,calendarDate",pandas="1")
#
#    trade_calendar = trade_date[trade_date['isOpen']==1]
#
#    trade_calendar['calendarDate'] = trade_calendar['calendarDate'].map(lambda x:x.replace('-',''))
#
#    trade_calendar = trade_calendar['calendarDate'].tolist()
#
#    return trade_calendar
#
#trade_calendar = get_trade_calendar('20110803','20170331')
#
#df_eq = get_component_return('000985','20110803','20170331') 
#
#df_eq['eq_ret'] = df_eq['closePrice'] / df_eq['preClosePrice'] - 1
#
#df_eq['eq_ret'] = df_eq.apply(lambda x: np.NaN if x['openPrice']==0 or np.isnan(x['openPrice']) else x['eq_ret'],axis=1) #获取个股收益率，停牌则设为缺失值
#
#data = pd.concat([eq_ret,df_index.set_index(['tradeDate'])['index_ret']],axis=1)
#
#data['ii'] = range(len(data))
#
#    
#
#convergence_30 = pd.rolling_apply(data.ii, 30, lambda x: window_convergence(x, data))
#convergence_30.to_csv('convergence_30.csv')
#
#convergence_data = pd.read_csv('convergence_30.csv',header=None)
#convergence_data = convergence_data.rename(columns={0:'tradeDate',1:'convergence_30'})
#convergence_data['tradeDate'] = convergence_data['tradeDate'].map(lambda x:parse(x))
#
#df_index = DataAPI.MktIdxdGet(ticker=u"000985",beginDate=u"20110803",endDate=u"20170331",field=u"tradeDate,preCloseIndex,closeIndex",pandas="1")
#df_index['index_ret'] = df_index['closeIndex'] / df_index['preCloseIndex'] - 1
#df_index['tradeDate'] = df_index['tradeDate'].map(lambda x:parse(x))





