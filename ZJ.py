# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 15:03:22 2017

@author: Administrator
"""
import sys
sys.path.append("..")
reload(sys)

import pandas as pd
import numpy as np
import os
#import time
from sqlalchemy import create_engine
#from PlotData.plotexcel import plotToExcel
import colligation# import ZH
import glob
import xlsxwriter
import tushare as ts
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class ZJ():
    def __init__(self):
        self.dir=u'E:\\work\\tick\\'
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        #储存每日资金流的CSV文件目录
        self.datadir=u"E:/work/数据备份/资金流/*.csv"      
        #self.cashflowdir==u"E:/work/报表/资金流/全市场"
    
    #按成交量划分10个类别,并把基础数据导出到CSV
    def tickTradeType(self,sdate,edate):
        names=['time','close','vol']
        fdirlist=os.listdir(self.dir)
        #筛选日期范围内数据
        sindex=fdirlist.index(sdate)
        eindex=fdirlist.index(edate)
        fdirlist=fdirlist[sindex:eindex+1]  
        for fdir in fdirlist:
            date=fdir
            fdir=os.path.join(self.dir,fdir)    
            flist=os.listdir(fdir)
            lastf=flist[-1]
            s2=pd.DataFrame()
            n=0
            for f in flist:
                fname=os.path.join(self.dir,fdir,f)
                try:
                    s1=pd.read_table(fname,names=names,dtype={'time':str,'close':int,'vol':int}).dropna()
                except Exception as e:
                    print e
                    print f
                    s1=pd.read_table(fname,names=names,dtype={'time':str}).dropna()
                    s1['close']=s1['close'].astype(int)
                    s1['vol']=s1['vol'].astype(int)
                    
                #计算amo
                s1['amo']=s1['vol']*s1['close']/10000000000
                  

                s1['datetime']=pd.to_datetime(date+' '+s1['time'].apply(lambda x:(x[:-4]+':'+x[-4:-2]).strip()))
                    
                #分类
                vbig=s1[np.abs(s1.amo)>=0.01000000]
                vbigs=vbig[vbig.vol<0]
                vbigb=vbig[vbig.vol>0]
                
                lbig=s1[(np.abs(s1.amo)<0.01000000)&(np.abs(s1.amo)>=0.00500000)]
                lbigs=lbig[lbig.vol<0]
                lbigb=lbig[lbig.vol>0]
                
                
                
                big=s1[(np.abs(s1.amo)<0.00500000)&(np.abs(s1.amo)>=0.00100000)]
                bigs=big[big.vol<0]            
                bigb=big[big.vol>0]
#                
#                mid=s1[(s1.amo<100000)&(s1.amo>=30000)]
#                mids=mid[mid.vol<0]            
#                midb=mid[mid.vol>0]
#                
#                small=s1[(s1.amo<30000)]
#                smalls=small[small.vol<0]
#                smallb=small[small.vol>0]
                
                #amo换算成亿为单位
                #s1['amo']=s1['amo']/100000000
                #amosum=s1['amo'].sum()
                
                vbigs['type']='vbigS'            
                vbigb['type']='vbigB'  
                     
                lbigs['type']='lbigS'
                lbigb['type']='lbigB'
                
                bigs['type']='bigS'
                bigb['type']='bigB' 
#                    
#                mids['type']='midS'
#                midb['type']='midB'
#                
#                smalls['type']='smallS'
#                smallb['type']='smallB'
                         
                #拼接分类的series，赋值给总数据                         
#                zjtype=pd.concat([vbigs['type'],vbigb['type'],lbigs['type'],lbigb['type'],bigs['type'],bigb['type'],mids['type'],midb['type'],smalls['type'],smallb['type']])
#                s1['type']=zjtype  
                         
                s1=pd.concat([vbigs,vbigb,lbigs,lbigb,bigs,bigb])
                 
                code=f[2:-4]             
                
                #if mflag==1:
                s1['hq_code']=code
                del s1['close'],s1['time'],s1['vol']
                s2=s2.append(s1)
                n+=1
                print fname
                if n%5==0:
                    s2.to_sql('bigcashflow',con=self.engine,if_exists='append',index=None)
                    s2=pd.DataFrame()
                elif f==lastf:
                    s2.to_sql('bigcashflow',con=self.engine,if_exists='append',index=None)

#                s2=s1.shift()
#                sg=s2[s2.close<s1.close]
#                xj=s2[s2.close>s1.close]
#                sg['type']='up'
#                xj['type']='down'
#                s1['type']=pd.concat([sg['type'],xj['type']])
#                s1.sort_index(inplace=True)
#                s1.fillna(0,inplace=True) 

    
    
    #按成交额大小排序
    def sortAmo(self,s1):
#        vbigs=s1.loc['vbigS'].sort_values('amo',ascending=False).set_index('code')
#        lbigs=s1.loc['lbigS'].sort_values('amo',ascending=False).set_index('code')
#        vbigb=s1.loc['vbigB'].sort_values('amo',ascending=False).set_index('code')
#        lbigb=s1.loc['lbigB'].sort_values('amo',ascending=False).set_index('code')
#        vbigs['name']=stockNames['stock_name']    
#        vbigb['name']=stockNames['stock_name']
#        lbigs['name']=stockNames['stock_name']    
#        lbigb['name']=stockNames['stock_name']    
#        bigs['name']=stockNames['stock_name']
#        bigb['name']=stockNames['stock_name']           
        
#        bigs=(vbigs+lbigs).sort_values('amo',ascending=False)
#        bigb=(vbigb+lbigb).sort_values('amo',ascending=False)
#        vbigs=vbigs.dropna().head(30) 
#        vbigb=vbigb.dropna().head(30)
#        lbigs=lbigs.dropna().head(30)
#        lbigb=lbigb.dropna().head(30)
#        bigs=bigs.dropna().head(30)
#        bigb=bigb.dropna().head(30)

        s1.sort_values('vbigper',ascending=False,inplace=True)
        
        c=colligation.ZH()
        s2=c.getRF(30,s1)   
        s1=s1[s1.vbigB>0.1]
        #s1.index=np.arange(len(s1))   
        vbiglist=list(s1['vbigD'].sort_values(ascending=False).head(5).index)
        lbiglist=list(s1['lbigD'].sort_values(ascending=False).head(5).index)
        biglist=list(s1['bigD'].sort_values(ascending=False).head(5).index)
        #sigindex=set(vbiglist+lbiglist+biglist)
        sigindex=[]
        sigindex.append(vbiglist)
        sigindex.append(lbiglist)
        sigindex.append(biglist)
            
        return s1,s2,sigindex
    
    def zqAmo(self,sdate,edate):
        #这里最后一级目录用\\，虽然也可用/，但/+'...'无法被识别
        fdirtitle=u'E:/work/数据备份/资金流/day\\'
        flist=glob.glob(fdirtitle+'*.csv')
        
        #筛选日期范围内数据
        sindex=flist.index(fdirtitle+sdate+'.csv')
        eindex=flist.index(fdirtitle+edate+'.csv')
        flist=flist[sindex:eindex+1]
        zqAmoData=pd.DataFrame()
        for f in flist:
            tmpdata=pd.read_csv(f,encoding='gbk',usecols=[1,2,5,8,9,10],index_col=0)
            zqAmoData=zqAmoData.append(tmpdata)
          
        names=zqAmoData['name'].drop_duplicates()
        zqAmoData=zqAmoData.groupby(level=0).sum()
        zqAmoData['name']=names
        zqAmoData.reset_index(level=0,inplace=True)
        zqAmoData['vbigper']=zqAmoData['bigD']/zqAmoData['amosum']
        zqAmoData.sort_values('vbigper',ascending=False,inplace=True)
        
        c=colligation.ZH(sdate,edate)
        zqAmoData30=c.getRF(30,zqAmoData)   
        zqAmoData200=c.getRF(200,zqAmoData)#.head(len(zqAmoData30))
        
        return zqAmoData200,zqAmoData30,zqAmoData
        

    def zqAmoMin(self,stime,etime):
        
#        amosql="select * from stocksystem.bigcashflow where datetime>='"+stime+ "'and datetime <='"+etime+"'"
#        print amosql
#        amodata=pd.read_sql(amosql,con=self.engine)
#        
#        print '资金流数据读取完成'
# 
#        s2=pd.DataFrame()
#        sindex=['vbigB','vbigS','lbigB','lbigS']
#        tmpgroup=amodata.groupby('hq_code')    
#        for code,tmpdata in tmpgroup:
#            tmpdata=tmpdata.groupby('type').agg({'amo':'sum','amoper':'sum'})
#            tmpdata=tmpdata.reindex(sindex) 
#            tmpdata=tmpdata.unstack().unstack()     
#            tmpdata['hq_code']=code
#            s2=s2.append(tmpdata)
#            
#        s2.fillna(0,inplace=True)
#        s2['bigD']=s2['vbigB']+s2['lbigB']-s2['vbigS']-s2['lbigS']
#        amodata=s2.loc['amo']
#        amoperdata=s2.loc['amoper']
#        amodata.set_index('hq_code',inplace=True)
#        amoperdata.set_index('hq_code',inplace=True)
#        amodata['bigper']=amoperdata['bigD']
#        amodata.sort_values('bigper',inplace=True,ascending=False)
        
        amosql="select hq_code,amo from stocksystem.bigcashflow where datetime>='"+stime+ "'and datetime <='"+etime+"'"
        print amosql
        amodata=pd.read_sql(amosql,con=self.engine,index_col='hq_code')


        amonet=amodata.groupby(level=0).sum()    
        
        
        amodata['amo']=np.abs(amodata['amo'])
        amodata=amodata.groupby(level=0).sum()
        
        stockdata=ts.get_today_all()
        stockdata['code']=stockdata['code'].astype(int)
        stockdata.set_index('code',inplace=True)
        
        #得到流通股本        
        amodata['nmc']=stockdata['nmc']
        amodata['amoper']=amodata['amo']/np.abs(amodata['amo']).sum()
        amodata['nmcper']=stockdata['nmc']/(stockdata['nmc'].sum())
        amodata['amopower']=amodata['amoper']/amodata['nmcper']
        amodata=amodata.sort_values('amopower',ascending=False)     
        
        amonet['nmc']=stockdata['nmc']
        amonet['amoper']=amonet['amo']/np.abs(amonet['amo']).sum()
        amonet['nmcper']=stockdata['nmc']/(stockdata['nmc'].sum())
        amonet['amopower']=amonet['amoper']/amonet['nmcper']
        amonet=amonet.sort_values('amopower',ascending=False)         
        

        return amodata.loc[:,['amo','amoper','amopower','nmcper']],amonet.loc[:,['amo','amoper','amopower','nmcper']]

    def tickCountDay(self,sdate,edate):
        fdirlist=os.listdir(self.dir)
        #筛选日期范围内数据
        sindex=fdirlist.index(sdate)
        eindex=fdirlist.index(edate)
        fdirlist=fdirlist[sindex:eindex+1]  
        for fdir in fdirlist:
            date=fdir
            fdir=os.path.join(self.dir,fdir)    
            flist=os.listdir(fdir)
            lastf=flist[-1]
            s2=pd.DataFrame()        
            n=0
            for f in flist:
                fname=os.path.join(self.dir,fdir,f)
                code=f[2:-4] 
                try:
                    s1=pd.read_table(fname,names=['time','close','vol'],dtype={'time':str,'close':int,'vol':int}).dropna()
                except Exception as e:
                    print e
                    print f
                    s1=pd.read_table(fname,names=['time','close','vol'],dtype={'time':str}).dropna()
                    s1['close']=s1['close'].astype(int)
                    s1['vol']=s1['vol'].astype(int)
                    
                #计算amo
                amosum=(s1['vol']*s1['close']/10000000000).sum()
                s1=pd.DataFrame({'code':[code],'date':[date],'amo':[amosum]})           
                s2=s2.append(s1)
                n+=1
                print fname
                if n%5==0:
                    s2.to_sql('cashflowday',con=self.engine,if_exists='append',index=None)
                elif f==lastf:
                    s2.to_sql('cashflowday',con=self.engine,if_exists='append',index=None)
                    
    def zqAmoDay(self,stime,etime):
        
        amosql="select code,amo from stocksystem.cashflowday where date>='"+stime+ "'and date <='"+etime+"'"
        print amosql
        amodata=pd.read_sql(amosql,con=self.engine,index_col='code').drop_duplicates()
        amodata=amodata.groupby(level=0).sum()
        amodata.sort_values('amo',ascending=False,inplace=True)
        c=colligation.ZH(stime,etime)
        amodata=c.getBoard(3000,amodata)
        amodata.to_csv(u'E:\\work\\数据备份\\板块资金流\\day\\stock'+stime.replace(':','时')+'至'+etime.replace(':','时')+'.csv',encoding='gbk',float_format='%.2f')
        amodata=amodata.groupby('board_name').sum()
        amosum=np.abs(amodata['amo']).sum()
        amodata['amoper']=amodata['amo']/amosum*100
        amodata.sort_values('amo',ascending=False,inplace=True)
        amodata.to_csv(u'E:\\work\\数据备份\\板块资金流\\day\\index'+stime.replace(':','时')+'至'+etime.replace(':','时')+'.csv',encoding='gbk',float_format='%.2f')
        
        return amodata              
        
    def bigCashflow(self,stime,etime):
        
        def picBigCashFlow(amo_index,amo_stock,amo_indexnet,amo_stocknet,period,networth):
            top=0
            left=0
            if networth==1:
                wbk =xlsxwriter.Workbook(u'E:\\work\\报表\\资金流\\ '+period+' networth.xlsx') 
            else:
                wbk =xlsxwriter.Workbook(u'E:\\work\\报表\\资金流\\ '+period+' flow.xlsx') 
            zw=wbk.add_format({'align':'center','valign':'vcenter','font_size':10,'num_format':'0.00'})
            ZJ_Sheet   = wbk.add_worksheet(u'大单效能')
            for row in xrange(len(amo_index)):
                if row==0:
                    ZJ_Sheet.write_row(top+row, left,amo_index,zw)
                    
                ZJ_Sheet.write_row(top+row+1, left,amo_index.iloc[row],zw)
            
            left+=6
            usestock=amo_stock[amo_stock[u'排名']<=500]
            for board in amo_index[u'行业']:
                tmpstock=usestock[usestock[u'行业']==board]
                for row in xrange(len(tmpstock)):
                    if row==0:
                        ZJ_Sheet.write_row(top+row, left,tmpstock,zw)
                    
                    ZJ_Sheet.write_row(top+row+1, left,tmpstock.iloc[row],zw)
                top+=len(tmpstock)+2
                
            left+=6
            top=0
            for row in xrange(len(amo_stock)):
                if row==0:
                        ZJ_Sheet.write_row(top+row, left,tmpstock,zw)
                ZJ_Sheet.write_row(top+row+1, left,amo_stock.iloc[row],zw)
            
            left+=6
            for row in xrange(len(amo_index)):
                if row==0:
                    ZJ_Sheet.write_row(top+row, left,amo_indexnet,zw)
                    
                ZJ_Sheet.write_row(top+row+1, left,amo_indexnet.iloc[row],zw)            
         
            left+=6
            usestock=amo_stocknet[amo_stocknet[u'排名']<=500]
            for board in amo_indexnet[u'行业']:
                tmpstock=usestock[usestock[u'行业']==board]
                for row in xrange(len(tmpstock)):
                    if row==0:
                        ZJ_Sheet.write_row(top+row, left,tmpstock,zw)
                    
                    ZJ_Sheet.write_row(top+row+1, left,tmpstock.iloc[row],zw)
                top+=len(tmpstock)+2
             
             
            left+=6
            top=0
            for row in xrange(len(amo_stocknet)):
                if row==0:
                        ZJ_Sheet.write_row(top+row, left,tmpstock,zw)
                ZJ_Sheet.write_row(top+row+1, left,amo_stocknet.iloc[row],zw)
                 
            wbk.close()         
         
        amodata,amonet=self.zqAmoMin(stime,etime)
        
        c=colligation.ZH(stime,etime) 
        
        amonet['hq_name']=amodata['hq_name']=c.getStockNames(amodata,c.stockNames)       
        amodata=c.getBoard(3000,amodata)
        amonet['board_name']=amodata['board_name']
        
        amo_stock=amodata.loc[:,['hq_name','board_name','amo','amopower']].dropna()
        amo_stocknet=amonet.loc[:,['hq_name','board_name','amo','amopower']].dropna()
        
        #amo_stock.to_csv(u'E:\\work\\数据备份\\板块资金流\\stockbig'+stime.replace(':','时')+'至'+etime.replace(':','时')+'.csv',encoding='gbk',float_format='%.2f')
        
        del amodata['hq_name'],amodata['amopower'],amonet['hq_name'],amonet['amopower']
        
        
        amodata=amodata.groupby('board_name').sum()
        amodata['amopower']=amodata['amoper']/amodata['nmcper']
        amodata.sort_values('amopower',ascending=False,inplace=True)
        amo_index=amodata.loc[:,['amo','amoper','amopower']].dropna()
        amo_index.reset_index(inplace=True)
        
        amonet=amonet.groupby('board_name').sum()    
        amonet['amopower']=amonet['amoper']/amonet['nmcper']
        amonet.sort_values('amopower',ascending=False,inplace=True)
        amo_indexnet=amonet.loc[:,['amo','amoper','amopower']].dropna()
        amo_indexnet.reset_index(inplace=True)        
          
        amo_indexnet.columns=[u'行业',u'大单净额',u'大单占比',u'效能']
        amo_stocknet.columns=[u'股票',u'行业',u'大单净额',u'效能']
    
        amo_index.columns=[u'行业',u'大单总量',u'大单占比',u'效能']
        amo_stock.columns=[u'股票',u'行业',u'大单总量',u'效能']
        
        amo_stock[u'排名']=np.arange(1,len(amo_stock)+1)
        amo_stocknet[u'排名']=np.arange(1,len(amo_stocknet)+1)
        
        period=stime.replace(':','时')+'至'+etime.replace(':','时')
        
        picBigCashFlow(amo_index,amo_stock,amo_indexnet,amo_stocknet,period,networth=0)
        #picBigCashFlow(amo_indexnet,amo_stocknet,period,networth=1)
        
        
        
    def plot_board_zjrank(self,sdate,edate):
        
        wbk=xlsxwriter.Workbook(u'E:/work/报表/全市场选股/'+sdate+'-'+edate+'监控图表.xlsx')
        
        pic_sheet=wbk.add_worksheet('pic')
        
        data_sheet=wbk.add_worksheet('data')   
        
        data_sheet.hide()
        
        #抓取上证指数数据，得到交易日期
        df_sz=ts.get_k_data('000001',index=True,start=sdate,end=edate)
        
        trading_days=df_sz['date']
        
        #周期容器，装纳每天数据
        df_period_rank=pd.DataFrame()
        
        for date in trading_days:
            
            excel_fname=u'E:/work/报表/全市场选股/F'+date+' 00时00至'+date+' 15时00.xlsx'
            
            csv_fname=u'E:/work/报表/全市场选股/F'+date+' 00时00至'+date+' 15时00.csv'
            
            if os.path.exists(excel_fname):
                
                df_rank=pd.read_excel(excel_fname)
                
                df_board_rank=df_rank.iloc[:,[0,1]].dropna()
                    
                df_board_rank['date']=date
                
                df_board_rank['rank']=range(1,len(df_board_rank)+1)
                    
                df_period_rank=df_period_rank.append(df_board_rank)   
                
                df_stock_rank=df_rank.iloc[:,3:16]                
            
            elif os.path.exists(csv_fname):
        
                df_rank=pd.read_csv(csv_fname,encoding='gbk')
                
                df_board_rank=df_rank.iloc[:,[1,2]].dropna()
                    
                df_board_rank['date']=date
                
                df_board_rank['rank']=range(1,len(df_board_rank)+1)
                    
                df_period_rank=df_period_rank.append(df_board_rank)   
                
                df_stock_rank=df_rank.iloc[:,4:17]                  
            
            else:
                continue
                 
            df_stock_rank=df_stock_rank[df_stock_rank[u'综合评分']>9]
                             
            df_stock_rank.replace([u'名称',u'板块','大单净额','大单效能','聪明钱','大单总量','综合评分',None],'',inplace=True)    

            tmp_sheet=wbk.add_worksheet(date)      
            
            tmp_sheet.write_row(0,0,df_stock_rank.columns)

            for i in xrange(len(df_stock_rank)):
                
                tmp_sheet.write_row(i+1,0,df_stock_rank.iloc[i])
                             
            print date
            

        def high_stats(df):
            
            high_times=df[(df['grade']>=13)|(df['rank']<=5)]['rank'].count()
            
            df['high_times']=high_times
            
            return df 
          
        df_period_rank.reset_index(inplace=True,drop=True)
          
        df_period_rank=df_period_rank.groupby('board_name').apply(high_stats)  
        
        df_period_rank.drop(df_period_rank[df_period_rank.high_times==0].index,inplace=True)
        
        #df_period_rank.drop('rank',axis=1,inplace=True)
        
        df_period_rank.sort_values('high_times',ascending=False,inplace=True)
        
        sort_boards=df_period_rank['board_name'].drop_duplicates()
        
        df_period_rank.sort_values('date',inplace=True)
        
        df_period_rank['grade']=df_period_rank['grade']-12
        
        header=list(df_period_rank)
        
        name_col=header.index('board_name')
        
        date_col=header.index('date')
        
        rank_col=header.index('grade')
        
        n=0
        
        top=0
        
        pic_top=0
        
        pic_sheet.write_row(0,15,[u'排名',u'板块',u'异动次数']) 
        
        for board in sort_boards:
            
            df_board_rank=df_period_rank[df_period_rank.board_name==board]
            
            data_len=len(df_board_rank)
            
            for i in xrange(data_len):
                
                data_sheet.write_row(top+i,0,df_board_rank.iloc[i])
                    
            pic_sheet.write(pic_top,0,board)
            
            pic_sheet.write_row(n+1,15,[str(n),board,df_board_rank['high_times'].iat[-1]])    
            
            board_chart=wbk.add_chart({'type': 'line'})
            
            board_chart.add_series({
             'name':['data', top, name_col],
             'categories':['data', top, date_col, top+data_len, date_col],
             'values':['data', top, rank_col, top+data_len, rank_col],   
            })         
            
            board_chart.set_x_axis({#'name':u'日期',
                                #'name_font': {'size': 10, 'bold': True},
                                'label_position': 'low',
                                'interval_unit': 2
                            
                                })            
            
            board_chart.set_size({'width':800,'height':300})
            
            pic_sheet.insert_chart(pic_top,1,board_chart)    
            
            top+=data_len+1
            
            pic_top+=20
            
            n+=1
            
            print n    
        
        wbk.close()
        
        return df_period_rank
 
if __name__=='__main__':
    
    z=ZJ()   
    
#    update=input('Do you want to update?')
#    if update==True:   
#        z.tickTradeType('20170703','20170714')
 
    sdate='2017-11-20'
    edate='2017-11-20'
    
    #fdate=sdate.replace('-','')
    
    #z.tickTradeType(sdate.replace('-',''),edate.replace('-',''))
    
    c=colligation.ZH(sdate,edate)
      
    #c.buildMinRankForm(factor=True)
    
    df_period_rank=z.plot_board_zjrank('2017-11-13','2017-11-20 ')    
#    
##    z.bigCashflow(sdate,sdate)

    #z.zqAllAmo('20170710','20170718')
    #z.tickCountDay('20170719','20170719')
    #amodata=z.zqAmoDay('2017-07-19','2017-07-19')
    
    #data200,data30=z.zqAmo('2017-06-26','2017-06-30')
    #生成基础数据，并导出到CSV
    #s=z.zqAmoMin('2017-07-06 14:30','2017-07-07 14:59')
    #amodata=z.zqAmoMin('2017-07-06 14:30','2017-07-07 14:59')