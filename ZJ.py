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
import time
from sqlalchemy import create_engine
from PlotData.plotexcel import plotToExcel
import colligation# import ZH
import glob

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class ZJ():
    def __init__(self):
        self.dir=u'E:\\工作\\tick\\'
        self.engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        #储存每日资金流的CSV文件目录
        self.datadir=u"E:/工作/数据备份/资金流/*.csv"
    
    #按成交量划分10个类别,并把基础数据导出到CSV
    def tradeType(self,sdate,edate,mflag=0):
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
                s1['amo']=np.abs(s1['vol'])*s1['close']/10000000000
                  
                if mflag==1:
                    s1['datetime']=pd.to_datetime(date+' '+s1['time'].apply(lambda x:(x[:-4]+':'+x[-4:-2]).strip()))
                    
                #分类
                vbig=s1[s1.amo>=0.01000000]
                vbigs=vbig[vbig.vol<0]
                vbigb=vbig[vbig.vol>0]
                
                lbig=s1[(s1.amo<0.01000000)&(s1.amo>=0.00500000)]
                lbigs=lbig[lbig.vol<0]
                lbigb=lbig[lbig.vol>0]
                
#                big=s1[(s1.amo<500000)&(s1.amo>=100000)]
#                bigs=big[big.vol<0]            
#                bigb=big[big.vol>0]
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
                amosum=s1['amo'].sum()
                
                vbigs['type']='vbigS'            
                vbigb['type']='vbigB'  
                     
                lbigs['type']='lbigS'
                lbigb['type']='lbigB'
                           
#                bigs['type']='bigS'
#                bigb['type']='bigB' 
#                    
#                mids['type']='midS'
#                midb['type']='midB'
#                
#                smalls['type']='smallS'
#                smallb['type']='smallB'
                         
                #拼接分类的series，赋值给总数据                         
#                zjtype=pd.concat([vbigs['type'],vbigb['type'],lbigs['type'],lbigb['type'],bigs['type'],bigb['type'],mids['type'],midb['type'],smalls['type'],smallb['type']])
#                s1['type']=zjtype  
                         
                s1=pd.concat([vbigs,vbigb,lbigs,lbigb])
                 
                code=f[2:-4]             
                
                if mflag==1:
                    s1['hq_code']=code
                    s1['amoper']=s1['amo']/amosum
                    del s1['close'],s1['time']
                    s2=s2.append(s1)
                    if n%5==0:
                        s2.to_sql('cashflow',con=self.engine,if_exists='append',index=None)
                        s2=pd.DataFrame()
                    if f==lastf:
                        s2.to_sql('cashflow',con=self.engine,if_exists='append',index=None)
                        
#                        if n==10:
#                            s2.to_csv(u'E:/工作/数据备份/资金流/min/'+date+'.csv',encoding='gbk',mode='w')
#                        else:
#                            s2.to_csv(u'E:/工作/数据备份/资金流/min/'+date+'.csv',encoding='gbk',mode='a')
#                    if f==lastf:
#                        s2.to_csv(u'E:/工作/数据备份/资金流/min/'+date+'.csv',encoding='gbk',mode='a')
                    n+=1
                    print n
                
                else:    
                    #计算成交额占比    
                    
                    s1=s1.groupby('type').agg({'amo':'sum'})
                    #s1['per']=s1['amo']/amosum
                    
                    #reindex，后面索引相同才可以相加
                    #sindex=['bigB','bigS','lbigB','lbigS','midB','midS','smallB','smallS','vbigB','vbigS']    
                    sindex=['vbigB','vbigS','lbigB','lbigS']
                    s1=s1.reindex(sindex)      
          
        #            s1['code']=int(code)              
        #            code=s1['code']
        #            del s1['code']           
        #            s1=s1.set_index([code,s1.index])        
        #            s1=s1.unstack()['amo']
        
                    s1=s1.unstack().unstack()
                    s1['hq_code']=int(code)      
                    s1['amosum']=amosum
                    s2=s2.append(s1)  
                    n+=1
                    if n%10==0:            
                        print n
                
            if mflag==0:
                    #给空值赋0，为了后面可以相加不为空
                s2.fillna(0,inplace=True)
                s2['vbigD']=s2['vbigB']-s2['vbigS']
                s2['lbigD']=s2['lbigB']-s2['lbigS']   
                s2['bigD']=s2['vbigD']+s2['lbigD']
                s2.index=s2['hq_code']
                stockNames=pd.read_sql_table('stockbaseinfo',con=self.engine,schema='stocksystem',index_col='stock_code',columns=['stock_code','stock_name'])  
                s2['name']=stockNames['stock_name']    
                s2=s2.loc[:,['name','vbigB','vbigS','vbigD','lbigB','lbigS','lbigD','bigD','amosum']]
                s2.reset_index(level=0,inplace=True)
                date=date[:4]+'-'+date[4:6]+'-'+date[6:]
                s2.to_csv(u'E:/工作/数据备份/资金流/day/'+date+'.csv',encoding='gbk')
                
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
        fdirtitle=u'E:/工作/数据备份/资金流/day\\'
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
        

    def zqMinAmo(self,stime,etime):
        
        amosql="select * from stocksystem.cashflow where datetime>='"+stime+ "'and datetime <='"+etime+"'"
        print amosql
        amodata=pd.read_sql(amosql,con=self.engine)
        
        print '资金流数据读取完成'
 
        s2=pd.DataFrame()
        sindex=['vbigB','vbigS','lbigB','lbigS']
        tmpgroup=amodata.groupby('hq_code')    
        for code,tmpdata in tmpgroup:
            tmpdata=tmpdata.groupby('type').agg({'amo':'sum','amoper':'sum'})
            tmpdata=tmpdata.reindex(sindex) 
            tmpdata=tmpdata.unstack().unstack()     
            tmpdata['hq_code']=code
            s2=s2.append(tmpdata)
            
        s2.fillna(0,inplace=True)
        s2['bigD']=s2['vbigB']+s2['lbigB']-s2['vbigS']-s2['lbigS']
        amodata=s2.loc['amo']
        amoperdata=s2.loc['amoper']
        amodata.set_index('hq_code',inplace=True)
        amoperdata.set_index('hq_code',inplace=True)
        amodata['bigper']=amoperdata['bigD']
        amodata.sort_values('bigper',inplace=True,ascending=False)
        
        return amodata.loc[:,['bigD','bigper']]

    def indexCashflow(self,stime,etime):
        amosql="select hq_code,amo from stocksystem.cashflow where datetime>='"+stime+ "'and datetime <='"+etime+"'"
        print amosql
        amodata=pd.read_sql(amosql,con=self.engine,index_col='hq_code')
        amodata=self.getBoard(3000,amodata)
        amodata=amodata.groupby('board_name').sum()
        amosum=amodata['amo'].sum()
        amodata['amoper']=amodata['amo']/amosum*100
        amodata.sort_values('amo',ascending=False,inplace=True)
        
        return amodata
                    
if __name__=='__main__':
    z=ZJ()
    z.tradeType('20170714','20170714',0)
    #data200,data30=z.zqAmo('2017-06-26','2017-06-30')
    #生成基础数据，并导出到CSV
    #s=z.zqMinAmo('2017-07-06 14:30','2017-07-07 14:59')
    #amodata=z.zqMinAmo('2017-07-06 14:30','2017-07-07 14:59')

    