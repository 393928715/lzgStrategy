import pandas as pd 
from sqlalchemy import create_engine 

#import sys
#reload(sys)      
#sys.setdefaultencoding('utf-8')

def getMacd(df):
    pass
    

#只有从6月份开始算才会准
sdate='2017-06-01'

engine=create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')

df_day=pd.read_sql('SELECT hq_code,hq_date,hq_close FROM stocksystem.hstockquotationday as hiq where hiq.index >= "'+sdate+'"',con=engine)

ema12=df_day.groupby('hq_code').apply(lambda x :x['hq_close'].ewm(span=12).mean())

ema26=df_day.groupby('hq_code').apply(lambda x :x['hq_close'].ewm(span=26).mean())

dif=ema12-ema26

#dif=pd.DataFrame({'hq_code':ema12['hq_code'],'dif':ema12['hq_close']-ema26['hq_close']})

dea=dif.groupby(level=0).apply(lambda x :x.ewm(span=9).mean())

#dif=dif.reset_index(level=0)
#
#dea=dea.reset_index(level=0)
#
#dif['date']=df_day['hq_date']
#
#dea['date']=df_day['hq_date']

macd=((dif-dea)*2).reset_index(level=0)

df=macd[macd.hq_code==1]

df['date']=df_day['hq_date']

#df_60=pd.read_sql('SELECT hq_code,hq_date,hq_time,hq_close FROM stocksystem.hstockquotationsixty as hiq where hiq.index >= "'+sdate+'"',con=engine)


#fname=u'E:\\test\\2017-10-09至2017-10-13.csv'
#
#s1=pd.read_csv(fname,encoding='gbk')
