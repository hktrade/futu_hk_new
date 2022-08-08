import json,sys,csv,time,threading,re
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from futu import *
import pandas_ta as ta
# Algo Master python
# IG	      : kong_ku_lou
# Youtube     : www.youtube.com/c/美股数据张老师
min2 = (datetime.now().strftime('%Y-%m-%d %H:%M')+':00')
pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 5000)
pd.set_option('display.width', 1000)
signal = '';	m_list = [];	df_ps = {};	F_bid = 0
df_tk = {}

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
trd_ctx = OpenFutureTradeContext(host='127.0.0.1', port=11111)

def intrade():
	wk=datetime.today().weekday()
	if wk < 5 and '0915'<=time.strftime('%H%M')<'1200':
		return(True)
	if wk < 5 and '1300'<=time.strftime('%H%M')<'1629':
		return(True)

	if wk < 5 and '1715'<=time.strftime('%H%M'):
		return(True)
	if wk < 6 and '0000'<=time.strftime('%H%M')<'0300':
		return(True)

	print(wk,time.strftime('%H%M'),'\t out of market hours,\t ',time.strftime('%H:%M:%S'))
	return(False)
def def_str(mystr):
	min2 = (datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	inputstr='\n'+min2+','+str(mystr)
	if os.stat(r'str.csv').st_size == 0:
		f=open(r'str.csv','w');f.write(inputstr);f.close()
	else:
		f=open(r'str.csv','a+');f.write(inputstr);f.close()
	print(inputstr)	
def snap_(symbol):
	ret, data = quote_ctx.get_market_snapshot(symbol)
	if ret != RET_OK:
		def_str('error, get_market_snapshot ')
		print(data)
		return([0,0,0])
	else:
		ask_p = data['ask_price'][0]
		bid_p = data['bid_price'][0]
		last_p = data['last_price'][0]		# return([ask_p,bid_p,last_p])
		return (data[['ask_price','bid_price','last_price']])
	
class TickerTest(TickerHandlerBase):# 获取期指的Ticker 实时推送
	def on_recv_rsp(self, rsp_str):
		global F_bid,F_lst,df_tk
		ret_code, data = super(TickerTest,self).on_recv_rsp(rsp_str)
		if ret_code != RET_OK:
			print("TickerTest: error, msg: %s" % data)
			return RET_ERROR, data
		# print(data[['price','volume','turnover','ticker_direction','type']].tail(3),len(data))
		df_tk = data[['price','volume','turnover','ticker_direction','type']]
		F_lst=data['price'].tail(3)
		F_bid=int(data['price'].iloc[-1])
		return data

def get_bar_min_tdd(symbols, N):
	if symbols.find('HK.')==-1:
		symbols = 'HK.'+symbols
	ohlc_dict = {'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}

	futu_freq = [1,3,5,15,30,60]
	other_freq_A = [2,4,10]
	other_freq_B = [120,180,240,360,480,720]
	def codes(i):
		switcher={1:SubType.K_1M,3:SubType.K_3M,5:SubType.K_5M,15:SubType.K_15M,30:SubType.K_30M,60:SubType.K_60M}
		return (switcher.get(i,"Invalid Symbols"))

	K_type = N 

	org_K = str(K_type)
	if K_type in futu_freq:
		K_type = codes(K_type)
	elif K_type in other_freq_A:
		K_type = SubType.K_1M
	elif K_type in other_freq_B:
		K_type = SubType.K_60M
	elif K_type == 1440:
		K_type = SubType.K_DAY
	else:
		print('Error K_type');	
		def_str('Error K_type')
		return None

	ret_sub, err_message = quote_ctx.subscribe(symbols, K_type, subscribe_push=False)
	if ret_sub == RET_OK:
		ret, data3 = quote_ctx.get_cur_kline(symbols, 1000, K_type)

		if len(data3)<2 or ret==-1:
			def_str(symbol + '  kline error')
			return None

		df = data3.copy()
		df['time_key'] = pd.to_datetime(df['time_key'])
		df.set_index('time_key', inplace=True,drop=False)
		
		if K_type not in futu_freq or K_type != SubType.K_DAY :

			T_freq = org_K + 'T'
			dfH =  df.resample(T_freq, closed='right').agg(ohlc_dict).dropna(how='any')
			dfH['time_key'] = dfH.index
			data3 = dfH.copy()
			
		if ret !=-1:
			return df
	else:
		print(err_message)
		return None


def idc_pv(df_pv, N, side):# 传入snapshot数组 和 k线数据 返回量价齐升 True or False / 或者量升价跌 True or False 
	start = 1;		codes=[];		old_N = N  # N is the last two K
	df_find = pd.DataFrame({})

	df_pv['turn1'] = df_pv['volume'].shift(1) #- df_pv['volume'].copy()
	df_pv['last1'] = df_pv['close'].shift(1) #- df_pv['close'].copy()
	df_pv.reset_index(drop=True, inplace=True)

	for i in range(old_N+1):
		if i > 0:
			if side == 'up':
				if (df_pv['close'].iloc[-i] >= df_pv['last1'].iloc[-i] and 
				df_pv['volume'].iloc[-i] >= df_pv['turn1'].iloc[-i]):
					start += 1
				else:
					return False
			elif side == 'down':
				if (df_pv['close'].iloc[-i] <= df_pv['last1'].iloc[-i] and 
				df_pv['volume'].iloc[-i] >= df_pv['turn1'].iloc[-i]):
					start += 1
				else:
					return False
	return True

		
def t2():
	global min2, df_new, df_ps, df_cfg,cash,df_tk
	df_cfg = pd.DataFrame({})

	while 1:
		time.sleep(1)
		wk=datetime.today().weekday()

		# if intrade()==False:
		if False:
			print('out of market hour')
			time.sleep(3)
			continue

		symbol = 'HK.999010'
		handler = TickerTest();quote_ctx.set_handler(handler);quote_ctx.subscribe([symbol], [SubType.TICKER])
		print(df_tk, len(df_tk))
		print(snap_(symbol))
		
		df = get_bar_min_tdd(symbol, 3)
		print(df[['volume','open','high','low','close']].tail(10))
		
		df.drop(df.index[-1], axis=0, inplace=True);df.reset_index(drop=True, inplace=True)	
		up_bool = idc_pv(df, 2, 'up')
		print(df_pv)
		down_bool = idc_pv(df, 2, 'down')
		print(df_pv)
		time.sleep(3)
		
t2()