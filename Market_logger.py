from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
import logging
import time
import threading
import os
import json
import datetime
import sys
import mysql.connector
import requests

# CONFIG FOR MYSQL

mydb = mysql.connector.connect(
  host="localhost",
  user="USER",
  passwd="PASSWORD",
  database="flash"
)

mycursor = mydb.cursor()

last_channel_save = {}
last_channel_data = {}
last_pair_save = {}

is_first_saved_for = {}
all_file_wrote = 0
all_sql_wrote = 0

start_time = time.time()
dirname = os.path.dirname(__file__)


def fileprint(data, folder, filename): # Creates a lot of file in a new /data/ folder! Not recommended, not used, but if you need, it works!
	global all_file_wrote

	data_path = os.path.join(dirname, 'data')
	if not os.path.exists(data_path):
		os.makedirs(data_path)
	folder_path = os.path.join(dirname, 'data/' + folder)
	if not os.path.exists('data/' + folder):
		os.makedirs('data/' + folder)
	filestr = str(filename)
	file_path = os.path.join(dirname, 'data/' + folder + "/" + filestr + '.txt')
	file = open(file_path, 'w+')
	file.write(data)
	file.close()
	all_file_wrote += 1

def sqlprint(ticker, depth, pair, now):
	global all_sql_wrote
	ticker = json.loads(ticker)
	depth = json.loads(depth)

	sql = "INSERT INTO ticker (stream, timestamp, close) VALUES (%s, %s, %s)" # Insert ticker last price to 'ticker' table
	val = (pair, now, ticker['data']['c'])
	mycursor.execute(sql, val)
	all_sql_wrote += 1

	for elem in depth['data']['bids']:
		sql = "INSERT INTO depth (stream, timestamp, side, price, amount) VALUES (%s, %s, %s, %s, %s)" # Insert top 20 bid to 'depth' table
		val = (pair, now, 'bid', elem[0], elem[1])
		mycursor.execute(sql, val)
		all_sql_wrote += 1

	for elem in depth['data']['asks']:
		sql = "INSERT INTO depth (stream, timestamp, side, price, amount) VALUES (%s, %s, %s, %s, %s)" # Insert top 20 ask to 'depth' table
		val = (pair, now, 'ask', elem[0], elem[1])
		mycursor.execute(sql, val)
		all_sql_wrote += 1

	mydb.commit()


def progress_data(streamdata):
	data = json.loads(streamdata)

	key = data['stream'] # ethbtc@ticker
	pair = data['stream'].split('@')[0] # ethbtc
	now = int(time.time())
	last_channel_save_diff = now - last_channel_save.get(key, 0)
	if last_channel_save_diff >= 1: # if last data of stream saved more than a second ago
		last_channel_data[key] = streamdata
		last_channel_save[key] = int(time.time())
		is_first_saved_for[key] = 1
		
		last_pair_save_diff = now - last_pair_save.get(pair, 0)
		if last_pair_save_diff >= 1 & is_first_saved_for.get(pair + '@miniTicker', 0) == 1 & is_first_saved_for.get(pair + '@depth20', 0) == 1:
			last_pair_save[pair] = int(time.time())
			tickdata = last_channel_data[pair + '@miniTicker']
			depthdata = last_channel_data[pair + '@depth20']
			#fileprint('{"ticker":' + tickdata + ',\n"depth":' + depthdata + '}', pair, now)
			sqlprint(tickdata, depthdata, pair, now)
	else:
		pass # if last stream data saved in a sec, skip.


# https://docs.python.org/3/library/logging.html
logging.basicConfig(filename=os.path.basename(__file__) + '.log',
					format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
					style="{")
logging.getLogger('unicorn-log').setLevel(logging.INFO)
logging.getLogger('unicorn-log').addHandler(logging.StreamHandler())

binance_websocket_api_manager = BinanceWebSocketApiManager(progress_data) # create instance of BinanceWebSocketApiManager and call progress_data

# define stream channels
channels = {'depth20', 'miniTicker'}

markets = ['ethbtc', 'ltcbtc', 'bnbbtc', 'neobtc', 'qtumeth', 'eoseth', 'snteth', 'bnteth', 'gasbtc', 'bnbeth', 'btcusdt', 'ethusdt', 'oaxeth', 'dnteth', 'mcoeth', 'mcobtc', 'wtcbtc', 'wtceth', 'lrcbtc', 'lrceth', 'qtumbtc', 'yoyobtc', 'omgbtc', 'omgeth', 'zrxbtc', 'zrxeth', 'stratbtc', 'strateth', 'snglsbtc', 'snglseth', 'bqxbtc', 'bqxeth', 'kncbtc', 'knceth', 'funbtc', 'funeth', 'snmbtc', 'snmeth', 'neoeth', 'iotabtc', 'iotaeth', 'linkbtc', 'linketh', 'xvgbtc', 'xvgeth', 'mdabtc', 'mdaeth', 'mtlbtc', 'mtleth', 'eosbtc', 'sntbtc', 'etceth', 'etcbtc', 'mthbtc', 'mtheth', 'engbtc', 'engeth', 'dntbtc', 'zecbtc', 'zeceth', 'bntbtc', 'astbtc', 'asteth', 'dashbtc', 'dasheth', 'oaxbtc', 'btgbtc', 'btgeth', 'evxbtc', 'evxeth', 'reqbtc', 'reqeth', 'vibbtc', 'vibeth', 'trxbtc', 'trxeth', 'powrbtc', 'powreth', 'arkbtc', 'arketh', 'yoyoeth', 'xrpbtc', 'xrpeth', 'enjbtc', 'enjeth', 'storjbtc', 'storjeth', 'bnbusdt', 'yoyobnb', 'powrbnb', 'kmdbtc', 'kmdeth', 'nulsbnb', 'rcnbtc', 'rcneth', 'rcnbnb', 'nulsbtc', 'nulseth', 'rdnbtc', 'rdneth', 'rdnbnb', 'xmrbtc', 'xmreth', 'dltbnb', 'wtcbnb', 'dltbtc', 'dlteth', 'ambbtc', 'ambeth', 'ambbnb', 'batbtc', 'bateth', 'batbnb', 'bcptbtc', 'bcpteth', 'bcptbnb', 'arnbtc', 'arneth', 'gvtbtc', 'gvteth', 'cdtbtc', 'cdteth', 'gxsbtc', 'gxseth', 'neousdt', 'neobnb', 'poebtc', 'poeeth', 'qspbtc', 'qspeth', 'qspbnb', 'btsbtc', 'btseth', 'btsbnb', 'xzcbtc', 'xzceth', 'xzcbnb', 'lskbtc', 'lsketh', 'lskbnb', 'tntbtc', 'tnteth', 'fuelbtc', 'fueleth', 'manabtc', 'manaeth', 'bcdbtc', 'bcdeth', 'dgdbtc', 'dgdeth', 'iotabnb', 'adxbtc', 'adxeth', 'adxbnb', 'adabtc', 'adaeth', 'pptbtc', 'ppteth', 'cmtbtc', 'cmteth', 'cmtbnb', 'xlmbtc', 'xlmeth', 'xlmbnb', 'cndbtc', 'cndeth', 'cndbnb', 'lendbtc', 'lendeth', 'wabibtc', 'wabieth', 'wabibnb', 'ltceth', 'ltcusdt', 'ltcbnb', 'tnbbtc', 'tnbeth', 'wavesbtc', 'waveseth', 'wavesbnb', 'gtobtc', 'gtoeth', 'gtobnb', 'icxbtc', 'icxeth', 'icxbnb', 'ostbtc', 'osteth', 'ostbnb', 'elfbtc', 'elfeth', 'aionbtc', 'aioneth', 'aionbnb', 'neblbtc', 'nebleth', 'neblbnb', 'brdbtc', 'brdeth', 'brdbnb', 'mcobnb', 'edobtc', 'edoeth', 'navbtc', 'naveth', 'navbnb', 'lunbtc', 'luneth', 'appcbtc', 'appceth', 'appcbnb', 'vibebtc', 'vibeeth']

""" markets = ['ethbtc', 'ltcbtc', 'bnbbtc', 'neobtc', 'qtumeth', 'eoseth', 'snteth', 'bnteth', 'gasbtc', 'bnbeth', 'btcusdt', 'ethusdt', 'oaxeth', 'dnteth', 'mcoeth', 'mcobtc', 'wtcbtc', 'wtceth', 'lrcbtc', 'lrceth', 'qtumbtc', 'yoyobtc', 'omgbtc', 'omgeth', 'zrxbtc', 'zrxeth', 'stratbtc', 'strateth', 'snglsbtc', 'snglseth', 'bqxbtc', 'bqxeth', 'kncbtc', 'knceth', 'funbtc', 'funeth', 'snmbtc', 'snmeth', 'neoeth', 'iotabtc', 'iotaeth', 'linkbtc', 'linketh', 'xvgbtc', 'xvgeth', 'mdabtc', 'mdaeth', 'mtlbtc', 'mtleth', 'eosbtc', 'sntbtc', 'etceth', 'etcbtc', 'mthbtc', 'mtheth', 'engbtc', 'engeth', 'dntbtc', 'zecbtc', 'zeceth', 'bntbtc', 'astbtc', 'asteth', 'dashbtc', 'dasheth', 'oaxbtc', 'btgbtc', 'btgeth', 'evxbtc', 'evxeth', 'reqbtc', 'reqeth', 'vibbtc', 'vibeth', 'trxbtc', 'trxeth', 'powrbtc', 'powreth', 'arkbtc', 'arketh', 'yoyoeth', 'xrpbtc', 'xrpeth', 'enjbtc', 'enjeth', 'storjbtc', 'storjeth', 'bnbusdt', 'yoyobnb', 'powrbnb', 'kmdbtc', 'kmdeth', 'nulsbnb', 'rcnbtc', 'rcneth', 'rcnbnb', 'nulsbtc', 'nulseth', 'rdnbtc', 'rdneth', 'rdnbnb', 'xmrbtc', 'xmreth', 'dltbnb', 'wtcbnb', 'dltbtc', 'dlteth', 'ambbtc', 'ambeth', 'ambbnb', 'batbtc', 'bateth', 'batbnb', 'bcptbtc', 'bcpteth', 'bcptbnb', 'arnbtc', 'arneth', 'gvtbtc', 'gvteth', 'cdtbtc', 'cdteth', 'gxsbtc', 'gxseth', 'neousdt', 'neobnb', 'poebtc', 'poeeth', 'qspbtc', 'qspeth', 'qspbnb', 'btsbtc', 'btseth', 'btsbnb', 'xzcbtc', 'xzceth', 'xzcbnb', 'lskbtc', 'lsketh', 'lskbnb', 'tntbtc', 'tnteth', 'fuelbtc', 'fueleth', 'manabtc', 'manaeth', 'bcdbtc', 'bcdeth', 'dgdbtc', 'dgdeth', 'iotabnb', 'adxbtc', 'adxeth', 'adxbnb', 'adabtc', 'adaeth', 'pptbtc', 'ppteth', 'cmtbtc', 'cmteth', 'cmtbnb', 'xlmbtc', 'xlmeth', 'xlmbnb', 'cndbtc', 'cndeth', 'cndbnb', 'lendbtc', 'lendeth', 'wabibtc', 'wabieth', 'wabibnb', 'ltceth', 'ltcusdt', 'ltcbnb', 'tnbbtc', 'tnbeth', 'wavesbtc', 'waveseth', 'wavesbnb', 'gtobtc', 'gtoeth', 'gtobnb', 'icxbtc', 'icxeth', 'icxbnb', 'ostbtc', 'osteth', 'ostbnb', 'elfbtc', 'elfeth', 'aionbtc', 'aioneth', 'aionbnb', 'neblbtc', 'nebleth', 'neblbnb', 'brdbtc', 'brdeth', 'brdbnb', 'mcobnb', 'edobtc', 'edoeth', 'navbtc', 'naveth', 'navbnb', 'lunbtc', 'luneth', 'appcbtc', 'appceth', 'appcbnb', 'vibebtc', 'vibeeth', 'rlcbtc', 'rlceth', 'rlcbnb', 'insbtc', 'inseth', 'pivxbtc', 'pivxeth', 'pivxbnb', 'iostbtc', 'iosteth', 'steembtc', 'steemeth', 'steembnb', 'nanobtc', 'nanoeth', 'nanobnb', 'viabtc', 'viaeth', 'viabnb', 'blzbtc', 'blzeth', 'blzbnb', 'aebtc', 'aeeth', 'aebnb', 'ncashbtc', 'ncasheth', 'ncashbnb', 'poabtc', 'poaeth', 'poabnb', 'zilbtc', 'zileth', 'zilbnb', 'ontbtc', 'onteth', 'ontbnb', 'stormbtc', 'stormeth', 'stormbnb', 'qtumbnb', 'qtumusdt', 'xembtc', 'xemeth', 'xembnb', 'wanbtc', 'waneth', 'wanbnb', 'wprbtc', 'wpreth', 'qlcbtc', 'qlceth', 'sysbtc', 'syseth', 'sysbnb', 'qlcbnb', 'grsbtc', 'grseth', 'adausdt', 'adabnb', 'gntbtc', 'gnteth', 'gntbnb', 'loombtc', 'loometh', 'loombnb', 'xrpusdt', 'repbtc', 'repeth', 'repbnb', 'btctusd', 'ethtusd', 'zenbtc', 'zeneth', 'zenbnb', 'skybtc', 'skyeth', 'skybnb', 'eosusdt', 'eosbnb', 'cvcbtc', 'cvceth', 'cvcbnb', 'thetabtc', 'thetaeth', 'thetabnb', 'xrpbnb', 'tusdusdt', 'iotausdt', 'xlmusdt', 'iotxbtc', 'iotxeth', 'qkcbtc', 'qkceth', 'agibtc', 'agieth', 'agibnb', 'nxsbtc', 'nxseth', 'nxsbnb', 'enjbnb', 'databtc', 'dataeth', 'ontusdt', 'trxbnb', 'trxusdt', 'etcusdt', 'etcbnb', 'icxusdt', 'scbtc', 'sceth', 'scbnb', 'npxsbtc', 'npxseth', 'keybtc', 'keyeth', 'nasbtc', 'naseth', 'nasbnb', 'mftbtc', 'mfteth', 'mftbnb', 'dentbtc', 'denteth', 'ardrbtc', 'ardreth', 'ardrbnb', 'nulsusdt', 'hotbtc', 'hoteth', 'vetbtc', 'veteth', 'vetusdt', 'vetbnb', 'dockbtc', 'docketh', 'polybtc', 'polybnb', 'hcbtc', 'hceth', 'gobtc', 'gobnb', 'paxusdt', 'rvnbtc', 'rvnbnb', 'dcrbtc', 'dcrbnb', 'mithbtc', 'mithbnb', 'bchabcbtc', 'bchabcusdt', 'bnbpax', 'btcpax', 'ethpax', 'xrppax', 'eospax', 'xlmpax', 'renbtc', 'renbnb', 'bnbtusd', 'xrptusd', 'eostusd', 'xlmtusd', 'bnbusdc', 'btcusdc', 'ethusdc', 'xrpusdc', 'eosusdc', 'xlmusdc', 'usdcusdt', 'adatusd', 'trxtusd', 'neotusd', 'trxxrp', 'xzcxrp', 'paxtusd', 'usdctusd', 'usdcpax', 'linkusdt', 'linktusd', 'linkpax', 'linkusdc', 'wavesusdt', 'wavestusd', 'wavespax', 'wavesusdc', 'bchabctusd', 'bchabcpax', 'bchabcusdc', 'ltctusd', 'ltcpax', 'ltcusdc', 'trxpax', 'trxusdc', 'bttbtc', 'bttbnb', 'bttusdt', 'bnbusds', 'btcusds', 'usdsusdt', 'usdspax', 'usdstusd', 'usdsusdc', 'bttpax', 'btttusd', 'bttusdc', 'ongbnb', 'ongbtc', 'ongusdt', 'hotbnb', 'hotusdt', 'zilusdt', 'zrxbnb', 'zrxusdt', 'fetbnb', 'fetbtc', 'fetusdt', 'batusdt', 'xmrbnb', 'xmrusdt', 'zecbnb', 'zecusdt', 'zecpax', 'zectusd', 'zecusdc', 'iostbnb', 'iostusdt', 'celrbnb', 'celrbtc', 'celrusdt', 'adapax', 'adausdc', 'neopax', 'neousdc', 'dashbnb', 'dashusdt', 'nanousdt', 'omgbnb', 'omgusdt', 'thetausdt', 'enjusdt', 'mithusdt', 'maticbnb', 'maticbtc', 'maticusdt', 'atombnb', 'atombtc', 'atomusdt', 'atomusdc', 'atompax', 'atomtusd', 'etcusdc', 'etcpax', 'etctusd', 'batusdc', 'batpax', 'battusd', 'phbbnb', 'phbbtc', 'phbusdc', 'phbtusd', 'phbpax', 'tfuelbnb', 'tfuelbtc', 'tfuelusdt', 'tfuelusdc', 'tfueltusd', 'tfuelpax', 'onebnb', 'onebtc', 'oneusdt', 'onetusd', 'onepax', 'oneusdc', 'ftmbnb', 'ftmbtc', 'ftmusdt', 'ftmtusd', 'ftmpax', 'ftmusdc']

Too long url, need to split later

markets = [] # Get all markets
r = requests.get(url='https://www.binance.com/api/v1/exchangeInfo')
data = r.json()
for elem in data['symbols']:
    if elem['status'] == 'TRADING':
        markets.append(elem['symbol'].lower()) """

print('Connecting to ' + str(len(markets)) + ' market')

binance_get_kline_stream_id1 = binance_websocket_api_manager.create_stream(channels, markets)

while True:
	runsince = int(time.time() - start_time)
	runsince = str(datetime.timedelta(seconds=runsince))
	your_text = runsince + '  Inserted rows: ' + str("{:,}".format(all_sql_wrote))
	binance_websocket_api_manager.print_summary(add_string=your_text)
	time.sleep(1)
