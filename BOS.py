'''
Pseudocode:

Execution start part:
1. login and generate access token of Fyres & Kite.
2. sleep upto 9:15am,

Intialisation part:
3. call trade_log_df and capital_mgt_df
4. get near expiry date for BANKNIFTY,
5. get master contract and filter for BANKNIFTY, CE & PE, expiry_date and get ltp_dataframe (columns name: trading_symbol, instrument token, strike_price, ltp of all CE & PE strike prices,
6. intial_lots = 100.

Fetch & Update the dateFormat
7. get ltp of all strike prices with using apply function and fill the ltp price in ltp column for all strike prices.
8. to get the CE & PE traiding symbol, filter the dataframe with CE or PE and get the minimum value of (ltp - 80)
9. get the historical option data from XTS at 9:30 of CE & PE trading symbol for last 15 minutes named: CE_df & PE_df.
10. Apply RSI indicator for period 14 on both CE_df and PE_df.
11. get the highest value of the day: compare sorted close price with latest close price

Signal generation:
12. (rsi_value > 60) & current close price > day highest close price and get close price of trading symbol for option buy

Order placement:
13. check system_points, if system_points < 200, no change in initial lots and if system_points//100* > 2, add (pnl_points//100) in initial lots. (*100 will be variable.)
Execute in slices of 25 lots. inital slice will be start after lots > 50
14. divide intial_lots in 4 parts, if divisble by 4 else, subtract the intial_lots%4 in intial_lots and divide in 4 lots. This additional lots shall be added in first lot.
15. all parts of lot price for limit order shall be get before order placement,
16. placed the order for every lots as per signal generation and wait for 1 second,
17. check the orders of all part of lots executed or not. If any one or more than one part of lots will not be excuted, then 10% of total lots of that to be modified to market order and remaining non executed lots to be modified with revised ltp.
18. repeate the above point till complition of order placement process.


Trailing Stoploss:

19. get initial stoploss price: entry_price * 0.3 and placed stoploss limit order in four parts as per following.
    first parts of lot price for limit order = trading_symbol_current_price,
    second parts of lot price for limit order = trading_symbol_current_price - 1,
    second parts of lot price for limit order = trading_symbol_current_price + 1,
    second parts of lot price for limit order = trading_symbol_current_price + 2,
20. trail stoploss as per following: trail stoploss price required ticker price of trading symbol to get current_price.
    if close price >= entry_price*(1+initial_stoploss_pct):
        stoploss_pct = (close price - entry_price) / entry_price
        trail stroploss: inital stoploss * (1 + stoploss_pct)
    if new trail stoploss < trail stoploss:
        trail stoploss
    else:
        new trail stoploss
    if trail stoploss > current_price:
        exit order to be placed
    check the orders of all part of lots executed or not. If any one or more than one part of lots will not be excuted, then one part of remaining lots to be placed orders as market order and remaining part of lots to be modified with revised ltp.
21. update the trade_log_df after execution of all trades.


General Points:
21. if time > 11:00AM stop signal generation process,
22. time > 13:00 and time < 15:00 Repeate the Signal generation, Order placement and Trailing Stoploss process

Exit based on time:
23. if time > 15:25 exit the all trades

Dataframe for store trade log data: trade_log_df
columns_name = ['date', 'trading_symbol', 'option_type', 'entry_time', 'quantity', 'requested_entry_price',
    'average_entry_price', 'exit_time', 'requested_exit_price', 'average_exit_price', 'exit_reason', 'pnl', 'system_points']

system_points = average_exit_price - average_entry_price.
pnl = system_points * quantity
Store the trade data after each trade is over.
The quantity for the next trade is determined by finding the system_points sum of all the entries in the dataframe.

System parameters: capital_mgt_df
Another dataframe that will store the system parameters - initial_capital, current_capital, initial_lots, current_lots, points_change_for_lot_size_change, addon-deleteon_capital


'''


import time
from jugaad_trader import Zerodha
import os
import requests
import pandas as pd
import datetime
import talib
import calendar
import json
import logging
import threading
import telegram
import time
from json import JSONEncoder
import uuid

from fyers_api import fyersModel
from fyers_api import accessToken

# Telegram credentials
bot_token = '1941215362:AAHQHxrmzXiZ_jvqAU3BYq8-Cp62pPoHxCU'
chat_id = -565361309
bot = telegram.Bot(token=bot_token)

# Class that has two functions. One that generates a trade id for a trade object that is generated upon an entry signal
# Second a function that return a unix timestamp.
class Utils:
    dateFormat = "%Y-%m-%d"

    @staticmethod
    def generateTradeID():
        return str(uuid.uuid4())

    @staticmethod
    def getEpoch(datetimeObj=None):
        # This method converts given datetimeObj to epoch seconds
        if datetimeObj == None:
            datetimeObj = datetime.datetime.now()
        epochSeconds = datetime.datetime.timestamp(datetimeObj)
        return int(epochSeconds)  # converting double to long

    @staticmethod
    def getTodayDateStr():
      return Utils.convertToDateStr(datetime.datetime.now())

    @staticmethod
    def convertToDateStr(datetimeObj):
      return datetimeObj.strftime(Utils.dateFormat)

    @staticmethod
    def convertEpochToDateObj(epoch):
        return datetime.datetime.fromtimestamp(epoch)

# This class is used to create individual trade objects and has all the relevant information that is required for...
# ...managing the trade.
class Trade:
    def __init__(self, tradingSymbol=None):
        self.tradeID=Utils.generateTradeID()
        self.tradingSymbol=tradingSymbol # this is the symbol to be used for jugaad trader
        self.executiontradingSymbol=None # this is the symbol to be used for fyres_obj
        self.strategy='BOS' # strategy name
        self.optionType=None # call or put
        self.direction="" # Buy or Sell
        self.productType=ProductType.INTRADAY # to be used while placing order where product type is required
        self.isMarket=False # set to True if you want to place market order
        self.intradaySquareOffTimestamp=None # as the name suggests
        self.requestedEntry=0  # Requested entry
        self.signalEntryPrice=0
        self.entry_average_price=0 # Average price at which entry order filled
        self.qty=0  # Requested quantity
        self.filledQty=0  # In case partial fill, qty is not equal to filled quantity
        self.initialStopLoss=None  # Initial stop loss
        self.stopLoss=None  # This is the current stop loss. In case of trailing SL the current stopLoss and initialStopLoss will be different after some time
        self.cmp=0  # Last traded price
        self.tradeState=TradeState.CREATED  # state of the trade. Check TradeState class for other states
        self.timestamp=None
        self.createTimestamp=Utils.getEpoch() # Timestamp when the trade is created (Not triggered)
        self.startTimestamp=None  # Timestamp when the trade gets triggered and order placed
        self.endTimestamp=None  # Timestamp when the trade ended
        self.pnl=0  # Profit loss of the trade. If trade is Active this shows the unrealized pnl else realized pnl
        self.pnlPercentage=0  # Profit Loss in percentage terms
        self.exit_average_price=0 # Average price at which exit order filled
        self.exitReason=None  # SL/Target/SquareOff/Any Other
        self.entryOrder=[]  # list of Object of class Order (atributes of class Order...)
        self.slOrder=[]  # list of Object of Class Order



    def equals(self, trade):  # compares to trade objects and returns True if equals
        if trade == None:
            return False
        if self.tradeID == trade.tradeID:
            return True
        if self.tradingSymbol != trade.tradingSymbol:
            return False
        if self.productType != trade.productType:
            return False
        if self.requestedEntry != trade.requestedEntry:
            return False
        if self.qty != trade.qty:
            return False
        if self.timestamp != trade.timestamp:
            return False
        return True

    # Returns this when the object is used in print or logging. We can include the attributes that we want printed.
    def __str__(self):

        return "ID=" + str(self.tradeID) + ", state=" + self.tradeState + ", symbol=" + self.tradingSymbol \
            + ", strategy=" + self.strategy + ", productType=" + self.productType + ", reqEntry=" + str(self.requestedEntry) + ", reqQty=" + str(self.qty) \
            + ", trailing_stopLoss=" + str(self.stopLoss) + ", entry=" + str(self.entry_average_price) + ", exit=" + str(self.exit_average_price) \
            + ", profitLoss" + str(self.pnl)


class TradeState:
    CREATED='created'  # Trade created but not yet order placed, might have not triggered
    ACTIVE='active'  # order placed and trade is active
    COMPLETED='completed'  # completed when exits due to SL/Target/SquareOff
    CANCELLED='cancelled'  # cancelled/rejected comes under this state only
    DISABLED='disabled'  # disable trade if not triggered within the time limits or for any other reason

# This is used in ZerodhaOrderManager. Fyres uses numbers to convey order status.
class OrderStatus:
  OPEN="OPEN"
  COMPLETE="COMPLETE"
  OPEN_PENDING="OPEN PENDING"
  VALIDATION_PENDING="VALIDATION PENDING"
  PUT_ORDER_REQ_RECEIVED="PUT ORDER REQ RECEIVED"
  TRIGGER_PENDING="TRIGGER PENDING"
  REJECTED="REJECTED"
  CANCELLED="CANCELLED"

# Intraday and Margin are used in Fyres derivative orders. CNC is common to both Fyres and Zerodha.
class ProductType:
  MIS = "MIS"
  NRML = "NRML"
  CNC = "CNC"
  INTRADAY = "INTRADAY"
  MARGIN = "MARGIN"

# Order types are common to both Zerodha and Fyres
class OrderType:
  LIMIT = "LIMIT"
  MARKET = "MARKET"
  SL_MARKET = "SL_MARKET"
  SL_LIMIT = "SL_LIMIT"

# Used for buy or sell which placing order.
class Direction:
  LONG = "LONG"
  SHORT = "SHORT"

# Used for storing trade object as a json.
class TradeEncoder(JSONEncoder):
  def default(self, o):
    return o.__dict__

# This class is used to create individual order objects and has all the relevant information that is required for...
# ...managing an order.
class Order:
  def __init__(self, orderInputParams=None):
    self.tradingSymbol=orderInputParams.tradingSymbol if orderInputParams != None else "" # Use the Fyres symbol here
    self.price=orderInputParams.price if orderInputParams != None else 0

    self.triggerPrice=orderInputParams.triggerPrice if orderInputParams != None else 0 # Applicable in case of SL orders
    self.qty=orderInputParams.qty if orderInputParams != None else 0
    self.orderId=None  # The order id received from broker after placing the order
    self.modifiedOrderId=None
    self.orderStatus=None  # One of the status defined in ordermgmt.OrderStatus
    self.averagePrice=0  # Average price at which the order is filled
    self.filledQty=0  # Filled quantity
    self.pendingQty=0  # Qty - Filled quantity
    self.orderPlaceTimestamp=None  # Timestamp when the order is placed
    self.lastOrderUpdateTimestamp=None  # Applicable if you modify the order Ex: Trailing SL
    self.message=None  # In case any order rejection or any other error save the response from broker in this field
    self.lastMessage=None

  def __str__(self):
    return "orderId=" + str(self.orderId) + ", orderStatus=" + str(self.orderStatus) \
      + ", symbol=" + str(self.tradingSymbol) + ", productType=" + str(self.productType) \
      + ", orderType=" + str(self.orderType) + ", price=" + str(self.price) \
      + ", triggerPrice=" + str(self.triggerPrice) + ", qty=" + str(self.qty) \
      + ", filledQty=" + str(self.filledQty) + ", pendingQty=" + str(self.pendingQty) \
      + ", averagePrice=" + str(self.averagePrice)

# This class is used to send order information to the Order management api for placing orders.
class OrderInputParams:
    def __init__(self, tradingSymbol):
        self.productType=ProductType.INTRADAY  # default as we are using Fyres.
        self.tradingSymbol=tradingSymbol
        self.direction=""
        self.orderType=""  # One of the values of class OrderType
        self.qty=0
        self.price=0
        self.triggerPrice=0  # Applicable in case of SL order
        self.exchange='NFO' # USed in ZerodhaOrderManager
        self.segment='NFO-OPT'

    def __str__(self):
        return "symbol=" + str(self.tradingSymbol) + ", exchange=" + self.exchange \
            + ", productType=" + self.productType + ", segment=" + self.segment \
            + ", direction=" + self.direction + ", orderType=" + self.orderType \
            + ", qty=" + str(self.qty) + ", price=" + str(self.price) + \
            ", triggerPrice=" + str(self.triggerPrice)

# This class is used to send order information to the Order management api for modifying existing orders.
class OrderModifyParams:
  def __init__(self):
    self.newPrice=0
    self.newTriggerPrice=0  # Applicable in case of SL order
    self.newQty=0
    self.newOrderType=None  # Ex: Can change LIMIT order to SL order or vice versa. Not supported by all brokers

  def __str__(self):
    return + "newPrice=" + str(self.newPrice) + ", newTriggerPrice=" + str(self.newTriggerPrice) \
      + ", newQty=" + str(self.newQty) + ", newOrderType=" + str(self.newOrderType)

# This is for zerodha jugaad trader login
class OMS():

    @staticmethod
    def jugard_trade_login():

        os.system('jtrader zerodha startsession')
        jugaad_trader_kite = Zerodha()
        jugaad_trader_kite.set_access_token()

        print(jugaad_trader_kite.ltp(['NSE:MARUTI'])) # This will show successful login

        return jugaad_trader_kite

# Fyers order management system.
class Fyers:

    def __init__(self, bot, chat_id):

        try:
            file = open("fyersToken.txt","r")
            access_token = file.read()
            file.close()

            #df = pd.read_pickle('credentials.pkl')
            app_id = "SXHBJ2V13T-100" #df['app_id']

            self.fyers = fyersModel.FyersModel(client_id=app_id,
                                           token=access_token,
                                           log_path='/home/prtkrock/final_Code/BOS')
            #---------------------------------------------------telegram starts
            self.bot = bot
            self.chat_id = chat_id
            self.bot.send_message(chat_id=self.chat_id,text=f'Logged in at {datetime.datetime.now()}')
            #---------------------------------------------------telegram ends

            self.master_contract_nfo = self.master_contract_nfo() # Gets the NFO instrument list
            self.master_contract_nse_cm = self.master_contract_nse_cm() # Gets the cash market instrument list.

        except Exception as e:
            print('Error while creating fyers object.')
            print(e)

    def master_contract_nse_cm(self):

        response = requests.get('http://public.fyers.in/sym_details/NSE_CM.csv').text
        response_text = response.split('\n')
        master_contract = pd.DataFrame(list(map(lambda x: x.split(','), response_text)))
        master_contract = master_contract.drop(10, axis=1)
        master_contract.columns = ['fyers_token', 'symbol_details', 'exchange_instrument_type',
                                   'minimum_lot_size', 'tick_size', 'isin', 'trading_session',
                                   'last_update_date', 'expiry_date', 'symbol_ticker', 'exchange', 'segment', 'scrip_code']
        master_contract = master_contract.iloc[:-1]
        master_contract.sort_values('symbol_ticker', inplace=True)
        master_contract.reset_index(drop=True, inplace=True)
        master_contract['exchange'] = master_contract['symbol_ticker'].apply(
            lambda x: x.split(':')[0])

        return master_contract

    def master_contract_nfo(self):

        response = requests.get('http://public.fyers.in/sym_details/NSE_FO.csv').text
        response_text = response.split('\n')

        master_contract = pd.DataFrame(list(map(lambda x: x.split(','), response_text)))
        master_contract = master_contract.drop(10, axis=1)
        master_contract.columns = ['fyers_token', 'symbol_details', 'exchange_instrument_type',
                                   'minimum_lot_size', 'tick_size', 'isin', 'trading_session',
                                   'last_update_date', 'expiry_date', 'symbol_ticker', 'exchange', 'segment', 'scrip_code']
        master_contract['expiry_date'] = master_contract['expiry_date'].apply(
            lambda x: datetime.datetime.fromtimestamp(int(x)).date() if x != None else x)
        master_contract = master_contract.iloc[:-1]

        # def get_instrument_details(details):
        #
        #     try:
        #         year, month, day, strike_price, instrument_type = re.findall(r'(21|22|23|24|25) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-3][0-9]) ([0-9]{1,5}|[0-9]{1,5}.[0-9]{1,2}) (CE|PE)', details)[0]
        #     except:
        #         year, month, day, instrument_type = re.findall(r'(21|22) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-3][0-9]) (FUT)', details)[0]
        #         strike_price = None
        #
        #     expiry_date = datetime.datetime.strptime((year+month+day).upper(), '%y%b%d').date()
        #     return expiry_date, strike_price, instrument_type
        #
        # master_contract['temp'] = np.vectorize(get_instrument_details, otypes=[
        #                                        "O"])(master_contract['symbol_details'])
        # master_contract['strike_price'] = master_contract['temp'].apply(lambda x: x[1])
        # master_contract['strike_price'] = master_contract['strike_price'].astype(float)
        # master_contract['instrument_type'] = master_contract['temp'].apply(lambda x: x[2])
        # master_contract.drop('temp', axis=1, inplace=True)
        # master_contract.sort_values('expiry_date', inplace=True)
        # master_contract.reset_index(drop=True, inplace=True)

        return master_contract

    def get_instrument_details(self, symbol, strike=None, instrument_type='EQ', expiry_offset=0):
        '''
        symbol is the instrument name
        strike is the strike price of the option and is not compulsory to feed in
        instrument_type can be 'EQ', 'CE', 'PE' or 'FUT'
        expiry_offset is the expiry we want to trade in 0 for near, 1 for next and so on
        '''

        if instrument_type == 'EQ':
            return self.master_contract_nse_cm[master_contract_nse_cm['scrip_code'] == symbol].iloc[0]

        elif (instrument_type == 'CE') | (instrument_type == 'PE'):
            return self.master_contract_nfo[(self.master_contract_nfo['scrip_code'] == symbol) & (self.master_contract_nfo['strike_price'] == strike) & (self.master_contract_nfo['instrument_type'] == instrument_type)].iloc[expiry_offset]

        elif instrument_type == 'FUT':
            return self.master_contract_nfo[(self.master_contract_nfo['scrip_code'] == symbol) & (self.master_contract_nfo['instrument_type'] == instrument_type)].iloc[expiry_offset]

    def get_profile(self):

        try:
            return self.fyers.get_profile()
        except:
            return None


    def get_orderbook(self):

        try:
            return self.fyers.orderbook()['orderBook']
        except:
            return None

    def get_tradebook(self):

        try:
            return self.fyers.tradebook()['tradeBook']
        except:
            return None


    def get_open_positions(self):

        try:
            return self.fyers.positions()['netPositions']
        except:
            return None

    def place_order(self, orderInputParams):
        '''
        ticker = "NSE:SBIN-EQ"
        trade_type = 'BUY' or 'SELL'
        quantity = 500
        product_type = 'INTRADAY'
        order_type = 'MARKET' or 'LIMIT'
        limit_price = price if order is limit type else 0 for market order
        '''
        if orderInputParams.orderType == OrderType.LIMIT:
            type = 1
            if orderInputParams.direction == Direction.LONG:
                side = 1
            elif orderInputParams.direction == Direction.SHORT:
                side = -1
            limit_price = orderInputParams.price
            stop_price = 0

        elif orderInputParams.orderType == OrderType.MARKET:
            type = 2
            if orderInputParams.direction == Direction.LONG:
                side = 1
            elif orderInputParams.direction == Direction.SHORT:
                side = -1
            limit_price = 0
            stop_price = 0

        elif orderInputParams.orderType == OrderType.SL_LIMIT:
            type = 4
            if orderInputParams.direction == Direction.LONG:
                side = 1
            elif orderInputParams.direction == Direction.SHORT:
                side = -1
            limit_price = orderInputParams.price
            stop_price = orderInputParams.triggerPrice

        data = {"symbol": orderInputParams.tradingSymbol,
                "qty": abs(orderInputParams.qty),
                "type": type,
                "side": side,
                "productType": orderInputParams.productType,
                "limitPrice": limit_price,
                "stopPrice": stop_price,
                "validity": "DAY",
                "disclosedQty": 0,
                "offlineOrder": "False",
                "stopLoss": 0,
                "takeProfit": 0}

        try:
            order_fyers = self.fyers.place_order(data)
            message = order_fyers['message']
            orderId = order_fyers['id']
            logging.info(f'Fyres {message}')

            order=Order(orderInputParams)
            order.orderId=orderId
            order.orderPlaceTimestamp=Utils.getEpoch()
            order.lastOrderUpdateTimestamp=Utils.getEpoch()
            order.direction=orderInputParams.direction
            order.message=message
            order.lastMessage=message

            self.bot.send_message(chat_id=self.chat_id,text=f'Order placed with parameters {orderInputParams} at {datetime.datetime.now()}')

            return order

        except Exception as e:
            self.bot.send_message(chat_id=self.chat_id,text=f'Failed to place order with parameters {orderInputParams} at {datetime.datetime.now()}')
            logging.info('Fyres order placement failed: %s', str(e))
            raise Exception(str(e))

    # To modify the price of a limit order.
    def modifyLimitOrderPrice(self, order, ordermodifyParams):

        orderId = order.orderId
        limit_price = ordermodifyParams.newPrice
        type = 1

        data = {
                  "id":orderId,
                  "type": type,
                  "limitPrice": limit_price
               }
        try:
            order_fyers = self.fyers.modify_order(data)
            message = order_fyers['message']
            orderId = order_fyers['id']
            logging.info(f'Fyres {message}')
            logging.info('Fyres order modified successfully for orderId = %s', orderId)
            order.lastOrderUpdateTimestamp=Utils.getEpoch()
            order.lastMessage=message

            self.bot.send_message(chat_id=self.chat_id,text=f'Limit order price modified to {limit_price} at {datetime.datetime.now()}')

            return order
        except Exception as e:
            self.bot.send_message(chat_id=self.chat_id,text=f'Failed to modify order {order} with parameters {ordermodifyParams} at {datetime.datetime.now()}')
            logging.info('Fyres order modification failed: %s', str(e))
            raise Exception(str(e))

    # To modify stop loss orders that changes the trigger price.
    def modifySLLimitOrderPrice(self, order, ordermodifyParams):

        orderId = order.orderId
        stop_price = ordermodifyParams.newTriggerPrice
        limit_price = ordermodifyParams.newPrice
        type = 4

        data = {
                  "id":orderId,
                  "type": type,
                  "limitPrice": limit_price,
                  "stopPrice": stop_price
               }
        try:
            order_fyers = self.fyers.modify_order(data)
            message = order_fyers['message']
            orderId = order_fyers['id']
            logging.info(f'Fyres {message}')
            logging.info('Fyres order modified successfully for orderId = %s', orderId)
            order.lastOrderUpdateTimestamp=Utils.getEpoch()
            order.lastMessage=message

            self.bot.send_message(chat_id=self.chat_id,text=f'Stop loss updated to trigger price {stop_price} at {datetime.datetime.now()}')
            return order
        except Exception as e:
            self.bot.send_message(chat_id=self.chat_id,text=f'Failed to modify stoploss order {order} with parameters {ordermodifyParams} at {datetime.datetime.now()}')
            logging.info('Fyres order modification failed: %s', str(e))
            raise Exception(str(e))

    # To change the order type to market order.
    def modifyOrderToMarket(self, order):

        orderId = order.orderId
        self.cancel_order(orderId)

        type = 2

        if order.direction == Direction.LONG:
            side = 1
        elif order.direction == Direction.SHORT:
            side = -1

        limit_price = 0
        stop_price = 0

        data = {"symbol": order.tradingSymbol,
                "qty": abs(order.qty),
                "type": type,
                "side": side,
                "productType": ProductType.INTRADAY,
                "limitPrice": limit_price,
                "stopPrice": stop_price,
                "validity": "DAY",
                "disclosedQty": 0,
                "offlineOrder": "False",
                "stopLoss": 0,
                "takeProfit": 0}

        try:
            order_fyers = self.fyers.place_order(data)
            message = order_fyers['message']
            orderId = order_fyers['id']
            logging.info(f'Fyres {message}')
            logging.info('Fyres order modified to market successfully with new orderId = %s', orderId)
            order.lastOrderUpdateTimestamp=Utils.getEpoch()
            order.orderId = orderId
            order.lastMessage = message

            # self.bot.send_message(chat_id=self.chat_id,text=f'Stop loss order modified to market order')

            return order
        except Exception as e:
            self.bot.send_message(chat_id=self.chat_id,text=f'Failed to modify order {order} to market at {datetime.datetime.now()}')
            logging.info('Fyres order modification to market failed: %s', str(e))
            raise Exception(str(e))

    def stop_loss_order(self, order_id, stop_loss_percentage, order_type=3, base=0.05):
        '''
        order_type = 3 means stop_loss market order
        '''
        for order in self.get_orderbook():
            if order['id'] == order_id['id']:
                if order['message'] == 'TRADE CONFIRMED':
                    traded_price = order['tradedPrice']
                    filled_quantity = order['filledQty']
                    ticker = order['symbol']
                    product_type = order['productType']

        if filled_quantity > 0:
            stop_loss_side = -1
            stop_loss_price = traded_price - traded_price*stop_loss_percentage/100
            print(stop_loss_price)

        elif filled_quantity < 0:
            stop_loss_side = 1
            stop_loss_price = traded_price + traded_price*stop_loss_percentage/100

        stop_loss_price = round(base * round(stop_loss_price/base), 2)

        data = {
            "symbol": ticker,
            "qty": filled_quantity,
            "type": order_type,
            "side": stop_loss_side,
            "productType": product_type,
            "limitPrice": 0,
            "stopPrice": stop_loss_price,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": "False",
        }

        try:
            stop_loss_order_id = self.fyers.place_order(data)['id']
            return stop_loss_order_id
        except:
            logging.info('Fyres stop loss order placement failed')
            return None

    def close_position(self, symbol):
        '''
        symbol is required
        '''
        self.fyers.exit_positions({'symbol': symbol})

    def close_all_open_positions(self):

        open_positions = []

        for position in self.get_open_positions():
            if abs(position['qty']) > 0:
                open_positions.append(position['symbol'])

        for position in open_positions:
            self.close_position({'symbol': position})

    def cancel_order(self, order_id):
        '''
        orders cancelled by order_id
        '''
        self.fyers.cancel_order({'id': order_id})

    def cancel_all_orders(self):

        open_stop_loss_order_id = []
        for order in self.get_orderbook():
            # order['type'] == 3 means stop_loss market order
            # order['status'] == 6 means order status is pending
            if ((order['type'] == 3) | (order['type'] == 4)) & (order['status'] == 6):
                open_stop_loss_order_id.append(order['id'])

        for order_id in open_stop_loss_order_id:
            self.cancel_order(order_id)

    def get_ltp(self, trading_symbol):

        data = {"symbols": f"NSE:{trading_symbol}"}
        quote = self.fyers.quotes(data)['d']
        ltp = quote['v']['lp']
        return ltp

# Class that generates the strategy signal and sends orders.
class BOS():

    def __init__(self, bot, chat_id):

        self.create_logging_file()

        self.app_id = 'SXHBJ2V13T-100'
        self.app_secret = 'THCKR2YIMC'
        self.client_id='XP00637'
        self.password='India@2021'
        self.two_fa='AEYPM9611H'
        self.marketStartTime = self.getTimeOfDay(9, 15, 0) # time that market starts.
        self.marketstoptime = self.getTimeOfDay(15, 30, 0) # time that market ends.
        self.morningstarttime = self.getTimeOfDay(9, 29, 0) # strategy has to start at 9:29 in the morning so that symbol can be generated by 9:30.
        self.morningstoptime = self.getTimeOfDay(11, 0, 0) # morning phase end time
        self.afternoonstarttime = self.getTimeOfDay(12, 59, 0) # afternoon phase start time.
        self.afternoonstoptime = self.getTimeOfDay(15, 0, 0) # afternoon phase end time.
        self.morningtradestarttime = self.getTimeOfDay(9, 30, 0)
        self.afternoontradestarttime = self.getTimeOfDay(13, 0, 0) # afternoon phase start time.
        self.intradaySquareOffTimestamp = self.getTimeOfDay(15, 9, 0) # square off time.
        self.ce_entry_signal = 0 # entry signal flag for call option
        self.pe_entry_signal = 0 # entry signal flag for put option
        self.signalEntryPrice = 0 # Price when signal is generated
        self.lookbackdays = 1 # number of days for historical dataframe
        self.historical_data_interval = 'minute' # historical data timeframe in historical dataframe.
        self.rsi_timeperiod = 14 # lookback period for calculating rsi.
        self.maxmorningtrades = 1 # sets the limit for the maximum morning trades
        self.maxafternoontrades = 1 # sets the limit for the maximum afternoon trades
        self.morning_trade = 0 # counter for the morning trade
        self.afternoon_trade = 0 # counter for the afternoon trade
        self.initial_stoploss_pct = 30 # stoploss percentage
        self.trades = [] # list of the trade objects of the Trade class
        self.lots_per_order = 25 # to be used for slicing orders into 25 lots at a time. if trading 100 lots, orders get split in to 4
        self.lotsize = 25 # contrat size for each lot
        self.initial_capital = 200000 # initial capital to be used
        self.systemCapitalPerLot = 50000 # system capital requirement for a single lots
        self.systemCompoundingPerLot = 100 # profit earned in a system at which number of lots traded is increased by 1.
        self.base = 0.05
        self.bot = bot # telegram bot
        self.chat_id = chat_id
        self.get_bnf_option_df() # gets the banknifty option contract list for the latest expiry
        logging.info('Created BOS object')

    def create_logging_file(self):

        # starts a logging file
        logFileDir = '/home/prtkrock/final_Code/BOS/LOG_FILE_DIRECTORIES/'
        if os.path.exists(logFileDir) == False:
            print("LogFile Directory " + logFileDir + " does not exist. Making the directory.")
            os.makedirs(logFileDir)

        date_now = datetime.datetime.now()
        date_now_string = date_now.strftime("%d_%b_%Y")
        filepath = logFileDir + "Logging_" + date_now_string + ".log"
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(filename=filepath, format=format,
                            level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")

    # Return time of day in full datetime format
    def getTimeOfDay(self, hours, minutes, seconds, dateTimeObj=None):
        if dateTimeObj == None:
            dateTimeObj = datetime.datetime.now()
        dateTimeObj = dateTimeObj.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
        return dateTimeObj

    # Login to jugaad trader and gets the zerodha instrument list for bnf for options in the lastest expiry
    def login_jugard_trade(self):
        self.kite = OMS.jugard_trade_login()
        self.zerodhacontractlist = self.get_master_contract()
        self.zerodhacontractlist = self.zerodhacontractlist[self.zerodhacontractlist['name'] == 'BANKNIFTY']
        expiryDate = self.zerodhacontractlist['expiry'].unique()[0]
        self.zerodhacontractlist = self.zerodhacontractlist[self.zerodhacontractlist['expiry'] == expiryDate]

    def re_login_jugard_trade(self):
        self.kite = OMS.jugard_trade_login()

    # Login to fyres and gets the fyres instrument list for bnfs for options in the latest expiry
    def fyres_login(self):
        self.fyres_obj = Fyers(self.bot, self.chat_id)#app_id=self.app_id, app_secret=self.app_secret, client_id=self.client_id, password=self.password, two_fa=self.two_fa)
        self.fyrescontractlist = self.fyres_obj.master_contract_nfo[self.fyres_obj.master_contract_nfo['scrip_code'] == 'BANKNIFTY']
        expiryDate = self.fyrescontractlist['expiry_date'].unique()[0]
        self.fyrescontractlist = self.fyrescontractlist[self.fyrescontractlist['expiry_date'] == expiryDate]

        self.fyrescontractlist['option_type'] = self.fyrescontractlist['symbol_details'].apply(lambda x: x.split()[-1])
        self.fyrescontractlist['strike'] = self.fyrescontractlist['symbol_details'].apply(lambda x: float(x.split()[-2]))

    # gets the master contract for zerodha
    def get_master_contract(self):

        try:
            response = requests.get('http://api.kite.trade/instruments')
            response_text = response.text
            response_text = response_text.split('\n')
            master_contract = pd.DataFrame(list(map(lambda x: x.split(','), response_text)))
            master_contract.columns = master_contract.loc[0].values
            master_contract = master_contract.iloc[1:]
            master_contract = master_contract[master_contract['tradingsymbol'].apply(
                lambda x: x is not None)]
            master_contract['instrument_token'] = master_contract['instrument_token'].apply(int) # converts instrument token to integer format
            master_contract['expiry'] = master_contract[master_contract['expiry'] != '']['expiry'].apply(
                lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date()) # converts expiry to datetime fromat
            master_contract['strike'] = master_contract['strike'].apply(float) # converts strike to float format
            master_contract['name'] = master_contract['name'].apply(lambda x: str(x)[1:-1])
            master_contract.sort_values('expiry', inplace=True)
            return master_contract
        except Exception as e:
            logging.info(f"Could not get master contract :: {e}")
            return None

    # function to get the lastest expiry banknifty contracts
    def get_bnf_option_df(self):
        count = 0
        while True:
            try:
                self.bnf_option_df = self.get_master_contract()
                self.bnf_option_df = self.bnf_option_df[(self.bnf_option_df['name'] == 'BANKNIFTY') & (
                    self.bnf_option_df['segment'] == 'NFO-OPT')]
                self.get_current_expiry()
                self.bnf_option_df = self.bnf_option_df[self.bnf_option_df['expiry'] == self.expiry_date]
                logging.info("Saved banknifty option contract list")
                break
                # self.bnf_option_df = self.bnf_option_df[self.bnf_option_df['expiry'] == sorted(
                #     self.bnf_option_df['expiry'])[0]]
                # self.bnf_option_df = self.bnf_option_df[[
                #     'instrument_token', 'tradingsymbol', 'strike', 'instrument_type', 'expiry']]
            except Exception as e:
                if count >= 180:
                    logging.info(f"Could not get banknifty option contract list")
                    break
                logging.info(f"Could not get banknifty option contract list :: {e}")
                time.sleep(5)
                logging.info('Trying again')
                count += 1
                continue

    # function to get the latest expiry date. Gets the year, month, day in seperate variables.
    def get_current_expiry(self):

        self.expiry_date = sorted(self.bnf_option_df['expiry'])[0]

    # returns the instrument token for a trading symbol
    def get_instrument_token(self, trading_symbol):

        instrument_token = self.kite.ltp(f'NFO:{trading_symbol}')[
            f'NFO:{trading_symbol}']['instrument_token']

        return instrument_token

    # function that iterates over call trading symbols till we reach the one with price just above 80
    # we start with a ce_strike_price and change strike price till crtieria is met
    # we call this function when the ltp of the trading symbol with ce_strike_price is less than 80.

    # The trading symbol is generated over two phases. In the first phase we get the trading symbol for call...
    # ...and in the second phase we confirm whether it is the right trading symbol. The first phase, context is 'initial' and...
    # ...second phase context is 'check'. When context is 'check' we get the final trading symbol based on which...
    # ...signal is generated.

    # As an example when a trading symbol needs to be checked for signal  at 9:30, we generate an initial trading symbol at 9:29. This is the 'initial' phase...
    # ... then when there are 3 seconds left for 9:30, we check whether the trading symbol is still the relevant one. This is the 'check' phase.
    def move_itm_ce(self, ce_strike_price, prev_trading_symbol_ce, context):

            while True:
                # ce_strike_price is the strike at which we start
                x = self.bnf_option_df[(self.bnf_option_df['strike'] == ce_strike_price) & (self.bnf_option_df['instrument_type'] == 'CE')].iloc[0]['tradingsymbol']
                ltp = self.kite.ltp([f'NFO:{x}'])[f'NFO:{x}']['last_price']
                if ltp < 80:
                    prev_trading_symbol_ce = x
                    ce_strike_price -= 100 # if ltp is less than 80, we move the strike down and check the ltp of the next trading symbol for the new trading symbol
                    time.sleep(0.1) # sleep for 0.1 seconds, otherwhise jugaad trader gets too many requests error.
                    continue
                else:
                    if context == 'initial':
                        self.ce_trading_symbol = x # sets a trading symbol in the initial phase.
                        self.ce_strike = ce_strike_price
                        ltp = self.get_ltp(self.ce_trading_symbol)
                        logging.info(f'ce_trading_symbol is: {self.ce_trading_symbol}')
                    elif context == 'check':
                        self.final_ce_trading_symbol = x # Sets the final trading symbol
                        self.final_ce_trading_symbol_token = self.get_instrument_token(self.final_ce_trading_symbol) # Sets the final trading symbol token
                        ltp = self.get_ltp(self.final_ce_trading_symbol) # Gets the ltp for logging purpose.
                        logging.info(f'final_ce_trading_symbol is: {self.final_ce_trading_symbol}')

                    logging.info(f'ce price: {ltp}')
                    break

    # In this function, the ltp of the call trading symbol starts above 80 and then we move the strike up till ee reach a strike...
    # ...with ltp just above 80.
    def move_otm_ce(self, ce_strike_price, prev_trading_symbol_ce, context):

            while True:
                x = self.bnf_option_df[(self.bnf_option_df['strike'] == ce_strike_price) & (self.bnf_option_df['instrument_type'] == 'CE')].iloc[0]['tradingsymbol']
                ltp = self.kite.ltp([f'NFO:{x}'])[f'NFO:{x}']['last_price']
                if ltp >= 80:
                    prev_trading_symbol_ce = x
                    ce_strike_price += 100
                    time.sleep(0.1)
                    continue
                else:
                    if context == 'initial':
                        self.ce_trading_symbol = prev_trading_symbol_ce
                        self.ce_strike = ce_strike_price - 100
                        ltp = self.get_ltp(self.ce_trading_symbol)
                        logging.info(f'ce_trading_symbol is: {self.ce_trading_symbol}')
                    elif context == 'check':
                        self.final_ce_trading_symbol = prev_trading_symbol_ce
                        self.final_ce_trading_symbol_token = self.get_instrument_token(self.final_ce_trading_symbol)
                        ltp = self.get_ltp(self.final_ce_trading_symbol)
                        logging.info(f'final_ce_trading_symbol is: {self.final_ce_trading_symbol}')

                    logging.info(f'ce price: {ltp}')
                    break

    # Same two functions for put option trading symbols as well.
    def move_itm_pe(self, pe_strike_price, prev_trading_symbol_pe, context):

            while True:
                x = self.bnf_option_df[(self.bnf_option_df['strike'] == pe_strike_price) & (self.bnf_option_df['instrument_type'] == 'PE')].iloc[0]['tradingsymbol']
                ltp = self.kite.ltp([f'NFO:{x}'])[f'NFO:{x}']['last_price']
                if ltp < 80:
                    prev_trading_symbol_pe = x
                    pe_strike_price += 100
                    time.sleep(0.1)
                    continue
                else:
                    if context == 'initial':
                        self.pe_trading_symbol = x
                        self.pe_strike = pe_strike_price
                        ltp = self.get_ltp(self.pe_trading_symbol)
                        logging.info(f'pe_trading_symbol is: {self.pe_trading_symbol}')
                    elif context == 'check':
                        self.final_pe_trading_symbol = x
                        self.final_pe_trading_symbol_token = self.get_instrument_token(self.final_pe_trading_symbol)
                        ltp = self.get_ltp(self.final_pe_trading_symbol)
                        logging.info(f'final_pe_trading_symbol is: {self.final_pe_trading_symbol}')

                    logging.info(f'pe price: {ltp}')
                    break

    # Same two functions for put option trading symbols as well.
    def move_otm_pe(self, pe_strike_price, prev_trading_symbol_pe, context):

            while True:
                x = self.bnf_option_df[(self.bnf_option_df['strike'] == pe_strike_price) & (self.bnf_option_df['instrument_type'] == 'PE')].iloc[0]['tradingsymbol']
                ltp = self.kite.ltp([f'NFO:{x}'])[f'NFO:{x}']['last_price']
                if ltp >= 80:
                    prev_trading_symbol_pe = x
                    pe_strike_price -= 100
                    time.sleep(0.1)
                else:
                    if context == 'initial':
                        self.pe_trading_symbol = prev_trading_symbol_pe
                        self.pe_strike = pe_strike_price + 100
                        ltp = self.get_ltp(self.pe_trading_symbol)
                        logging.info(f'pe_trading_symbol is: {self.pe_trading_symbol}')
                    elif context == 'check':
                        self.final_pe_trading_symbol = prev_trading_symbol_pe
                        self.final_pe_trading_symbol_token = self.get_instrument_token(self.final_pe_trading_symbol)
                        ltp = self.get_ltp(self.final_pe_trading_symbol)
                        logging.info(f'final_pe_trading_symbol is: {self.final_pe_trading_symbol}')

                    logging.info(f'pe price: {ltp}')
                    break

    # Matches the trading symbol from zerodha with trading symbol for Fyres
    def get_execution_trading_symbol(self, option_type=None):

        if option_type == 'CE':

            # get the strike for the trading symbol
            strike = self.zerodhacontractlist[self.zerodhacontractlist['tradingsymbol'] == self.final_ce_trading_symbol]['strike'].iloc[0]
            logging.info(f'strike is: {strike}')

            # get the trading symbol in Fyres
            self.executiontradingSymbol = self.fyrescontractlist[(self.fyrescontractlist['strike'] == strike) & (self.fyrescontractlist['option_type'] == 'CE')]['symbol_ticker'].iloc[0]
        elif option_type == 'PE':
            strike = self.zerodhacontractlist[self.zerodhacontractlist['tradingsymbol'] == self.final_pe_trading_symbol]['strike'].iloc[0]
            logging.info(f'strike is: {strike}')
            self.executiontradingSymbol = self.fyrescontractlist[(self.fyrescontractlist['strike'] == strike) & (self.fyrescontractlist['option_type'] == 'PE')]['symbol_ticker'].iloc[0]


    # Call this function to get trading symbols every minute. This is for the initial phase.
    def get_trading_symbol(self):

        # Gets the atm for banknifty
        atm = int(round(self.kite.ltp(['NSE:NIFTY BANK'])[
                  'NSE:NIFTY BANK']['last_price'] / 100) * 100)
        # Gets the call atm trading symbol
        x_ce = self.bnf_option_df[(self.bnf_option_df['strike'] == atm) & (self.bnf_option_df['instrument_type'] == 'CE')].iloc[0]['tradingsymbol']

        # Gets the call ltp.
        ltp_ce = self.kite.ltp([f'NFO:{x_ce}'])[f'NFO:{x_ce}']['last_price']

        # if the atm option ltp is less than 80, then we have to move the strikes down towards itm. else we have to move otm.
        if ltp_ce < 80:
            self.move_itm_ce(atm, x_ce, 'initial')
        else:
            self.move_otm_ce(atm, x_ce, 'initial')

        x_pe = self.bnf_option_df[(self.bnf_option_df['strike'] == atm) & (self.bnf_option_df['instrument_type'] == 'PE')].iloc[0]['tradingsymbol']
        ltp_pe = self.kite.ltp([f'NFO:{x_pe}'])[f'NFO:{x_pe}']['last_price']

        if ltp_pe < 80:
            self.move_itm_pe(atm, x_pe, 'initial')
        else:
            self.move_otm_pe(atm, x_pe, 'initial')

    # Call this function to check trading symbols every minute. This is for the check phase.
    def check_trading_symbol(self):

        ltp_ce = self.kite.ltp([f'NFO:{self.ce_trading_symbol}'])[f'NFO:{self.ce_trading_symbol}']['last_price']
        prev_trading_symbol_ce = self.ce_trading_symbol
        ce_strike_price = self.ce_strike

        if ltp_ce < 80:
            self.move_itm_ce(ce_strike_price, prev_trading_symbol_ce, 'check')
        else:
            self.move_otm_ce(ce_strike_price, prev_trading_symbol_ce, 'check')

        ltp_pe = self.kite.ltp([f'NFO:{self.pe_trading_symbol}'])[f'NFO:{self.pe_trading_symbol}']['last_price']
        prev_trading_symbol_pe = self.pe_trading_symbol
        pe_strike_price = self.pe_strike

        if ltp_pe < 80:
            self.move_itm_pe(pe_strike_price, prev_trading_symbol_pe, 'check')
        else:
            self.move_otm_pe(pe_strike_price, prev_trading_symbol_pe, 'check')

    # function returns the ltp for the trading symbol passed in the argument
    def get_ltp(self, trading_symbol):

        ltp = self.kite.ltp([f'NFO:{trading_symbol}'])[f'NFO:{trading_symbol}']['last_price']
        return ltp

    # funciton returns the latest low for the trading symbol. Used in the stop loss check.
    def get_trading_symbol_historical_data_low(self, trade):

        # sets the from and to date.
        try:
            count = 0
            while True:
                from_date = datetime.datetime.today() - datetime.timedelta(days=self.lookbackdays)
                to_date = datetime.datetime.today()
                now = datetime.datetime.now()
                token = self.get_instrument_token(trade.tradingSymbol)
                tradingSymboldf = pd.DataFrame(self.kite.historical_data(token, from_date, to_date, self.historical_data_interval))
                latestMinute = tradingSymboldf['date'].iloc[-1].minute
                # this condition ensures that low for the current minute is returned
                if now.minute == latestMinute:
                    low = tradingSymboldf['low'].iloc[-1]
                    return low
                else:
                    count += 1
                    if count >= 20:
                        logging.info(f'Could not get latest low for trading symbol {trade.tradingSymbol}')
                        return None

        except:
            logging.info(f'Failed to get historical data for trading symbol {trade.tradingSymbol}')
            self.bot.send_message(chat_id=self.chat_id,text=f'Failed to get historical data for trading symbol {trade.tradingSymbol} at {datetime.datetime.now()}')
            return None

    # gets the close for the minute ended for the trading symbol. Used to update the trailing stop loss.
    def get_trading_symbol_historical_data_close(self, trade):

        try:
            count = 0
            while True:
                from_date = datetime.datetime.today() - datetime.timedelta(days=self.lookbackdays)
                to_date = datetime.datetime.today()
                now = datetime.datetime.now()
                token = self.get_instrument_token(trade.tradingSymbol)
                tradingSymboldf = pd.DataFrame(self.kite.historical_data(token, from_date, to_date, self.historical_data_interval))
                latestMinute = tradingSymboldf['date'].iloc[-1].minute
                if now.minute == latestMinute:
                    close = tradingSymboldf['close'].iloc[-2]
                    return close
                else:
                    count += 1
                    if count >= 20:
                        logging.info(f'Could not get latest close for trading symbol {trade.tradingSymbol}')
                        return None

        except:
            logging.info(f'Failed to get historical data for trading symbol {trade.tradingSymbol}')
            self.bot.send_message(chat_id=self.chat_id,text=f'Failed to get historical data for trading symbol {trade.tradingSymbol} at {datetime.datetime.now()}')
            return None

    # gets the historical data for the trading symbol. Used to get the rsi value and the max close for the day.
    def get_historical_data(self):

        try:
            count = 0
            while True:
                from_date = datetime.datetime.today() - datetime.timedelta(days=self.lookbackdays)
                to_date = datetime.datetime.today()
                self.final_ce_trading_symbol_token = self.get_instrument_token(self.final_ce_trading_symbol)
                self.final_pe_trading_symbol_token = self.get_instrument_token(self.final_pe_trading_symbol)
                self.df_ce = pd.DataFrame(self.kite.historical_data(
                    self.final_ce_trading_symbol_token, from_date, to_date, self.historical_data_interval))
                self.df_pe = pd.DataFrame(self.kite.historical_data(
                    self.final_pe_trading_symbol_token, from_date, to_date, self.historical_data_interval))
                latestMinute = self.df_ce['date'].iloc[-1].minute
                now = datetime.datetime.now()
                # ensures that the last row of the historical dataframe is the current minute and is different from...
                # ...the last time checked. last time checked comes from signal generation function and records the latest
                # ...time that signal check was done. So ensures the historical dataframe is pulled once per minut
                if now.minute == latestMinute and ((now.time() != self.lastTimeChecked and now.minute != self.lastTimeChecked.minute) or (now.time() != self.lastTimeChecked and now.minute == self.lastTimeChecked.minute and now.hour != self.lastTimeChecked.hour)):
                    logging.info(f'Last 2 entries of call df {self.df_ce.tail(2)}')
                    logging.info(f'Last 2 entries of put df {self.df_pe.tail(2)}')
                    break

                if count == 0:
                    logging.info(f'Stuck in historical while loop. Latest minute is {now.minute}, last row minute is {latestMinute}, last time checked {self.lastTimeChecked}')
                elif count % 5 == 0:
                    self.kite = OMS.jugard_trade_login()
                    logging.info(f'Stuck in historical while loop. Latest minute is {now.minute}, last row minute is {latestMinute}, last time checked {self.lastTimeChecked}')


                count += 1
        except:
            try:
                logging.info(f'Retreiving historical data for ce trading instrument token: {self.final_ce_trading_symbol_token}  and pe trading instrument token: {self.final_pe_trading_symbol_token} failed')
                self.bot.send_message(chat_id=self.chat_id,text=f'Retreiving historical data for ce trading instrument token: {self.final_ce_trading_symbol_token}  and pe trading instrument token: {self.final_pe_trading_symbol_token} failed at {datetime.datetime.now()}')
            except:
                logging.info('Have not generated ce and pe symbol tokens')

    # function gets the rsi and creates column in the historical dataframe. Uses ta-lib library.
    def put_rsi(self):

        self.get_historical_data()
        try:
            self.df_ce['rsi'] = talib.RSI(self.df_ce['close'], self.rsi_timeperiod)
            self.df_pe['rsi'] = talib.RSI(self.df_pe['close'], self.rsi_timeperiod)
            self.df_ce = self.df_ce[self.df_ce['date'].apply(lambda x: x.date()) == datetime.datetime.now().date()]
            self.df_pe = self.df_pe[self.df_pe['date'].apply(lambda x: x.date()) == datetime.datetime.now().date()]
        except:
            logging.info('Have not generated ce and pe symbol tokens and hence no historical data')
            self.bot.send_message(chat_id=self.chat_id,text=f'Have not generated ce and pe symbol tokens and hence no historical data at {datetime.datetime.now()}')

    # function where entry signal flag is checked
    def signal_generation(self):

        self.put_rsi()
        try:
            logging.info('Finished rsi operation for both ce and pe dfs.')
            logging.info(f'CE symbol rsi is {self.df_ce["rsi"].iloc[-2]}')
            logging.info(f'PE symbol rsi is {self.df_pe["rsi"].iloc[-2]}')
            logging.info(f'CE symbol max close is {self.df_ce["close"].iloc[:-1].max()}')
            logging.info(f'PE symbol max close is {self.df_pe["close"].iloc[:-1].max()}')
            logging.info(f'CE symbol last close is {self.df_ce["close"].iloc[-2]}')
            logging.info(f'PE symbol last close is {self.df_pe["close"].iloc[-2]}')
        except:
            logging.info('Finished rsi operation for both ce and pe dfs, but no historical data found. Error!')
            self.bot.send_message(chat_id=self.chat_id,text=f'Finished rsi operation for both ce and pe dfs, but no historical data found. Error! at {datetime.datetime.now()}')

        try:
            # check if rsi > 60 and close is equal to the highest close of the day for call.
            if (self.df_ce['rsi'].iloc[-2] >= 60) and (self.df_ce['close'].iloc[-2] == self.df_ce['close'].iloc[:-1].max()):

                self.ce_entry_signal = 1
                self.signalEntryPrice = self.df_ce['close'].iloc[-2]

                logging.info('Buy CE option...')

            # same for put option
            elif (self.df_pe['rsi'].iloc[-2] >= 60) and (self.df_pe['close'].iloc[-2] == self.df_pe['close'].iloc[:-1].max()):

                self.pe_entry_signal = 1
                self.signalEntryPrice = self.df_pe['close'].iloc[-2]

                logging.info('Buy PE option...')

        except:
            logging.info(f'Error while generating signal!')
            self.bot.send_message(chat_id=self.chat_id,text=f'Error while generating signal! at {datetime.datetime.now()}')

    # function that runs the signal check module in a thread
    def check_signal(self):

        logging.info("Starting signal check.")

        # gets the current datetime
        now = datetime.datetime.now()

        # checks if the current datetime is after market stop time. If it is, market's closed and returns from function.
        if now > self.marketstoptime:
            logging.info('Market has closed for the day.')
            return

        # waits till market opens at 9:15
        self.waitTillMarketOpens("check_signal")
        self.bot.send_message(chat_id=self.chat_id,text=f'Starting signal check at {datetime.datetime.now()}')

        now = datetime.datetime.now()
        prev_minute_signal = now.minute
        prev_minute_trading_symbol = now.minute
        prev_minute_final_trading_symbol = now.minute
        prev_minute_stoploss = now.minute
        signal_check = 0
        trading_symbol_check = 0
        final_trading_symbol_check = 0
        stoploss_check = 0
        relogincount = 0
        slrelogincount = 0
        self.lastTimeChecked = now.time()

        # starts the while loop that runs through the day
        while True:

            # checks whether morning session
            now = datetime.datetime.now()
            if (now >= self.morningstarttime and now <= self.morningstoptime):
                morningSession = 1
            else:
                morningSession = 0

            #check whether afternoon session
            if (now >= self.afternoonstarttime and now <= self.afternoonstoptime):
                afternoonSession = 1
            else:
                afternoonSession = 0

            # checks whether market has ended again.
            if now > self.marketstoptime:
                logging.info('Signal check module shutting down as market has closed for the day.')
                break

            try:
                # every minute the trading_symbol_check flag set to 0
                if now.minute != prev_minute_trading_symbol:
                    trading_symbol_check = 0

                # when trading_symbol_check_flag is 0, get the trading symbol for the minute.
                if morningSession == 1 and self.morning_trade < self.maxmorningtrades:

                    if trading_symbol_check == 0:
                        trading_symbol_check = 1
                        prev_minute_trading_symbol = now.minute
                        self.get_trading_symbol()

                if afternoonSession == 1 and self.afternoon_trade < self.maxafternoontrades:

                    if trading_symbol_check == 0:
                        trading_symbol_check = 1
                        prev_minute_trading_symbol = now.minute
                        self.get_trading_symbol()

                if now.minute != prev_minute_final_trading_symbol:
                    final_trading_symbol_check = 0

                # when there are 3 seconds left to next minute, set the final trading symbol.
                if morningSession == 1 and self.morning_trade < self.maxmorningtrades:
                    if datetime.datetime.now().second > 57 and final_trading_symbol_check == 0:
                        self.lastTimeChecked = datetime.datetime.now().time()
                        final_trading_symbol_check = 1
                        prev_minute_final_trading_symbol = now.minute
                        self.check_trading_symbol()
                        logging.info(f'Going to check for entry signal in both trading symbols for the minute {now.minute}.')
                        self.signal_generation()

                if afternoonSession == 1 and self.afternoon_trade < self.maxafternoontrades:
                    if datetime.datetime.now().second > 57 and final_trading_symbol_check == 0:
                        self.lastTimeChecked = datetime.datetime.now().time()
                        final_trading_symbol_check = 1
                        prev_minute_final_trading_symbol = now.minute
                        self.check_trading_symbol()
                        logging.info(f'Going to check for entry signal in both trading symbols for the minute {now.minute}.')
                        self.signal_generation()

                # Check if entry signal generated.
                if now > self.morningtradestarttime and now < self.morningstoptime:

                    # check if entry signal for call is 1 and if morning trade counter is less than the max morning trades.
                    if self.ce_entry_signal == 1 and self.morning_trade < self.maxmorningtrades:
                        logging.info(f'morning session call signal generated for trading symbol {self.final_ce_trading_symbol}')

                        self.bot.send_message(chat_id=self.chat_id,text=f'Signal generated for CE trading symbol {self.final_ce_trading_symbol} at {datetime.datetime.now()}')

                        ltp = self.get_ltp(self.final_ce_trading_symbol)
                        logging.info(f'ltp for trading symbol {self.final_ce_trading_symbol} is {ltp}')

                        # if entry signal generated, then get the trading symbol for fyres.
                        self.get_execution_trading_symbol(option_type='CE')

                        # call the function that generates the trade object.
                        self.generateTrade(trading_symbol=self.final_ce_trading_symbol,
                                            price=ltp, option_type='CE')
                        self.ce_entry_signal = 0
                        self.morning_trade += 1 # set the morning trade counter to 1.

                    # repeat same process of call for put.
                    elif self.pe_entry_signal == 1 and self.morning_trade < self.maxmorningtrades:
                        logging.info(f'morning session put signal generated for trading symbol {self.final_pe_trading_symbol}')

                        self.bot.send_message(chat_id=self.chat_id,text=f'Signal generated for PE trading symbol {self.final_pe_trading_symbol} at {datetime.datetime.now()}')

                        ltp = self.get_ltp(self.final_pe_trading_symbol)
                        logging.info(f'ltp for trading symbol {self.final_pe_trading_symbol} is {ltp}')
                        self.get_execution_trading_symbol(option_type='PE')
                        self.generateTrade(trading_symbol=self.final_pe_trading_symbol,
                                            price=ltp, option_type='PE')
                        self.pe_entry_signal = 0
                        self.morning_trade += 1

                # do the same for the afternoon session
                elif now > self.afternoontradestarttime and now < self.afternoonstoptime:

                    if self.ce_entry_signal == 1 and self.afternoon_trade < self.maxafternoontrades:
                        logging.info(f'afternoon session call signal generated for trading symbol {self.final_ce_trading_symbol}')

                        self.bot.send_message(chat_id=self.chat_id,text=f'Signal generated for CE trading symbol {self.final_ce_trading_symbol} at {datetime.datetime.now()}')

                        ltp = self.get_ltp(self.final_ce_trading_symbol)
                        logging.info(f'ltp for trading symbol {self.final_ce_trading_symbol} is {ltp}')
                        self.get_execution_trading_symbol(option_type='CE')
                        self.generateTrade(trading_symbol=self.final_ce_trading_symbol,
                                            price=ltp, option_type='CE')
                        self.ce_entry_signal = 0
                        self.afternoon_trade += 1

                    elif self.pe_entry_signal == 1 and self.afternoon_trade < self.maxafternoontrades:
                        logging.info(f'afternoon session put signal generated for trading symbol {self.final_pe_trading_symbol}')

                        self.bot.send_message(chat_id=self.chat_id,text=f'Signal generated for PE trading symbol {self.final_pe_trading_symbol} at {datetime.datetime.now()}')

                        ltp = self.get_ltp(self.final_pe_trading_symbol)
                        logging.info(f'ltp for trading symbol {self.final_pe_trading_symbol} is {ltp}')
                        self.get_execution_trading_symbol(option_type='PE')
                        self.generateTrade(trading_symbol=self.final_pe_trading_symbol,
                                            price=ltp, option_type='PE')
                        self.pe_entry_signal = 0
                        self.afternoon_trade += 1

                else:
                    self.ce_entry_signal = 0
                    self.pe_entry_signal = 0

            except Exception as e:
                logging.info(f'Error while checking for entry signal {str(e)}')
                try:
                    self.re_login_jugard_trade()
                    logging.info('Logged in to jugaad trader again!')
                except:
                    if relogincount % 100000:
                        logging.info('Could not log in to jugaad trader!')
                    relogincount += 1
                    continue

            # this part updates the stop loss every minute if a trade is generated.
            try:
                if now.minute != prev_minute_stoploss:
                    stoploss_check = 0

                if stoploss_check == 0:
                    stoploss_check = 1
                    prev_minute_stoploss = now.minute
                    logging.info(f'Going to update stoploss for the minute {now.minute}.')
                    self.update_trailing_stoploss()

            except Exception as e:
                logging.info(f'Error while updating stoploss {str(e)}')
                try:
                    self.re_login_jugard_trade()
                    logging.info('Logged in to jugaad trader again!')
                except:
                    if slrelogincount % 100000:
                        logging.info('Could not log in to jugaad trader!')
                    slrelogincount += 1
                    continue

    # function to get the initial stop loss.
    def get_initial_stoploss(self, trade):

        # initial stop loss calculated on the average entry price after order execution.
        trade.initialStopLoss = trade.entry_average_price * \
            (1 - (self.initial_stoploss_pct / 100))

        trade.initialStopLoss = round(self.base * round(trade.initialStopLoss/self.base), 2)
        # set trailing stop to initial stop loss in the beginning.
        trade.stopLoss = trade.initialStopLoss
        logging.info(f'Set initial SL to {trade.initialStopLoss}')

    # function to update the trailing stop loss every minute.
    def update_trailing_stoploss(self):

        # if no trade generated yet, function returns from here.
        if not self.trades:
            logging.info('No trade found.')
            return

        # for all the trades in the trades list, update the trailing stop loss if the trade is still active
        no_active_trade = 0
        for trade in self.trades:
            # checks if the tradestate is active and whether the order is filly executed.
            if trade.tradeState == TradeState.ACTIVE and trade.filledQty == trade.qty:

                # if initial stop loss is not yet set, then call the function to get the initial stop loss.
                if trade.initialStopLoss == None:
                    logging.info('Setting initial stoploss.')
                    self.get_initial_stoploss(trade)
                    return

                # if the ltp of the trading symbol is greater than the stop loss then get the latest close of the trading symbol...
                # ...then get the % distance away from the entry average price and this is the trail stop loss %
                # if the trail stop loss determined is greater than the current stop loss, then update the stop loss.
                if self.get_ltp(trade.tradingSymbol) > trade.stopLoss:
                    close = self.get_trading_symbol_historical_data_close(trade)
                    if close != None:
                        trail_stoploss_pct = ((close -
                                               trade.entry_average_price) / trade.entry_average_price) * 100
                        next_trailing_stoploss_price = trade.initialStopLoss * \
                            (1 + trail_stoploss_pct/200)

                        if next_trailing_stoploss_price > trade.stopLoss:
                            oldSL = trade.stopLoss
                            trade.stopLoss = next_trailing_stoploss_price
                            trade.stopLoss = round(self.base * round(trade.stopLoss/self.base), 2)
                            logging.info(f'Updating trailing stoploss from {oldSL} to {next_trailing_stoploss_price}.')
                            self.bot.send_message(chat_id=self.chat_id,text=f'Updating trailing stoploss from {oldSL} to {next_trailing_stoploss_price} at {datetime.datetime.now()}')
                        else:
                            logging.info(f'Stoploss stays same at {trade.stopLoss}.')

                    else:
                        logging.info(f'Could not update trailing stoploss for trading symbol {trade.tradingSymbol}.')

            else:
                no_active_trade += 1

        if no_active_trade == len(self.trades):
            logging.info('No active trades.')
    # function to generate a trade object
    def generateTrade(self, trading_symbol=None, price=None, option_type=None):

        logging.info(f'Generating trade for trading symbol {trading_symbol}')
        trade = Trade(trading_symbol) # trade object generated when entry criteria fulfilled.
        trade.executiontradingSymbol = self.executiontradingSymbol # sets the fyres trading symbol
        trade.optionType = option_type # sets the option type
        trade.productType = ProductType.INTRADAY # sets the product type for fyres.
        trade.requestedEntry = price # sets the ltp at the time of signal generation
        trade.signalEntryPrice = self.signalEntryPrice
        trade.qty = self.tradingLots # sets the number of lots to be traded for the current trade. This is determined in the trade manager function.
        trade.tradeState = TradeState.CREATED # sets the tradestate to created in the beginning.
        trade.intradaySquareOffTimestamp = Utils.getEpoch(self.intradaySquareOffTimestamp) # sets the intraday square off time.
        trade.direction = Direction.LONG # sets whether trade is buy or sell.

        # add symbol to created trades list
        self.trades.append(trade)
        logging.info(f'Trade created with parameters {trade}')

    # function to get the average price from the fyres positon book for a particular trading symbol
    def get_average_price_buy(self, trade=None):
        count = 0
        while True:

            try:
                position_dict = self.fyres_obj.get_open_positions()

                for position in position_dict:
                    if position['symbol'] == trade.executiontradingSymbol:
                        average_price = position['buyAvg']
                        return average_price

                return None

            except:
                logging.info('failed to get position for count: %s', str(count))
                self.fyres_login()
                count += 1
                if count == 2:
                    logging.info('failed to get position for count: %s', str(count))
                    logging.info('returning None average price')
                    return None

    def get_average_price_sell(self, trade=None):
        count = 0
        while True:

            try:
                position_dict = self.fyres_obj.get_open_positions()

                for position in position_dict:
                    if position['symbol'] == trade.executiontradingSymbol:
                        average_price = position['sellAvg']
                        return average_price

                return None

            except:
                logging.info('failed to get position for count: %s', str(count))
                self.fyres_login()
                count += 1
                if count == 2:
                    logging.info('failed to get position for count: %s', str(count))
                    logging.info('returning None average price')
                    return None

    # trade manager function handles the order placement and tracking and is the second thread of the algo
    def trade_manager(self):

        logging.info("Starting trade manager.")

        now = datetime.datetime.now()
        if now > self.marketstoptime:
            logging.info('Trademanager shutting down as market has closed for the day.')
            return

        # this function sets the quantity to be traded for the day.
        self.get_trading_lots()

        logging.info(f'Quantity to trade for the day {self.tradingLots}')
        # if quantity to trade for the day is 0, then does not take any trade
        if self.tradingLots == 0:
            logging.info('Not enough capital to trade the system.')
            return

        # Waits till market is open
        self.waitTillMarketOpens("TradeManager")

        self.bot.send_message(chat_id=self.chat_id,text=f'Starting trade manager at {datetime.datetime.now()}')

        tradeCount = 0
        entryCount = 0
        tradeEntryCount = 0
        SLPlaceCount = 0
        SLCheckCount = 0
        SLCheckloggingcount = 0
        systemUpdateCount = 0

        # starts the while loop that runs till market close.
        while True:

            now = datetime.datetime.now()

            # Exit when market ends. Do a daily pnl update
            if now > self.marketstoptime:
                logging.info('Market has closed for the day.')
                self.do_system_update()
                break

            # if there are no trades generated then go the next iteration.
            if not self.trades:
                if tradeCount % 10000000 == 0:
                    logging.info(f'No trade found in the last {tradeCount} runs.')
                tradeCount += 1
                continue

            # once trade is generated, check if the status is created.
            active_trades = 0
            for trade in self.trades:
                if trade.tradeState == TradeState.ACTIVE and len(trade.slOrder) > 0:
                    active_trades += 1

            active_trades_check = 0
            for trade in self.trades:

                if trade.tradeState == TradeState.CREATED and len(trade.entryOrder) == 0:
                    if entryCount % 1000000 == 0:
                        logging.info(f'Going to place entry order')
                        try:
                            # if status is created place entry order. function returns true only if order fully executed.
                            issuccess = self.place_entry_order(trade)

                            # if entry order successful and fully executed, set all the relevant parameters.
                            if issuccess:
                                trade.tradeState = TradeState.ACTIVE # tradestate set to active.
                                trade.startTimestamp = Utils.getEpoch() # trade start time stamp is set.
                                trade.entry_average_price = self.get_average_price_buy(trade) # entry average price retreived from positon book
                                trade.filledQty = trade.qty # filled quantity set as requested quantity.
                                logging.info(f'Executed trade with parameters {trade}')
                                # self.bot.send_message(chat_id=self.chat_id,text=f'Executed trade with parameters {trade} at {datetime.datetime.now()}')

                            else:
                                trade.tradeState = TradeState.CANCELLED # Order has been rejected, so check the issue and re run the program.
                                logging.info(f'Trade with parameters {trade} has been cancelled because an order has been rejected')

                        except Exception as e:

                            logging.info(f'Error while placing entry order: {str(e)}')
                            logging.info('Creating fyers object again')
                            try:
                                self.fyres_login()
                            except:
                                logging.info('Could not create Fyers object again')
                            logging.info('Logging into jugaad trader again')
                            try:
                                self.re_login_jugard_trade()
                                logging.info('Logged in to jugaad trader again!')
                            except Exception as e:
                                logging.info('Could not log in to jugaad trader!')

                    entryCount += 1

                elif trade.tradeState == TradeState.CREATED and len(trade.entryOrder != 0):
                    if tradeEntryCount % 1000000 == 0:
                        logging.info('Entry order submitted but tradestate not updated')
                        # Checks whether quantity is filled.

                        try:
                            orderBook = self.fyres_obj.get_orderbook()

                            # for each order, locates the relevant order and checks if quantity filled.
                            for bOrder in orderBook:
                                for order in trade.entryOrder:
                                    if order.orderId == bOrder['id']:
                                        order.filledQty = bOrder['filledQty']
                                        order.pendingQty = bOrder['remainingQuantity']
                                        order.orderStatus = bOrder['status']

                                        # if filled, then filledQtyOrders counter increaed by 1.
                                        if order.filledQty == order.qty:
                                            trade.tradeState = TradeState.ACTIVE # tradestate set to active.
                                            trade.startTimestamp = Utils.getEpoch() # trade start time stamp is set.
                                            trade.entry_average_price = self.get_average_price_buy(trade) # entry average price retreived from positon book
                                            trade.filledQty = trade.qty # filled quantity set as requested quantity.
                                            logging.info(f'Executed trade with parameters {trade}')
                                            logging.info(f'order with orderID {order.orderId} completely filled')

                                        else:
                                            # if not filled, then checks if rejected and returns false if rejected.
                                            if order.orderStatus == 5:
                                                logging.info(f'order with orderID {order.orderId} rejected')
                                                trade.tradeState = TradeState.CANCELLED # Order has been rejected, so check the issue and re run the program.
                                                logging.info(f'Trade with parameters {trade} has been cancelled because an order has been rejected')

                                            else:
                                                logging.info(f'order with orderID {order.orderId} not filled')
                                                logging.info(f'Message received when order submitted: {order.lastMessage}')

                                            # new_market_order_qty = math.ceil(order.pendingQty * 0.1)
                                            # ZerodhaOrderManager.modifyOrderToMarketOrder(order=order, quantity=new_qty)

                        except Exception as e:
                            logging.info(f'Error while updating entry order: {str(e)}')

                    tradeEntryCount += 1


                # if entry order executed and stop loss not set, then set stop loss and place exit order.
                elif trade.tradeState == TradeState.ACTIVE and not trade.slOrder:
                    if SLPlaceCount % 1000000 == 0:
                        logging.info(f'Going to place stoploss order')
                        try:
                            self.place_exit_order(trade)
                        except Exception as e:
                            logging.info(f'Error while placing exit order: {str(e)}')
                            logging.info('Creating fyers object again')
                            try:
                                self.fyres_login()
                            except:
                                logging.info('Could not create Fyers object again')
                            logging.info('Logging into jugaad trader again')
                            try:
                                self.re_login_jugard_trade()
                                logging.info('Logged in to jugaad trader again!')
                            except Exception as e:
                                logging.info('Could not log in to jugaad trader!')

                    SLPlaceCount += 1

                # if trade is active and stop loss order set, then track the order
                elif trade.tradeState == TradeState.ACTIVE and len(trade.slOrder) > 0:
                    active_trades_check += 1
                    # This ensures the stop loss order check runs every 5 seconds.
                    if SLCheckCount % 10000000 == 0:
                        logging.info(f'Going to track stoploss order.')
                        try:
                            slsuccess = self.track_sl_order(trade)

                            # If returns true, then stop loss hit
                            if slsuccess == True:
                                trade.tradeState = TradeState.COMPLETED # tradestate set to complete
                                trade.endTimestamp = Utils.getEpoch() # end time stamp determined
                                trade.exit_average_price = self.get_average_price_sell(trade) # exit average price determined.
                                trade.exitReason = 'SL hit' # exit reason also set

                            # if returns trigger pending, then get the cmp of the symbol and get the unrealized pnl
                            elif slsuccess == 'trigger_pending':
                                trade.cmp = self.get_ltp(trade.tradingSymbol)
                                trade.pnl = trade.cmp - trade.entry_average_price
                                if SLCheckloggingcount % 10000000 == 0:
                                    logging.info(f'SL not triggered in the last {SLCheckloggingcount} runs.')
                                SLCheckloggingcount += 1

                            # if return rejected, then an order has been rejected and needs investigation
                            elif slsuccess == 'rejected':
                                logging.info(f'One of the SL order is rejected. Fix the error.')

                            # if returns squareofftime, then square of time reached.
                            elif slsuccess == 'squareofftime':
                                trade.tradeState = TradeState.COMPLETED
                                trade.endTimestamp = Utils.getEpoch()
                                trade.exit_average_price = self.get_average_price_sell(trade)
                                trade.exitReason = 'Time Square-off'

                        except Exception as e:
                            logging.info(f'Error while tracking trade: {str(e)}')
                            logging.info('Creating fyers object again')
                            try:
                                self.fyres_login()
                            except:
                                logging.info('Could not create Fyers object again')
                            logging.info('Logging into jugaad trader again')
                            try:
                                self.re_login_jugard_trade()
                                logging.info('Logged in to jugaad trader again!')
                            except:
                                logging.info('Could not log in to jugaad trader!')

                    if active_trades_check == active_trades:
                        SLCheckCount += 1

            # current trade data is saved every 30 seconds.
            if systemUpdateCount % 10000000 == 0:
                try:
                    self.do_system_update()
                except Exception as e:
                    logging.info(f'Error while updating system: {str(e)}')
            systemUpdateCount += 1

    # function places entry order.
    def place_entry_order(self, trade):

        logging.info(f'Placing entry order now!')

        # Uses order slicing. If there are 110 lots, then this will place 4 orders of 25 lots and one order of 10 lots.
        # if there are 2 lots, then this will place one order of 2 lots.
        last_qty = trade.qty % self.lots_per_order
        for n in range(0, trade.qty, self.lots_per_order):

            if n >= self.lots_per_order:
                oip = OrderInputParams(trade.executiontradingSymbol) # sets the orderinputparams object attributes to be sent to order management system.
                oip.qty = self.lots_per_order*self.lotsize
                oip.price = self.get_ltp(trade.tradingSymbol)
                oip.direction = Direction.LONG
                oip.orderType = OrderType.LIMIT

            elif n < self.lots_per_order:
                if last_qty > 0:
                    oip = OrderInputParams(trade.executiontradingSymbol)
                    oip.qty = last_qty*self.lotsize
                    oip.price = self.get_ltp(trade.tradingSymbol)
                    oip.direction = Direction.LONG
                    oip.orderType = OrderType.LIMIT

                elif last_qty == 0:
                    oip = OrderInputParams(trade.executiontradingSymbol)
                    oip.qty = self.lots_per_order*self.lotsize
                    oip.price = self.get_ltp(trade.tradingSymbol)
                    oip.direction = Direction.LONG
                    oip.orderType = OrderType.LIMIT

            logging.info(f'Placing entry order with parameters {oip}')
            trade.entryOrder.append(self.fyres_obj.place_order(oip)) # Appends each order in the entry order list in the trade object.

        # Checks whether quantity is filled. Starts a while true loop till quantity filled.
        filledQtyOrders = 0
        while True:

            # checks if filledQtyOrders counter is equal to the entry order list. If yes, then all orders filled.
            if filledQtyOrders == len(trade.entryOrder):
                logging.info('all orders filled')
                return True

            # gets the order book from fyres.
            time.sleep(2)
            orderBook = self.fyres_obj.get_orderbook()

            # for each order, locates the relevant order and checks if quantity filled.
            for bOrder in orderBook:
                for order in trade.entryOrder:
                    if order.orderId == bOrder['id']:
                        order.filledQty = bOrder['filledQty']
                        order.pendingQty = bOrder['remainingQuantity']
                        order.orderStatus = bOrder['status']

                        # if filled, then filledQtyOrders counter increaed by 1.
                        if order.filledQty == order.qty:
                            filledQtyOrders += 1
                            logging.info(f'order with orderID {order.orderId} completely filled')

                        else:
                            # if not filled, then checks if rejected and returns false if rejected.
                            if order.orderStatus == 5:
                                logging.info(f'order with orderID {order.orderId} rejected')
                                return False
                            # new_market_order_qty = math.ceil(order.pendingQty * 0.1)
                            # ZerodhaOrderManager.modifyOrderToMarketOrder(order=order, quantity=new_qty)

                            # gets the ltp of the trading symbol and modifies order to tlatest ltp.
                            ltp = self.get_ltp(trade.tradingSymbol)
                            omp = OrderModifyParams()
                            omp.newPrice = ltp
                            logging.info(f'placing modify order for orderID {order.orderId} with ltp: {ltp}')
                            self.fyres_obj.modifyLimitOrderPrice(order, omp)

    # function to place exit orders.
    def place_exit_order(self, trade):

        # If initial stop loss not yet set, then returns.
        if trade.initialStopLoss == None:
            logging.info(f'Initial stoploss price for trade {trade} not yet determined')
            return

        # follows the same order slicing logic as entry order.
        last_qty = trade.qty % self.lots_per_order
        for n in range(0, trade.qty, self.lots_per_order):

            if n >= self.lots_per_order:
                oip = OrderInputParams(trade.executiontradingSymbol)
                oip.qty = self.lots_per_order*self.lotsize
                oip.price = round((trade.stopLoss - 0.3), 2)
                oip.direction = Direction.SHORT
                oip.triggerPrice = trade.stopLoss
                oip.orderType = OrderType.SL_LIMIT

            elif n < self.lots_per_order:
                if last_qty > 0:
                    oip = OrderInputParams(trade.executiontradingSymbol)
                    oip.qty = last_qty*self.lotsize
                    oip.price = round((trade.stopLoss - 0.3), 2)
                    oip.direction = Direction.SHORT
                    oip.triggerPrice = trade.stopLoss
                    oip.orderType = OrderType.SL_LIMIT

                elif last_qty == 0:
                    oip = OrderInputParams(trade.executiontradingSymbol)
                    oip.qty = self.lots_per_order*self.lotsize
                    oip.price = round((trade.stopLoss - 0.3), 2)
                    oip.direction = Direction.SHORT
                    oip.triggerPrice = trade.stopLoss
                    oip.orderType = OrderType.SL_LIMIT

            logging.info(f'Placing SL order with parameters {oip}')
            trade.slOrder.append(self.fyres_obj.place_order(oip)) # appends sl orders to slorder attribute of trade object

    # tracks the sl order status.
    def track_sl_order(self, trade):

        now = datetime.datetime.now()
        # if square off time reached, then get the order book and modify sl orders to market.
        if now >= self.intradaySquareOffTimestamp:
            orderBook = self.fyres_obj.get_orderbook()

            for bOrder in orderBook:
                for sl_order in trade.slOrder:
                    if sl_order.orderId == bOrder['id']:
                        self.fyres_obj.modifyOrderToMarket(sl_order)
            filledQtyOrders = 0
            # After modifying checks if all orders filled.
            while True:
                if filledQtyOrders == len(trade.slOrder):
                    logging.info(f'Intraday square off for trade {trade}')

                    self.bot.send_message(chat_id=self.chat_id,text=f'Intraday square off for trade {trade} at {datetime.datetime.now()}')

                    return 'squareofftime'

                orderBook = self.fyres_obj.get_orderbook()
                for bOrder in orderBook:
                    for sl_order in trade.slOrder:
                        if sl_order.orderId == bOrder['id']:
                            sl_order.filledQty = bOrder['filledQty']
                            sl_order.pendingQty = bOrder['remainingQuantity']
                            sl_order.orderStatus = bOrder['status']
                            if sl_order.filledQty == sl_order.qty:
                                filledQtyOrders += 1

        # gets the current low of the trading symbol
        low = self.get_trading_symbol_historical_data_low(trade)
        trigger_pending = 0
        filledQtyOrders = 0
        while True:

            # checks if trigger pending counter matches the length of the sl order. if true, return 'trigger_pending'
            if trigger_pending == len(trade.slOrder):
                logging.info(f'Stop loss orders for trade {trade} not yet triggered')
                return 'trigger_pending'
            # checks if slorder is filled
            if filledQtyOrders == len(trade.slOrder):
                logging.info(f'Stop loss orders for trade {trade} have been filled')
                self.bot.send_message(chat_id=self.chat_id,text=f'Stop loss orders for trade {trade} have been filled at {datetime.datetime.now()}')
                return True

            orderBook = self.fyres_obj.get_orderbook()
            for bOrder in orderBook:
                for sl_order in trade.slOrder:
                    if sl_order.orderId == bOrder['id']:
                        sl_order.filledQty = bOrder['filledQty']
                        sl_order.pendingQty = bOrder['remainingQuantity']
                        sl_order.orderStatus = bOrder['status']

                        #checking if order is filled
                        if sl_order.filledQty == sl_order.qty:
                            filledQtyOrders += 1

                        # checking if trigger pending/open
                        elif sl_order.orderStatus == 6:

                            # if stoploss hit, and order is open, then need to modify order to get out of trade.
                            if sl_order.filledQty < sl_order.qty and low <= trade.stopLoss:
                                logging.info(f'Order with orderID {sl_order.orderId} triggered, but not filled.')
                                ltp = self.get_ltp(trade.tradingSymbol)
                                omp = OrderModifyParams()
                                omp.newPrice = ltp
                                logging.info(f'Placing modify order for orderID {sl_order.orderId} with ltp: {ltp}')
                                self.fyres_obj.modifyLimitOrderPrice(sl_order, omp)
                            else:
                                trigger_pending += 1
                                # checks if current stop loss is greater than the trigger price. If it is, then modify the order.
                                if trade.stopLoss > sl_order.triggerPrice:
                                    omp = OrderModifyParams()
                                    omp.newTriggerPrice = trade.stopLoss
                                    omp.newPrice = round((trade.stopLoss - 0.3), 2)
                                    try:
                                        self.fyres_obj.modifySLLimitOrderPrice(sl_order, omp)
                                        logging.info(f'TradeManager: Trail SL: Successfully modified stopLoss to {trade.stopLoss} for tradeID {trade.tradeID}')
                                        sl_order.triggerPrice = omp.newTriggerPrice
                                        sl_order.price = omp.newPrice
                                    except Exception as e:
                                        logging.error(f'TradeManager: Failed to modify SL order for tradeID {trade.tradeID} Error => {str(e)}')
                        # check if the order is rejected.
                        elif sl_order.orderStatus == 5:
                            logging.info(f'SL order with orderID {sl_order.orderId} rejected.')
                            self.bot.send_message(chat_id=self.chat_id,text=f'SL order with orderID {sl_order.orderId} rejected at {datetime.datetime.now()}')
                            return 'rejected'

    # start algo function that begins the two threads.
    def start_algo(self):

        if self.isHoliday():
            print("Cannot start algo as Today is Trading Holiday.")
            self.bot.send_message(chat_id=self.chat_id,text='Cannot start algo as Today is Trading Holiday')
            return

        logging.info("Starting Algo...")

        tm = threading.Thread(target=self.trade_manager)
        tm.start()

        # start running strategies: Run each strategy in a separate thread

        cs = threading.Thread(target=self.check_signal)
        cs.start()

        logging.info("Algo started.")

    #function to get the trading lots for the day.
    def get_trading_lots(self):

        pnlFileDir = '/home/prtkrock/final_Code/BOS/PNL_FILE_DIRECTORIES/'

        if os.path.exists(pnlFileDir) == False:
            print("Pnl File Directory " + pnlFileDir + " does not exist. Making the directory.")
            os.makedirs(pnlFileDir)

        filepath = pnlFileDir + "bos_trades.csv"
        # columns_names: ['datetime', 'todaysPnL', 'pnlUptoDate']

        # opens the csv file that has the above columns. todays pnl consists of the days pnl and the pnluptodate is the total pnl generated by the system till date.
        try:
            df_system_update = pd.read_csv(filepath)
        except:
            df_system_update = pd.DataFrame(columns=[['tradeId', 'tradingSymbol', 'quantity', 'entryDatetime', 'entryPrice', 'initialStoploss', 'lastTrailingStoploss', 'exitDatetime', 'exitPrice', 'pnl', 'capital', 'pnlPoints', 'cumulativePnlPoints']])
            df_system_update.to_csv(pnlFileDir + "bos_trades.csv", index=False)

        # if the df_pnl is empty, then trading lots based on initial capital
        if df_system_update.empty:
            self.tradingLots = int(self.initial_capital / self.systemCapitalPerLot)

        # if not, then get the trading lots based on initial capital and the system generated pnl.
        else:
            capital = df_system_update['capital'].iloc[-1]
            initialTradingLots = int(self.initial_capital / self.systemCapitalPerLot)
            self.tradingLots = initialTradingLots + int(df_system_update['cumulativePnlPoints'].iloc[-1] / self.systemCompoundingPerLot)

            if int(self.tradingLots) < 1:
                if capital >= self.systemCapitalPerLot:
                    self.tradingLots = 1
                else:
                    self.tradingLots = 0

        self.tradingLots = int(self.tradingLots)

    # stores a trades.json file and a csv file to keep track of the days trades.
    def do_system_update(self):

        pnlFileDir = '/home/prtkrock/final_Code/BOS/PNL_FILE_DIRECTORIES/'

        if os.path.exists(pnlFileDir) == False:
            print("LogFile Directory " + pnlFileDir +
                  " does not exist. Making the directory.")
            os.makedirs(pnlFileDir)
            df_system_update = pd.DataFrame(columns=[['tradeId', 'tradingSymbol', 'quantity', 'entryDatetime', 'entryPrice', 'initialStoploss', 'lastTrailingStoploss', 'exitDatetime', 'exitPrice', 'pnl', 'capital', 'pnlPoints', 'cumulativePnlPoints']])
            df_system_update.to_csv(pnlFileDir + "bos_trades.csv", index=False)

        filepath = pnlFileDir + "bos_trades.csv"

        try:
            df_system_update = pd.read_csv(filepath)
        except:
            df_system_update = pd.DataFrame(columns=[['tradeId', 'tradingSymbol', 'quantity', 'entryDatetime', 'entryPrice', 'initialStoploss', 'lastTrailingStoploss', 'exitDatetime', 'exitPrice', 'pnl', 'capital', 'pnlPoints', 'cumulativePnlPoints']])
            df_system_update.to_csv(filepath, index=False)

        for trade in self.trades:
            try:
                entryDatetime = Utils.convertEpochToDateObj(trade.startTimestamp)
            except:
                entryDatetime = trade.startTimestamp
            try:
                exitDatetime = Utils.convertEpochToDateObj(trade.endTimestamp)
            except:
                exitDatetime = trade.endTimestamp
            tradingSymbol = trade.tradingSymbol
            initialStoploss = trade.initialStopLoss
            lastTrailingStoploss = trade.stopLoss
            entryPrice = trade.entry_average_price
            exitPrice = trade.exit_average_price
            quantity = trade.qty
            tradeId = trade.tradeID

            if trade.tradeState == TradeState.COMPLETED:
                if tradeId not in list(df_system_update['tradeId']):
                    pnlPoints = exitPrice - entryPrice
                    pnl = pnlPoints * self.tradingLots * self.lotsize
                    if df_system_update.empty:
                        capital = self.initial_capital + pnl
                        cumPnlPoints = pnlPoints
                    else:
                        capital = df_system_update['capital'].iloc[-1] + pnl
                        cumPnlPoints = df_system_update['cumulativePnlPoints'].iloc[-1] + pnlPoints

                    df_temp = pd.DataFrame({'tradeId': [tradeId], 'tradingSymbol': [tradingSymbol], 'quantity': [quantity], 'entryDatetime': [entryDatetime], 'entryPrice': [entryPrice], 'initialStoploss': [initialStoploss], 'lastTrailingStoploss': [lastTrailingStoploss], 'exitDatetime': [exitDatetime], 'exitPrice': [exitPrice], 'pnl': [pnl], 'capital': [capital], 'pnlPoints': [pnlPoints], 'cumulativePnlPoints': cumPnlPoints})
                    df_system_update = df_system_update.append(df_temp, ignore_index=True)
                    df_system_update.reset_index(drop = True, inplace = True)
                    df_system_update.to_csv(filepath, index=False)
                    logging.info('TradeManager: Updated new completed trade to bos_trades.csv')

        tradesFileDir = os.path.join(pnlFileDir, Utils.getTodayDateStr())
        if os.path.exists(tradesFileDir) == False:
          logging.info('TradeManager: Intraday Trades Directory %s does not exist. Hence going to create.', tradesFileDir)
          os.makedirs(tradesFileDir)
        tradesFilepath = os.path.join(tradesFileDir, 'trades.json')
        with open(tradesFilepath, 'w') as tFile:
            json.dump(self.trades, tFile, indent=2, cls=TradeEncoder)
        logging.info('TradeManager: Saved %d trade to file %s',
                     len(self.trades), tradesFilepath)



    # function to wait till market opens.
    def waitTillMarketOpens(self, context):

        now = datetime.datetime.now()
        hour = self.marketStartTime.hour
        minute = self.marketStartTime.minute
        marketstarttime = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        waitSeconds = (marketstarttime - datetime.datetime.now()).seconds
        waitDays = (marketstarttime - datetime.datetime.now()).days
        if waitSeconds > 0 and waitDays >= 0:
            logging.info("%s: Waiting for %d seconds till market opens...", context, waitSeconds)
            time.sleep(waitSeconds)

    # holiday list is stores in a json file that is checked everyday.
    def getHolidays(self):

        json_file_path = '/home/prtkrock/final_Code/BOS/config_holidays.json'
        with open(json_file_path, 'r') as holidays:
            holidaysData = json.load(holidays)
            return holidaysData

    # Checks if the day is a holiday.
    def isHoliday(self):

        datetimeObj = datetime.datetime.now()
        dayOfWeek = calendar.day_name[datetimeObj.weekday()]
        # if dayOfWeek == 'Saturday' or dayOfWeek == 'Sunday':
        #     return True
        dateStr = datetimeObj.strftime("%Y-%m-%d")
        holidays = self.getHolidays()
        if (dateStr in holidays):
            return True
        else:
            return False


'''

1. Create an object of the BOS execution class.
2. Login to jugaad trader for data.
3. Login to fyres for order execution
4. Start the algo.

'''
bos_object = BOS(bot,chat_id)
bos_object.login_jugard_trade()
bos_object.fyres_login()

bos_object.start_algo()

'''
One trade object will be created for entry signal. trade object will be updated with multiple orders.


# In start_algo function, begin two threads. One is the signal generation and stoploss updation thread and other is...
# ...order placement and tracking thread.

# In the signal generation thread, run a while loop for an active trading day where it will search for signal in the...
# ...morning and afternoon phase every minute for a call and put trading symbol whose ltp is above 80.

# When entry signal is true, generate a trade object for the trading symbol and given price level and append it to a trades list.
# Quantity to be traded is dependent on the initial capital and the current profit generated by the system.

# In the trade manager thread, a while loop is started where the trades list is constantly scanned for trade objects.

# Once trade is found, it places entry order on fyres.

# Once entry order is filled, it will place a stop loss order.

# Then it tracks the stoploss order and modifies the stoploss order when trailing stoploss changes.

# It saves the trades in a json and dataframe

# It waits till either trailing stoploss is hit or square off time is reached.

# Once the market ends, the trademanager and the signal check module shuts down.

'''
int(-100/100)
