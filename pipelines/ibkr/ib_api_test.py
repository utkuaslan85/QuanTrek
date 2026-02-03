from ibapi.client import *
from ibapi.wrapper import *
import time 
import threading
from conf import gateway_ip, paper_port
from ibapi.ticktype import TickTypeEnum
import datetime
from ibapi.tag_value import *


class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
    
    def nextValidId(self, orderId: OrderId):
        # self.orderId = orderId
        # self.reqScannerParameters()
        sub = ScannerSubscription()
        sub.instrument = "STK"
        sub.locationCode = "STK.US.MAJOR"
        
        
        # mycontract = Contract()
        # mycontract.symbol = "AAPL"
        # mycontract.secType = "STK"
        # mycontract.currency = "USD"
        # mycontract.exchange = "SMART"
        # mycontract.primaryExchange = "NASDAQ"

        # self.reqContractDetails(orderId, mycontract)
        
    def scannerParameters(self, xml):
        with open("scanner.xml", "w") as f:
            f.write(xml)
        print("Scanner parameters received.")
            
            
    # def nextId(self):
    #     self.orderId += 1
    #     return self.orderId

    # def currentTime(self, time: int):
    #     print(time)

    # def error(self, reqId, errorTime, errorCode, errorString, advancedOrderReject):
    #     print(f"""reqId: {reqId}, errorTime: {errorTime}, errorCode: {errorCode},
    #           errorString: {errorString}, advancedOrderReject: {advancedOrderReject}""")
    
    # def contractDetails(self, reqId: int, contractDetails: ContractDetails):
    #     # attrs = vars(contractDetails)
    #     # # print("\n".join(f"{name}:{value}" for name,value in attrs.items()))
    #     # print(contractDetails.contract)
    #     print(contractDetails.contract)
        
    #     myorder = Order()
    #     myorder.orderId = reqId
    #     myorder.action = "SELL"
    #     myorder.tif = "GTC"
    #     myorder.orderType = "LMT"
    #     myorder.lmtPrice = 283.10
    #     myorder.totalQuantity = 10
        
        # self.placeOrder(orderId=myorder.orderId, contract=contractDetails.contract, order=myorder)
        # self.cancelOrder(orderId=myorder.orderId, orderCancel=OrderCancel())
        
    # def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
    #     print(f"openOrder. orderId: {orderId}, contract: {contract}, orderState: {orderState}")
    
    # def orderStatus(self, orderId: OrderId, status: str, filled:Decimal, remaining: Decimal, 
    #                 avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, 
    #                 clientId: int, whyHeld: str, mktCapPrice:float):
    #     print(f"""orderStatus. orderId: {orderId}, status: {status}, filled: {filled}, 
    #           remaining: {remaining}, avgFillPrice: {avgFillPrice}, permId: {permId}
    #           parentId: {parentId}, lastFillPrice: {lastFillPrice}, clientId: {clientId}
    #           whyHeld: {whyHeld}, mktCapPrice: {mktCapPrice}""")
    
    # def execDetails(self, reqId: int, contract: Contract, execution: Execution):
    #     print(f"execDetails. reqId: {reqId}, contract: {contract}, execution: {execution}")
    
    # def tickPrice(self, reqId, tickType, price, attrib):
    #     print(f"reqId: {reqId}, tickType: {TickTypeEnum.toStr(tickType)}, price: {price}, attrib: {attrib}")
    
    # def tickSize(self, reqId, tickType, size):
    #     print(f"reqId: {reqId}, tickType: {TickTypeEnum.toStr(tickType)}, size: {size}")
    
    # def headTimestamp(self, reqId, headTimestamp):
    #     print(headTimestamp)
    #     print(datetime.datetime.fromtimestamp(int(headTimestamp)))
    #     self.cancelHeadTimeStamp(reqId)
    
    # def historicalData(self, reqId, bar):
    #     print(reqId, bar)
    
    # def historicalDataEnd(self, reqId, start, end):
    #     print(f"Historical data ended for reqId: {reqId}. Started at {start}, ending at {end}")
    
    # def contractDetailsEnd(self, reqId):
    #     print("End of contract details")
    #     self.disconnect()
    
app = TestApp()
app.connect(gateway_ip, paper_port, 0)
# threading.Thread(target=app.run).start()
# time.sleep(1)
app.run()

# for i in range (0,5):
#     print(app.nextId())
#     app.reqCurrentTime()

# mycontract = Contract()

# # Stock
# mycontract.symbol = "AAPL"
# mycontract.secType = "STK"
# mycontract.currency = "USD"
# mycontract.exchange = "SMART"
# mycontract.primaryExchange = "NASDAQ"

# Future
# mycontract.symbol = "ES"
# mycontract.secType = "FUT"
# mycontract.currency = "USD"
# mycontract.exchange = "CME"
# mycontract.lastTradeDateOrContractMonth = 20301220
# mycontract.conId = 495512563

# Option
# mycontract.symbol = "SPX"
# mycontract.secType = "OPT"
# mycontract.currency = "USD"
# mycontract.exchange = "SMART"
# # mycontract.lastTradeDateOrContractMonth = 202602
# mycontract.right = "P" # "P" put, "C" call
# mycontract.tradingClass = "SPXW"
# mycontract.strike = 5540

# app.reqContractDetails(app.nextId(), mycontract)

# app.reqMarketDataType(marketDataType=3)

# app.reqMktData(reqId=app.nextId(), contract=mycontract, genericTickList="232", snapshot=False, regulatorySnapshot=False, mktDataOptions=[])

# app.reqHeadTimeStamp(reqId=app.nextId(), contract=mycontract, whatToShow="TRADES", useRTH=1, formatDate=2)

# app.reqHistoricalData(reqId=app.nextId(), contract=mycontract, endDateTime="20251201 16:00:00 US/Eastern", 
#                       durationStr="2 D", barSizeSetting="1 hour", whatToShow="TRADES", useRTH=1, formatDate=1,
#                       keepUpToDate=False, chartOptions=[])
