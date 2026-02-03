from ibapi.client import *
from ibapi.wrapper import *
from ibapi.tag_value import TagValue
import datetime
import time
import threading

port = 7497

bank = {}
position_ref = {}

class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)

    def nextValidId(self, orderId: OrderId):
        self.orderId = orderId
    
    def nextId(self):
        self.orderId += 1
        return self.orderId
    
    def error(self, reqId, errorCode, errorString, advancedOrderReject):
        print(f"reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderReject}")

    def updateAccountValue(self, key, val, currency, accountName):
        if key == "TotalCashBalance" and currency == "BASE":
          bank[key] = float(val)

          # If we drop below $1M, disconnect
          if float(val) <1000000:
              self.disconnect()
    
    def position(self, account, contract, position, avgCost):
        position_ref[contract.symbol] = position

    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        if rank < 5:
            rankId = rank+reqId
            bank[rankId] = {"contract": contractDetails.contract}
            position_ref[contractDetails.contract.symbol] = 0
            app.reqMktData(rankId, contractDetails.contract, "", False, False, [])
            print(f"Rank {rank} Contract: {contractDetails.contract.symbol} @ {contractDetails.contract.exchange}")

    def scannerDataEnd(self, reqId):
        self.cancelScannerSubscription(reqId)
      
    def tickPrice(self, reqId, tickType, price, attrib):
        if "LAST" not in bank[reqId].keys():
            bank[reqId]["LAST"] = price

        bankTick = bank[reqId]["LAST"]
        bankContract = bank[reqId]["contract"]

        order = Order()
        order.tif = "DAY"
        order.totalQuantity = 5
        order.orderType = "MKT"

        # If the new price is more than 5% higher than our previous price point.
        if (bankTick * 1.05) < price:
            order.action = "BUY"
            app.placeOrder(app.nextId(), bankContract, order)
        # If the new price is less than 6% of our previous price point
        elif (bankTick * 0.94) > price and position_ref[bankContract.symbol] >= 5:
            order.action = "SELL"
            app.placeOrder(app.nextId(), bankContract, order)

        bank[reqId]["LAST"] = price
    
    def openOrder(self, orderId, contract, order, orderState):
        if orderState.status == "Rejected":
            print(f"{datetime.datetime.now()} {orderState.status}: ID:{orderId} || {order.action} {order.totalQuantity} {contract.symbol}")
    
    def execDetails(self, reqId, contract, execution):
        print(f"Execution Details: ID:{execution.orderId} || {execution.side} {execution.shares} {contract.symbol} @ {execution.time}")

app = TestApp()
app.connect("localhost", port, 1005)
threading.Thread(target=app.run).start()
time.sleep(1)

app.reqAccountUpdates(True, "")

app.reqPositions()

sub = ScannerSubscription()
sub.instrument = "STK"
sub.locationCode = "STK.US.MAJOR"
sub.scanCode = "MOST_ACTIVE"
scan_options = []
filter_options = [
    TagValue("avgVolumeAbove","1000000"),
    TagValue("priceAbove", '10')
]
app.reqScannerSubscription(app.nextId(), sub, scan_options, filter_options)