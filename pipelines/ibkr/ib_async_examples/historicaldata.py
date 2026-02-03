from ib_async import *
# util.startLoop()  # uncomment this line when in a notebook

ib = IB()
ib.connect('192.168.193.243', 4002, clientId=1)

# Request historical data
contract = Forex('EURUSD')
bars = ib.reqHistoricalData(
    contract, endDateTime='', durationStr='30 D',
    barSizeSetting='1 hour', whatToShow='MIDPOINT', useRTH=True)

# Convert to pandas dataframe (pandas needs to be installed):
df = util.df(bars)
print(df.head())

ib.disconnect()