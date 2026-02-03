from ib_insync import *
ib = IB()
ib.connect('192.168.193.243', 7497, clientId=129)
print("Connected:", ib.isConnected())
ib.disconnect()