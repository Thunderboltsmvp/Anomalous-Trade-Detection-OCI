import csv, json

csvfile = open('C:\Venus\OCI_MVP\RATD_MVP\Trade_Data.csv', 'r')


fieldnames = ("FUND","SEC","TRADE_DT","TXN_TYPE","QTY","PRICE","TRADER","BROKER")
#reader = csv.DictReader(csvfile , fieldnames)
reader = csv.DictReader(csvfile)
output = []
for eachrow in reader:
  row = {}
  for field in fieldnames:
    row[field] = eachrow[field]
  print(row)
output.append(row)
