# importing required libraries
import time
from tqdm import tqdm
import urllib.request
from datetime import date
import subprocess
from pymongo import MongoClient,InsertOne
from datetime import datetime



def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo

    client = MongoClient(
        "mongodb+srv://higgsboson1209:RoFLAM!<@cluster0.r7ttr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

    return client['MUTUAL_FUNDS']


dbname = get_database()

collection_name = dbname["MUTUAL-FUNDS-DATA"]

last_run = open("/home/higgsboson/PycharmProjects/ProdigalTechnologies/venv/last_run.txt", 'r+')
def date_config_for_daily_runs():
    last_date = "05-Dec-2021"
    today = date.today()
    today = today.strftime('%d-%b-%Y')
    # Finding when our script was run the last time succesfully
    line = subprocess.check_output(
        ['tail', '-1', "/home/higgsboson/PycharmProjects/ProdigalTechnologies/venv/last_run.txt"])
    if (line):
        last_date = line.decode()
    if last_date==today:
        print("No New Data Today, EXITING")
        exit(0)

    return last_date, today


last_date, today = date_config_for_daily_runs()
#Finding the URL on which we request the data
url = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}&todt={}".format(last_date, today)

print(url)
try:
    #Fetching the data
    data = urllib.request.urlopen(url)
except:
    print("Not able to fetch the data")
    exit()

#Creating an array to fetch all the data into an array
dataa = []
count = 0
for data_line in data:
    if count==0:
        count += 1
        continue
    data_line = data_line.decode()
    data_line = data_line.rstrip()
    data_line = data_line.split(";")
    if (len(data_line) < 8):
        continue
    #Converting to datetime object so we can query it easily later
    data_line[-1]=datetime.strptime(data_line[-1],'%d-%b-%Y')
    dataa.append(data_line)
print("The number of enteries fetched is: ", len(dataa))
ec=0
requests=[]
#Iterating through all enteries linearly and pushing them to the database
for i in tqdm(dataa):
    item_dict = {"scheme_code": i[0], "sceheme_name": i[1], "ISIN-DIV-PAYOUT": i[2], "ISIN-DIV-REINVESTMENT": i[3],
                 "NET_ASSET_VALUE": i[4], "REPURCHASE_PRICE": i[5], "SALE-PRICE": i[6], "Date": i[7]}

    requests.append(InsertOne(item_dict))
t1=time.time()
try:
    #TRY PUSHING ITEMS TO COLLECTION AS A BULK REQUEST
    #CAN IMPLEMENT BATCH PROCESSING TO ENSURE AN EVEN BETTER PERFORMANCE
    collection_name.bulk_write(requests)
except Exception as e:
    #CAN'T PUSH ITEMS TO COLLECTION
    ec+=1
    print(e)
print("TIME TAKEN TO PUSH ALL THE RECORDS is",time.time()-t1)
if ec==0:
    last_run.write(today)
