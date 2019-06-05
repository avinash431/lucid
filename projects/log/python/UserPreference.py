import re
import csv
import sys
import os
rows = {}
currencies = []
insert_records = []
user_currencies_map={}
if len(sys.argv) != 2:
    print "Please pass file name "
    print "Usage python UserPreference.py hk|sg "
    sys.exit(1)

infilename = "catalina" + sys.argv[1]+".out"
outfilename = "catalinaout_" + sys.argv[1]+".csv"

FILE_LOC = "/hivestage/tmmart/tmmart/TDAP/InFile/DOL/"
print os.path.join(FILE_LOC, infilename) 
with open(os.path.join(FILE_LOC, infilename), "r") as f:
    for line in f:
        user = []
        if "returnList" in line:
            match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)
            if match:
                datetime = match.group()
            content = line.split('"returnList":')[1].replace("[", "").replace("]", "").split(",")
            for element in content:
                key = element.split(":")[0].replace("{", "").replace("]", "").replace('"', "")
                value = element.split(":")[1].replace('"', "").replace("}", "")
                if key == 'defaultAccount':
                    if value:
                        value = value.split("-")[1]
                if key == "flagField":
                    continue
                if key in rows:
                    rows[key].append(value)
                else:
                    rows[key] = [value]
        if "Username" in line :
            username = line.split("Username")[1].split(';')[0].replace(':', '').strip()
        if "putting value in:" in line:
            currency = line.split("putting value in:")[1].strip()
            match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)
            if match:
                timestamp = match.group()
            currencies.append((currency, timestamp))
        if currencies and "end" in line:
            user_currencies_map[username] = currencies

user_details = zip(*(rows.values()))

print user_currencies_map
write_records = []
for user in user_currencies_map:
    for rec in user_details:
        if user in rec:
            for currency, timestamp in user_currencies_map[user]:
                # write_records.append((datetime, ) + rec + (currency,))
                print currency,timestamp
                write_records.append((timestamp,) + rec + (currency,))
        else:
            write_records.append((datetime,) + rec)


user_info = "datetime," + ','.join(rows.keys()) + ",currencies"
headers = (tuple(user_info.split(",")),)

write_records = list(headers) + write_records
print write_records

with open(os.path.join(FILE_LOC, outfilename), 'w') as myfile:
    wr = csv.writer(myfile)
    wr.writerows(write_records)
