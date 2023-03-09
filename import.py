import csv
import pymysql
import requests
import sys

from datetime import datetime


company_name = sys.argv[1]


# Download data from nse
baseurl = "https://www.nseindia.com/"
url = f"https://www.nseindia.com/api/historical/cm/equity?symbol={company_name}&series=[%22EQ%22]&from=9-03-2022&to=9-03-2023&csv=true"
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                         'like Gecko) '
                         'Chrome/80.0.3987.149 Safari/537.36',
           'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br'}
session = requests.Session()
request = session.get(baseurl, headers=headers, timeout=5)
cookies = dict(request.cookies)
response = session.get(url, headers=headers, timeout=5, cookies=cookies)
#print(response.json())
url_content = response.content
csv_file = open('data.csv', 'wb')
csv_file.write(url_content)
csv_file.close()

# 2) Convert Date in CSV file

with open('data.csv', 'r') as csv_file:
    reader = csv.reader(csv_file)
    header = next(reader)  # skip header row
    rows = []
    for row in reader:
        # convert date format
        date_str = row[0]
        date_obj = datetime.strptime(date_str, '%d-%b-%Y')
        new_date_str = date_obj.strftime('%Y-%m-%d')
        row[0] = new_date_str
        rows.append(row)

with open('data.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(header)
    writer.writerows(rows)

# ) Replace comma from the file 

with open('data.csv', 'r') as input_file, open('output_file.csv', 'w', newline='') as output_file:
    reader = csv.reader(input_file)
    writer = csv.writer(output_file)
    
    for row in reader:
        new_row = [cell.replace(',', '') for cell in row]  # Replace commas in each cell
        writer.writerow(new_row)
        
# 3) Insert converted data in temp table
# open connection to MySQL server
connection = pymysql.connect(
    host='localhost',
    user='nitesh',
    password='root@123',
    database='market'
)
cursor = connection.cursor()

# 4) Truncate table temp
truncate_temp = 'truncate table temp;'
cursor.execute(truncate_temp)

# Create Table by Cmp_name

create_cmp = f'create table IF NOT EXISTS {company_name} like ITC'
cursor.execute(create_cmp)

# 5)read data from CSV file and insert into MySQL table
with open('output_file.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # skip header row
    for row in reader:
        sql = 'INSERT INTO temp (Date, Open, High, Low, Close, Volume) VALUES (%s, %s, %s, %s, %s, %s) ORDER BY Date ASC'
        cursor.execute(sql, (row[0], row[2], row[3], row[4], row[7], row[11]))

# 6)Insert data from temp to cmp table
insertdata = f'INSERT INTO {company_name} SELECT * FROM temp WHERE Date NOT IN (SELECT Date FROM {company_name})';
cursor.execute(insertdata)

#rm output_file.csv
#rm data.csv
# commit changes and close connection
connection.commit()
cursor.close()
connection.close()
