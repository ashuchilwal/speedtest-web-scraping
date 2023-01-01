from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
import mysql.connector
import os
import pymssql

url = "https://www.speedtest.net/global-index"

r = requests.get(url)
htmlcontent = r.content

soup = bs(htmlcontent, "html.parser")

data= soup.find("div", id="column-mobileMedian").find("div", class_="specific-results").find("table",class_="list-results").find("tbody")
rows = data.find_all("tr")


colms = []
for row in rows:
    cols = row.find_all('td')
    
    for ele in cols:
        colms.append(ele.text.strip())

# print(colms)
mobile_rank = []
for i in range(0,len(colms),5):
    mobile_rank.append(int(colms[i]))

mobile_country = []
for i in range(2, len(colms), 5):
    mobile_country.append(colms[i])

speed = []
for i in range(3, len(colms), 5):
    speed.append(colms[i])

# mobile_d_f = pd.DataFrame({'Rank':mobile_rank, 'Country':mobile_country, 'Speed':speed})

# mobile_d_f.to_csv("D:\\Course5I\\Assignment\\mobile.csv",index=False)

final_mobile = list(zip(mobile_rank, mobile_country, speed))
# print(list(final_mobile))



my_db = mysql.connector.connect(host='localhost', user='root', password='password', database='speedtest')
# print(my_db.connection_id)
cur = my_db.cursor()
cur.execute("Drop table if exists `mobile_speed`")
cur.execute("CREATE TABLE IF NOT EXISTS `mobile_speed` (`Rank` INT(5), `Country` varchar(35), `Speed` varchar(10))")
q = "INSERT INTO `mobile_speed` values(%s,%s,%s)"
cur.executemany(q,final_mobile)
my_db.commit()


# brod
data_brod = soup.find("div", id='column-fixedMedian').find("div", class_='specific-results').find("table", class_='list-results').find("tbody")
rows_brod = data_brod.find_all('tr')


brod_columns = []
for row in rows_brod:
    brod_cols = row.find_all('td')
    for ele in brod_cols:
        brod_columns.append(ele.text.strip())


brod_rank = []
for i in range(0,len(brod_columns),5):
    brod_rank.append(brod_columns[i])

brod_country = []
for i in range(2,len(brod_columns),5):
    brod_country.append(brod_columns[i])

brod_speed = []
for i in range(3,len(brod_columns),5):
    brod_speed.append(brod_columns[i])

# print(brod_country)
# print(brod_speed)

# brod_df = pd.DataFrame({'Rank':brod_rank, 'Country':brod_country, 'Speed':brod_speed})
# brod_df.to_csv("D:\\Course5I\\Assignment\\Broadband.csv",index=False)

final_brod = list(zip(brod_rank, brod_country, brod_speed))
# print(final_brod)

cur.execute("DROP TABLE IF EXISTS `brod_speed`")
cur.execute("CREATE TABLE IF NOT EXISTS `brod_speed` (`Rank` INT(5), `Country` varchar(35), `Speed` varchar(20))")
q1 = "INSERT INTO `brod_speed` VALUES (%s,%s,%s)"
cur.executemany(q1,final_brod)
my_db.commit()