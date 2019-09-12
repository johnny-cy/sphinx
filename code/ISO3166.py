import pyodbc
from datetime import datetime 
import pandas as pd
import requests
from bs4 import BeautifulSoup



conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'  # Driver 17 is for python > 3.0
                    'SERVER=cloud.emct.com.tw;'
                    'DATABASE=BirdFlu;'
                    'UID=Jeff;'
                    'PWD=Emct53117628')
cursor = conn.cursor()

res = requests.get('https://www.oie.int/wahis_2/public/wahid.php/Diseaseinformation/WI')
html_doc = res.text
soup = BeautifulSoup(html_doc, 'html.parser')
infos = soup.select('.simpletext a')
hrefs = [i.attrs['href'] for i in infos if 'avian influenza' in i.text]
texts = [i.text for i in infos if 'avian influenza' in i.text]

avian_influenza = pd.DataFrame({
    'info': texts, 
    'hrefs': hrefs, 
})


def ISO3166(areaDesc_EN):
    """
    Description:
        執行SQL語句，從資料庫獲取一筆國家中文名、英文名、國際碼

    Parameters:
        str : 英文國名
    
    Returns:
        可迭代的資料行 pyodbc.Row  

    Examples:
        ISO3166('Nepal')  => return (235, '尼泊爾', 'Nepal', 'NP', None)

    """
    cursor.execute("SELECT TOP 1 * FROM BirdFlu.dbo.ISO3166 WHERE areaDesc_EN = '{}'".format(areaDesc_EN))
    
    return cursor.fetchone()
    
# print(ISO3166('Nepal'))

def check_update():
    # retrieve latest from local database
    cursor.execute("SELECT TOP 1 * FROM BirdFlu.dbo.InterAI2 ORDER BY ID DESC;")
    dt = cursor.fetchone()[1].split("T")[0]
    FMT = "%Y-%m-%d"
    latest_local = datetime.strptime(dt, FMT)
    # get latest from OIE
    dt_ = avian_influenza['info'].apply(lambda t: t.split(":")[0].replace("/","-"))
    latest_ = []
   
c_list = [' Nepal',
 ' India',
 ' Nigeria',
 ' Chinese Taipei',
 ' South Africa',
 ' Vietnam',
 ' Mexico',
 ' Denmark',
 ' Iran',
 ' Bulgaria',
 ' United States of America',
 ' Israel',
 ' Afghanistan',
 ' Bhutan',
 " China (People's Rep. of)",
 ' Russia',
 ' Cambodia',
 ' Iraq',
 ' Dominican Republic',
 ' Togo',
 ' Egypt',
 ' Congo (Dem. Rep. of the)',
 ' Laos',
 ' Bangladesh',
 ' Malaysia',
 ' Saudi Arabia',
 ' France']
# print(ISO3166("China"))
print(ISO3166(c_list[0].replace(" ","")))
for i in c_list:
    if i[1:] == "China (People's Rep. of)":
        i = ' China'
    print(ISO3166(i[1:]))
