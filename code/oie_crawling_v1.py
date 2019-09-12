#!/usr/bin/env python
# coding: utf-8
# last updated 2019/09/10 by Johnny-cy@emct.com.tw

import pyodbc
from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re

ODBCDRIVER               = "{ODBC Driver 17 for SQL Server}"  # Driver 17 is for python > 3.0
SERVER                   = "cloud.emct.com.tw"
DATABASE                 = "BirdFlu"
UID                      = "Jeff"
PASSWORD                 = "Emct53117628"
OIETABLENAME             = "OIE"
INTERNATIONALAITABLENAME = "InternationalAI"

class OIE_system():
    def __init__(self):

        self.conn = pyodbc.connect('DRIVER='+ODBCDRIVER+';'  
                            'SERVER='+SERVER+';'
                            'DATABASE='+DATABASE+';'
                            'UID='+UID+';'
                            'PWD='+PASSWORD+';')

        self.cursor = self.conn.cursor()
        
        # check if assigned tables exist  
        self.notfound = None
        sql = """SELECT * 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' 
                AND  TABLE_NAME = '"""+OIETABLENAME+"'"
        _ = self.cursor.execute(sql)
        if _.fetchone() is None:
            print("OIETABLENAME", OIETABLENAME, "Not Found")
            self.notfound = True

        sql = """SELECT * 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' 
                AND  TABLE_NAME = '"""+INTERNATIONALAITABLENAME+"'"
        _ = self.cursor.execute(sql)
        if _.fetchone() is None:
            print("OIETABLENAME", INTERNATIONALAITABLENAME, "Not Found")
            self.notfound = True


    # fn: given areaDesc_EN, return related information from table ISO3166
    def ISO3166(self,areaDesc_EN):
        """
        Description:
            1. 轉換英文國名與ISO3166不符合的
            2. 再執行SQL語句，從ISO3166資料表獲取一筆國家中文名、英文名、國際碼
            3. 若國名在ISO3166表中查詢不到則回傳符號 - ，以及原本的英文名
        Parameters:
            str : 英文國名
        
        Returns:
            可迭代的資料行 pyodbc.Row  

        Examples:
            ISO3166('Nepal')  => return (235, '尼泊爾', 'Nepal', 'NP', None)

        """
        if areaDesc_EN == "China (People's Rep. of)":
            areaDesc_EN = 'China'
        if areaDesc_EN == "United States of America":
            areaDesc_EN = 'United States'
        if areaDesc_EN == "Congo (Dem. Rep. of the)":
            areaDesc_EN = "Democratic Republic of the Congo"
        self.cursor.execute("SELECT TOP 1 * FROM BirdFlu.dbo.ISO3166 WHERE areaDesc_EN = '{}'".format(areaDesc_EN))
        data = self.cursor.fetchone()
        if data:
            return data
        else:
            print(areaDesc_EN+"在ISO3166找不到對應的資料...")
            return list(["-","-",areaDesc_EN,"-","-"])
    
        # eg : pass 'Nepal" and return (235, '尼泊爾', 'Nepal', 'NP', None)

    # fn : fetchall reportid from local dbo.InterAI2
    def get_local_reportid(self):
        """
        Description:
            1. 執行SQL語句，從OIE資料表取的所有案例id
            
        Parameters:
            
        Returns:
            可迭代的列表 list  

        Examples:
            get_local_reportid()  => return ['31235, '34583', ..., '23987']

        """
        self.cursor.execute("SELECT ReportId FROM BirdFlu.dbo.OIE ORDER BY ID DESC;")
        # self.cursor.execute("SELECT web FROM BirdFlu.dbo.InterAI2 ORDER BY ID DESC;")
        rows = self.cursor.fetchall()
        # reportid = [row[0].split("=").pop() for row in rows]
        ReportId = [row[0] for row in rows ]
        return ReportId

    # fn : get targeted data from OIE website
    def get_avian_influenza(self):
        """
        Description:
            1. 連接OIE主頁，獲取主頁的所有案例id
            2. 判斷若有關鍵字而且主頁案例id不存在本地案例id，則視為新案例，需導入
            3. 用pandas存放資料並回傳

        Parameters:
            
        Returns:
            可迭代的資料列表 pandas.DataFrame  

        Examples:
            get_local_reportid()  => return ['31235, '34583', ..., '23987']

        """
        res = requests.get('https://www.oie.int/wahis_2/public/wahid.php/Diseaseinformation/WI')
        html_doc = res.text
        soup = BeautifulSoup(html_doc, 'html.parser')
        infos = soup.select('.simpletext a')
        hrefs = []
        texts = []
        local_list = self.get_local_reportid()
        for i in infos:
            id_ = re.search(r"/wahis_2/public/wahid.php/Reviewreport/Review.*?([0-9]+)", i.attrs['href'])[1]           
            if 'avian influenza' in i.text and int(id_) not in local_list:
                hrefs.append(i.attrs['href'])
                texts.append(i.text)

        avian_influenza = pd.DataFrame({
            'info': texts, 
            'hrefs': hrefs, 
        }) 
        return avian_influenza

    # fn : get started to crawl targeted data
    def crawl_inner_layer(self,avian_influenza):
        # only to assemble an url structure for further use cases
        splits = avian_influenza['hrefs'][0].split('"') 
        ''' ['javascript: open_report(',
            '/wahis_2/public/wahid.php/Reviewreport/Review?page_refer=MapFullEventReport&',
            ',31621)']'''
        area = avian_influenza['info'][0].split(', ')[1]
        '''Nepal
        '''
        url = "https://www.oie.int{}reportid={}".format(splits[1], splits[2][1:-1])
        '''https://www.oie.int/wahis_2/public/wahid.php/Reviewreport/Review?page_refer=MapFullEventReport&reportid=31621'''
        # requests and BeautifulSoup setp in for getting targeted information
        ress = requests.get(url)
        soup_ = BeautifulSoup(ress.text, 'html.parser')
        infos_ = soup_.select('.TableFoyers')[0]

        # to concise reportid, middle, area for further use cases
        avian_influenza['reportid'] = avian_influenza['hrefs'].apply(lambda t: t.split('"')[2][1:-1])
        avian_influenza['middle'] = avian_influenza['hrefs'].apply(lambda t: t.split('"')[1])
        avian_influenza['area'] = avian_influenza['info'].apply(lambda t: t.split(', ')[1])

        # Ready to crawl the inner page
        # FN1C-2資料處理：新案例資料必須符合目前系統頁面所需之資料格式及欄位，需進行資料轉換處理
        for i in tqdm(range(len(avian_influenza))):
            try:
                # dicts as targeted data containers
                data_to_dbo_OIE = {}
                data_to_dbo_InterAI2 = {}
                # get area from previous
                area = avian_influenza['area'][i]
                # get title from previous
                # title =  avian_influenza['info'][i]
                # get url middle from previous
                middle = avian_influenza['middle'][i]
                # get reportid from previous
                ReportId = avian_influenza['reportid'][i]
                url = "https://www.oie.int{}reportid={}".format(middle, ReportId)
                # beaufitulsoup again
                ress = requests.get(url)
                soup_ = BeautifulSoup(ress.text, 'html.parser') 
                infos_ = soup_.select('.TableFoyers') # for searching total outbreaks.
                infos_a = soup_.select('.TableFoyers a') # for searching related reports
                # pandas again
                df0 = pd.read_html(ress.text, attrs={'class':'TableFoyers'})[0] # for searching general information
                
                # get outbreaks
                outbreaks_ = re.search(r'class="filtrer_th ta_left">Total outbreaks: (.*?)</td>', str(infos_), re.S)
                outbreaks = outbreaks_[1] if outbreaks_ else "0"
                # method 1 : get general information
                # reportType,eventStart,eventConfirm,reportDate,summitedOIE,reasonNoti,prevDate,manifes,Causal_agent_,serotype,diagnosis,eventPertainTo  = df0.set_index(0).loc[[
                #                                                             'Report type',
                #                                                             'Date of start of the event',
                #                                                             'Date of confirmation of the event',
                #                                                             'Report date',
                #                                                             'Date submitted to OIE',
                #                                                             'Reason for notification',
                #                                                             'Date of previous occurrence',
                #                                                             'Manifestation of disease',
                #                                                             'Causal agent',
                #                                                             'Serotype',
                #                                                             'Nature of diagnosis',
                #                                                             'This event pertains to',
                #                                                             ],1]

                # method 2 : get general information
                
               
                df0['g'] = [i for i in range(df0.shape[0])] # as an index 
                values = df0.set_index('g').loc[:,1]
                titles = df0.set_index('g').loc[:,0].apply(lambda t:t.title().replace(" ",""))
                data_to_dbo_OIE = {titles[i] : values[i] for i in range(len(titles))}
                # get related reports
                RelatedReports = []
                for i in infos_a:
                    id__ = re.search(r"/wahis_2/public/wahid.php/Reviewreport/Review.*?([0-9]+)", i.attrs['href'])[1]
                    RelatedReports.append(id__)
                # lng,lat
                lng = "nan"
                lat = "nan"
                description = "Sourced from OIE website."
                # finished collecting data !
            
                # fn : areaDesc_EN, CN, ISOCODE etc.. for further use
                tmp = self.ISO3166(area) # retrieve from table ISO3166
                # causal_agent alternate  
                if 'CausalAgent' in data_to_dbo_OIE.keys():
                    if data_to_dbo_OIE['CausalAgent'] == "Highly pathogenic avian influenza virus" or data_to_dbo_OIE['CausalAgent'] == "Virus de la influenza aviar altamente patógena":
                        data_to_dbo_OIE['CausalAgent'] = "HPAI"
                    elif data_to_dbo_OIE['CausalAgent'] == "Low pathogenic avian influenza virus" or data_to_dbo_OIE['CausalAgent'] == "Virus de l'influenza aviaire faiblement pathogène":
                        data_to_dbo_OIE['CausalAgent'] = "LPAI"

                # forming structured data to birdflu dbo OIE
                data_to_dbo_OIE['ReportId'] = ReportId #
                data_to_dbo_OIE['ThisEventPertainsTo'] = str(data_to_dbo_OIE['ThisEventPertainsTo']).title() if 'ThisEventPertainsTo' in data_to_dbo_OIE.keys() else "nan"
                data_to_dbo_OIE['RelatedReports'] = ",".join(RelatedReports) if 'RelatedReports' in data_to_dbo_OIE.keys() else "nan"
                data_to_dbo_OIE['lng'] = lng 
                data_to_dbo_OIE['lat'] = lat 
                data_to_dbo_OIE['url'] = url 
                data_to_dbo_OIE['outbreaks'] = outbreaks 
                data_to_dbo_OIE['description'] = description 
                data_to_dbo_OIE['area'] = tmp[2] # areaDesc_EN 
                data_to_dbo_OIE['to'] = OIETABLENAME 

                # yield for storage
                yield data_to_dbo_OIE

                # if there is any outbreak, or it's not Chinese Taipei, then reformat data above and yield again for another odb.interAI2
                if outbreaks != "0" and area != "Chinese Taipei":
                    print("Info : Outbreak > 0 and not in Chinese Taipei")
                    # reassemble string for customed needs
                    # sent
                    tmp_ = datetime.strptime( data_to_dbo_OIE['DateSubmittedToOie'], "%d/%m/%Y") # d/m/Y => Y/m/d
                    sent = "{}-{}-{}T00:00+08:00".format(tmp_.year,tmp_.month,tmp_.day) # customed format
                    # headline
                    headline = tmp[1]+"─禽類禽流感"
                    
                    # description (sent, area, outbreaks, causal_agent, serotype) 
                    description = "OIE於"+str(tmp_.month)+"月"+str(tmp_.day)+"日公布"+str(tmp[1])+str(outbreaks)+"起"+str(data_to_dbo_OIE['CausalAgent'])+"的"+str(data_to_dbo_OIE['Serotype'])+"疫情。"
                    # areaDesc
                    areaDesc = tmp[1] 
                    # ISO3166
                    ISO3166 = tmp[3]
                    # forming structured data_to_dbo_InterAI2
                    data_to_dbo_InterAI2['sent'] = sent
                    data_to_dbo_InterAI2['headline'] = headline
                    data_to_dbo_InterAI2['description'] = description
                    data_to_dbo_InterAI2['areaDesc'] = areaDesc
                    data_to_dbo_InterAI2['ISO3166'] = ISO3166
                    data_to_dbo_InterAI2['lng'] = lng
                    data_to_dbo_InterAI2['lat'] = lat
                    data_to_dbo_InterAI2['web'] = url
                    data_to_dbo_InterAI2['to'] = INTERNATIONALAITABLENAME

                    # yield for storage
                    yield data_to_dbo_InterAI2 
                        
            except Exception as err:
                print(err)

    
    # # fn : pass dict in to insert to dbo.InterAI2 once at a time
    # def to_mssql_interAI2(self,target):
    #     # elimate values == "nan" from the target, it will storage as Null is MS SQL
    #     target = {key: value for key, value in target.items() if str(value) != "nan"}
    #     keys = ",".join(target.keys()) 
    #     values = ','.join(['%s']*len(target))
    #     sql = 'insert into [BirdFlu].[dbo].InterAI2(%s) values%s'%(keys,tuple(target.values()))
    #     self.cursor.execute(sql)
    #     print("Successfully inserted a row into InterAI")
    #     self.conn.commit()
        

    # # fn : pass dict in to insert to dbo.OIE once at a time
    # def to_mssql_OIE(self,target):
    #     # get existing fields
    #     sql = "select column_name from information_schema.columns where table_name = 'oie'"
    #     self.cursor.execute(sql)
    #     fields = self.cursor.fetchall()
    #     field_list = [i[0] for i in fields]
    #     # create new fields if it hasn't found in field_list
    #     newFields = [i for i in target.keys() if i not in field_list]
    #     # print(newFields)
    #     for f in newFields:
    #         sql = "ALTER TABLE OIE ADD {} varchar(255);".format(f)
    #         self.cursor.execute(sql)          
        
    #     target = {key: value for key, value in target.items() if str(value) != "nan"}
    #     keys = ",".join(target.keys()) 
    #     values = ','.join(['%s']*len(target))
    #     sql = 'insert into [BirdFlu].[dbo].OIE(%s) values%s'%(keys,tuple(target.values()))
    #     self.cursor.execute(sql)
    #     self.conn.commit()
   
    def to_mssql(self,target,table):
        del target['to']
        if table == OIETABLENAME:
            target = {key: value for key, value in target.items() if str(value) != "nan"}

            sql = "select column_name from information_schema.columns where table_name = '"+OIETABLENAME+"';"
            self.cursor.execute(sql)
            fields = self.cursor.fetchall()
            field_list = [i[0] for i in fields]
            # create new fields if it hasn't found in field_list
            newFields = [i for i in target.keys() if i not in field_list]

            # print(newFields)
            for f in newFields:
                sql = "ALTER TABLE "+OIETABLENAME+" ADD {} varchar(255);".format(f)
                self.cursor.execute(sql)
            
            keys = ",".join(target.keys()) 
            values = ','.join(['%s']*len(target))
            sql = 'insert into '+OIETABLENAME+'(%s) values%s'%(keys,tuple(target.values()))
            self.cursor.execute(sql)
            self.conn.commit()

        elif table == INTERNATIONALAITABLENAME:
             # elimate values == "nan" from the target, it will storage as Null is MS SQL
            target = {key: value for key, value in target.items() if str(value) != "nan" }
            keys = ",".join(target.keys()) 
            values = ','.join(['%s']*len(target))
            sql = 'insert into '+INTERNATIONALAITABLENAME+'(%s) values%s'%(keys,tuple(target.values()))
            self.cursor.execute(sql)
            print("Successfully inserted a row into "+INTERNATIONALAITABLENAME+".")
            self.conn.commit()

if __name__ == "__main__":
    
    if OIE_system().notfound is True:
        print("Tables ",OIETABLENAME,"or", INTERNATIONALAITABLENAME," either one of them may not be found at database :",DATABASE)
    else:
        OIE = OIE_system()
        # FN1C-1爬蟲：改寫目前爬蟲程式，進行爬蟲，並判斷是否有新案例，產出新案例資料。
        avian_influenza = OIE.get_avian_influenza() # return a avian_influenza, type is pd.DataFrame.
        
        # if the return data is not empty, otherwise it goes "already up to date message"
        if not avian_influenza.empty:
            # FN1C-3匯入資料表OIE：將原始爬取之新案例資料匯入資料表OIE中儲存。
            for i in OIE.crawl_inner_layer(avian_influenza): 
                if i['to'] == OIETABLENAME:      
                    OIE.to_mssql(i, OIETABLENAME)
                    
                # FN1C-4匯入資料表InternationalAI：前FN1C-2資料處理程序產出資料匯入資料表
                elif i['to'] == INTERNATIONALAITABLENAME :
                    OIE.to_mssql(i, INTERNATIONALAITABLENAME)
        else:
            print("local table has already been updated.")
        
        # close cursor and connection when completed
    OIE.cursor.close()
    OIE.conn.close()
    print(datetime.datetime.now(), "has completed a run !")


# FN1C-5系統自動排程：於系統環境設定每日執行之排程。
# 2019/9/10 開始每日12點執行一次, 執行環境Linux