#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import sqlite3
import urllib
from bs4 import BeautifulSoup
from hdfs import InsecureClient

# configuration variables
hdfsNamenode="http://metatron-hadoop-01:50070"
hdfsBasePath="datain/realty/"
saveToDB="sqlite"
debugMode=False

def collect_trade(ym,lawd_cd):

    API_KEY = "pXg3OuVv3OkPlp5SWHu06P1BM4OsOWxOQU5IZHzmS%2ByFzNZCZtvc2%2FTf8gpGriByrFMKDaMuoYL154XvgTq7cA%3D%3D"

    url="http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
    url=url+"?startPage=1&numOfRows=999&LAWD_CD="+lawd_cd+"&DEAL_YMD="+ym+"&serviceKey="+API_KEY
    if debugMode:
       print(url)

    resultXML = urllib.urlopen(url)
    result = resultXML.read()
    xmlsoup = BeautifulSoup(result, "lxml-xml")
    te=xmlsoup.findAll("item")
    sil=pd.DataFrame()
    
    for t in te:
        try:
           price=t.find("거래금액").text
           build_y=t.find("건축년도").text
           trade_y=t.find("년").text
           trade_m=t.find("월").text
           trade_d=t.find("일").text
           if t.find("도로명"):
               street_addr1=t.find("도로명").text
           else:
               street_addr1=""

           if t.find("도로명건물본번호코드"):
               street_addr2=t.find("도로명건물본번호코드").text
               street_addr3=t.find("도로명건물부번호코드").text
           else:
               street_addr2=""
               street_addr3=""

           dong_addr1=t.find("법정동").text
           dong_addr2=t.find("법정동본번코드").text
           dong_addr3=t.find("법정동부번코드").text
           dong_addr4=t.find("법정동지번코드").text
           apt_nm=t.find("아파트").text
           size=t.find("전용면적").text
           jibun=t.find("지번").text

           ji_code=t.find("지역코드").string
           floor=t.find("층").text
        
           temp = pd.DataFrame(([[ji_code,price,build_y,trade_y,trade_m,trade_d,street_addr1,street_addr2,street_addr3,dong_addr1,dong_addr2,dong_addr3,jibun,apt_nm,floor,size]]), columns=["ji_code","price","build_y","trade_y","trade_m","trade_d","street_addr1","street_addr2","street_addr3","dong_addr1","dong_addr2","dong_addr3","jibun","apt_nm","floor","size"])
           sil=pd.concat([sil,temp])
        except:
           if debugMode:
             print("data error..")
             print(t)
           continue

    sil=sil.reset_index(drop=True)
    return sil

def collect_rent(ym,lawd_cd):

    API_KEY = "pXg3OuVv3OkPlp5SWHu06P1BM4OsOWxOQU5IZHzmS%2ByFzNZCZtvc2%2FTf8gpGriByrFMKDaMuoYL154XvgTq7cA%3D%3D"

    url="http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent"
    url=url+"?startPage=1&numOfRows=999&LAWD_CD="+lawd_cd+"&DEAL_YMD="+ym+"&serviceKey="+API_KEY
    if debugMode:
       print(url)

    resultXML = urllib.urlopen(url)
    result = resultXML.read()
    xmlsoup = BeautifulSoup(result, "lxml-xml")
    te=xmlsoup.findAll("item")
    sil=pd.DataFrame()
    
    for t in te:
        try:
           deposit=t.find("보증금액").text
           monthlypay=t.find("월세금액").text
           build_y=t.find("건축년도").text
           trade_y=t.find("년").text
           trade_m=t.find("월").text
           trade_d=t.find("일").text
           street_addr1=""
           street_addr2=""
           street_addr3=""
           dong_addr1=t.find("법정동").text
           dong_addr2=""
           dong_addr3=""
           dong_addr4=""
           apt_nm=t.find("아파트").text
           size=t.find("전용면적").text
           jibun=t.find("지번").text

           ji_code=t.find("지역코드").string
           floor=t.find("층").text
        
           temp = pd.DataFrame(([[ji_code,deposit,monthlypay,build_y,trade_y,trade_m,trade_d,street_addr1,street_addr2,street_addr3,dong_addr1,dong_addr2,dong_addr3,jibun,apt_nm,floor,size]]), columns=["ji_code","deposit","monthlypay","build_y","trade_y","trade_m","trade_d","street_addr1","street_addr2","street_addr3","dong_addr1","dong_addr2","dong_addr3","jibun","apt_nm","floor","size"])
           sil=pd.concat([sil,temp])
        except Exception as ex:
           if debugMode:
              print("data error..", ex)
              print(t)
           continue

    sil=sil.reset_index(drop=True)
    return sil


if __name__ == "__main__":

    jibun_code=list(["11110","11140","11170","11200","11215","11230","11260","11290","11305","11320","11350","11380","11410","11440","11470","11500","11530","11545","11560","11590","11620","11650","11680","11710","11740","26140","26170","26200","26230","26260","26290","26320","26350","26380","26410","26470","26500","26530","26710","27260","27290","27710","28115","28116","28185","28200","28237","28245","28265","28710","28720","29200","30200","30230","31710","41110","41111","41113","41115","41117","41130","41131","41133","41135","41150","41170","41171","41173","41190","41210","41220","41250","41270","41271","41273","41280","41281","41285","41287","41290","41310","41360","41370","41390","41410","41430","41450","41460","41461","41463","41465","41480","41500","41550","41570","41590","41610","41630","41650","41670","41800","41820","41830","42110","42130","42150","42170","42190","42210","42230","42720","42730","42750","42760","42770","42780","42790","42800","42810","42820","42830","43110","43111","43112","43113","43114","43130","43150","43720","43730","43740","43745","43750","43760","43770","43800","44130","44131","44133","44150","44180","44200","44210","44230","44250","44270","44710","44760","44770","44790","44800","44810","44825","45110","45111","45113","45118","45130","45140","45145","45180","45190","45210","45710","45720","45730","45740","45750","45770","45790","45800","46110","46130","46150","46170","46230","46710","46720","46730","46770","46780","46790","46800","46810","46820","46830","46840","46860","46870","46880","46890","46900","46910","47110","47111","47113","47130","47150","47170","47190","47210","47230","47250","47280","47290","47720","47730","47750","47760","47770","47820","47830","47840","47850","47900","47920","47930","47940","48120","48121","48123","48125","48127","48129","48170","48220","48240","48245","48250","48270","48310","48330","48720","48730","48740","48840","48850","48860","48870","48880","48890","50110","50130"])
    #ym=list(["201701","201702","201703","201704","201705","201706","201707","201708","201709","201710","201711","201712","201801"])
    ym=[]
    #jibun_code=list(["11110"])

    if len(sys.argv) < 3:
       print "Usage : %s yyyymm saveType " % (sys.argv[0])
       exit(1)
    else:
       ym.append(sys.argv[1])
       if sys.argv[2].lower() != "":
          saveType=sys.argv[2].lower()

    # collect trade data
    sil_trade=pd.DataFrame()
    for m in ym:
      for co in jibun_code:
            temp=collect_trade(m,co)
            sil_trade=pd.concat([sil_trade,temp])
            if debugMode:
               print(co+", "+str(len(temp))+" is compleded")

      if debugMode:
         print("*"+str(m)+" is completed")

    # save data
    if saveType=="sqlite":
       con = sqlite3.connect("./apartment.db")
       sil_trade.to_sql('trade', con, if_exists='append', index=False)
       con.close()
    elif saveType=="csv":
       sil_trade.to_csv('apartment_trade.csv', sep=',', encoding='utf-8', index=False)
    elif saveType=="hdfs":
       client_hdfs = InsecureClient(hdfsNamenode)
       with client_hdfs.write(hdfsBasePath+m+'/apartment_trade.csv', encoding='utf-8',overwrite=True) as writer:
           sil_trade.to_csv(writer, sep=',', index=False)

    # collect lent data
    sil_rent=pd.DataFrame()
    for m in ym:
      for co in jibun_code:
            rent=collect_rent(m,co)
            sil_rent=pd.concat([sil_rent,rent])
            if debugMode:
               print(co+", "+str(len(rent))+" is compleded")

      if debugMode:
          print("*"+str(m)+" is completed")

    # save data
    if saveType=="sqlite":
       con = sqlite3.connect("./apartment.db")
       sil_rent.to_sql('rent', con, if_exists='append', index=False)
       con.close()
    elif saveType=="csv":
       sil_rent.to_csv('apartment_rent.csv', sep=',', encoding='utf-8', index=False)
    elif saveType=="hdfs":
       client_hdfs = InsecureClient(hdfsNamenode)
       with client_hdfs.write(hdfsBasePath+m+'/apartment_rent.csv', encoding='utf-8',overwrite=True) as writer:
           sil_rent.to_csv(writer, sep=',', index=False)

    # terminate program
    exit(0)
