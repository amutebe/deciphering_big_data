#Import necessary libraries
import pandas as pd
import numpy as np
import plotly.express as px
import requests
import warnings
import mysql.connector
from mysql.connector import Error
import sqlalchemy as db
from sqlalchemy import create_engine
from pandas.core.indexes.base import is_string_dtype




############################Read a mysql table directly into a dataframe #################################################
#Mysql connection parameters
hostname= "localhost"
database= "new_db"
username= ""
password= ""
prt=3308

#Defining mysql  connection engine using sqlalchemy
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(host=hostname, db=database, user=username, pw=password,port=prt))

##############################Read Property listing Data table from mysql####################################################
df_sql = pd.read_sql('SELECT * from property_listing', engine)
#print(df_sql.head())
#print(df_sql.head())


############################Selecting importing columns ffrom property lisiting##################################################################
property_list=df_sql[["zpid","hdpData.homeInfo.homeType","price","baths","beds","area","detailUrl","address","brokerName"]]

#print(property_list)
##Checking propert listing Data types

data_types = property_list.dtypes

print(data_types)

print(property_list["price"])

#processing the price column to remove non numeric charaters
property_list['price'] = property_list['price'].str.replace(',', '').str.replace('$', '').str.replace('From ', '').astype(int)

print(property_list["price"])

#triming the Lisiting by: prefix from the broker's name
property_list['brokerName'] = property_list['brokerName'].str.replace('Listing by: ', '')

print(property_list["brokerName"])

#decording the homeType column from having names to property codes i.e 1:APARTMET,2 CONDO etc, its linked to the property_type table 
#Save clean data to property table
data = [['APARTMENT', 1], ['CONDO', 2], ['MULTI_FAMILY', 3], ['SINGLE_FAMILY', 4], ['TOWNHOUSE', 5]] 

# Create the property type DataFrame 
property_type = pd.DataFrame(data, columns=['property_name', 'id']) 

#coding all property types
property_list['hdpData.homeInfo.homeType'] = property_list['hdpData.homeInfo.homeType'].str.replace('APARTMENT', '1')
property_list['hdpData.homeInfo.homeType'] = property_list['hdpData.homeInfo.homeType'].str.replace('CONDO', '2')
property_list['hdpData.homeInfo.homeType'] = property_list['hdpData.homeInfo.homeType'].str.replace('MULTI_FAMILY', '3')
property_list['hdpData.homeInfo.homeType'] = property_list['hdpData.homeInfo.homeType'].str.replace('SINGLE_FAMILY', '4')
property_list['hdpData.homeInfo.homeType'] = property_list['hdpData.homeInfo.homeType'].str.replace('TOWNHOUSE', '5')

print(property_list["hdpData.homeInfo.homeType"])
#COnvert numeric columns to int64. note data type int64 accepts null values in the data unlike int 
property_list['zpid'] = property_list['zpid'].astype('Int64') 
property_list['hdpData.homeInfo.homeType'] = property_list['hdpData.homeInfo.homeType'].astype('Int64') 
property_list['baths'] = property_list['baths'].astype('Int64') 
property_list['beds'] = property_list['beds'].astype('Int64') 
property_list['area'] = property_list['area'].astype('Int64') 

# Drop rows with missing zpid. zpid is a primary key and cannot be null in the final mysql database

property_list = property_list[property_list['zpid'].notna()]

#Renaming columns to match the names in mysql tables

property_list.rename(columns = {'hdpData.homeInfo.homeType':'property_type', 'baths':'bathrooms',
                              'beds':'bedrooms','area':'size_sqft','detailUrl':'url','brokerName':'broker_name'}, inplace = True)



# using dictionary to convert specific columns
#convert_dict = {"zpid":object,"hdpData.homeInfo.homeType":object,"price":int,"baths":'int64',"beds":int,"area":int,"detailUrl":object,"address":object,"brokerName":object
 #               }
#print(property_list["baths"])
#df = property_list.astype(convert_dict)

print(property_list["zpid"])

################################Save clean data to mysql normalised table#################################

hostname= "localhost"
database= "zillow"
username= ""
password= ""
prt=3308



engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(host=hostname, db=database, user=username, pw=password,port=prt))

#property types data
property_type.to_sql('property_type', engine, if_exists='replace', index=False)


#Save clean data to property table
property_list.to_sql('property', engine, if_exists='append', index=False)
