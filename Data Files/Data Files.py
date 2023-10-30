#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os


# In[2]:


os.chdir("D:\SCHULE\DecipheringBigData\e-portfolio\Data Files")


# In[3]:


os.getcwd()


# In[6]:


import csv

csvfile = open('data-text.csv', 'r')
reader = csv.reader(csvfile)

for row in reader:
    print(row)


# In[8]:


import csv

csvfile = open('data-text.csv', 'r')
reader = csv.DictReader(csvfile)

for row in reader:
    print(row)


# In[9]:


import json

json_data = open('data-text.json').read()

data = json.loads(json_data)

for item in data:
    print(item)


# In[18]:


from xml.etree import ElementTree as ET

tree = ET.parse('data-text.xml')
root = tree.getroot()
print(root)

data = root.find('Data')

all_data = []

for observation in data:
    record = {}
    for item in observation:

        lookup_key = item.attrib.keys()[0]

        if lookup_key == 'Numeric':
            rec_key = 'NUMERIC'
            rec_value = item.attrib['Numeric']
        else:
            rec_key = item.attrib[lookup_key]
            rec_value = item.attrib['Code']

        record[rec_key] = rec_value
    all_data.append(record)

print(all_data)

