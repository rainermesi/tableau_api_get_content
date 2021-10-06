### libraries

import requests
from getpass import getpass
import json
import math
import secrets
import sys
import time
import os

### variables

usr = input('Tableau usr:')
pwd = getpass('Tableau pwd:')
contentUrl = '' # tableau site name
site = ''
session = ''
host = '' # server url
api_version = '3.9'
pagesize = 100
headers = {'Accept':'application/json','Content-Type':'application/json'}

### session

def GernerateSession():
    global session
    session = secrets.token_urlsafe(16)
    
def LogIn():

    global site

    data = '{"credentials": {"name":"'+usr+'","password":"'+pwd+'","site": {"contentUrl":"'+contentUrl+'"}}}'
    path = '/api/'+api_version+'/auth/signin'
    url = host+path

    response = requests.post(url, data=data,headers=headers, verify=False)
    print('Login code: '+str(response.status_code))
    print('Login data: '+str(response.text))

    json_data = json.loads(response.text)

    try:
        site = json_data['credentials']['site']['id']
        user = json_data['credentials']['user']['id']
        token = json_data['credentials']['token']

    except KeyError:
        sys.exit(1)

    TokenHeader(token)
    
def TokenHeader(token):
    headers['X-Tableau-Auth'] = token
    
def LogOut():

    path = '/api/'+api_version+'/auth/signout'
    url = host+path

    response = requests.post(url, headers=headers, verify=False)

    print('Logout code: '+str(response.status_code))

    logging.info('SESSION:'+session+', logout code:'+str(response.status_code))


### misc helpers

def cleanFilename(sourcestring,  removestring ="%:/,\\[]<>*?"):
    #remove undesireable characters
    return ''.join([c for c in sourcestring if c not in removestring])

notebook_path = os.path.abspath('')
wb_output_path = os.path.join(notebook_path,'output/workbooks')
ds_output_path = os.path.join(notebook_path,'output/datasources')
os.mkdir(wb_output_path)
os.mkdir(ds_output_path)

### api calls

#get worbooks and datasources

def GetWorkbooks():
    run_start = time.perf_counter()
    path = '/api/'+api_version+'/sites/'+site+'/workbooks'
    url = host+path
    
    response = requests.get(url,headers=headers,verify=False)
    json_data = json.loads(response.text)
    
    total = json_data['pagination']['totalAvailable']
    repeats = math.ceil(int(total)/int(pagesize))
                     
    raw_wb_list = []
    output_list = []
    
    for i in range(repeats):
        new_path =  '/api/'+api_version+'/sites/'+site+'/workbooks?fields=name,id&pageSize='+str(pagesize)+'&pageNumber='+str(i+1)
        url_iter = host+new_path
        response_iter = requests.get(url_iter, headers=headers, verify=False)
        json_data_iter = json.loads(response_iter.text)
        raw_wb_list.append(json_data_iter)
    
    for page in range(len(raw_wb_list)):
        for i in raw_wb_list[page]['workbooks']['workbook']:
            output_list.append([i['name'],i['id']])
    
    run_end = time.perf_counter()
    
    print(f'N repeats: {repeats}')
    print(f'Runtime: {run_end - run_start:0.4f} seconds')
        
    return output_list

def GetDatasources():
    run_start = time.perf_counter()
    path = '/api/'+api_version+'/sites/'+site+'/datasources'
    url = host+path
    
    response = requests.get(url,headers=headers,verify=False)
    json_data = json.loads(response.text)
    
    total = json_data['pagination']['totalAvailable']
    repeats = math.ceil(int(total)/int(pagesize))
                     
    raw_ds_list = []
    output_list = []
    
    for i in range(repeats):
        new_path =  '/api/'+api_version+'/sites/'+site+'/datasources?fields=name,id&pageSize='+str(pagesize)+'&pageNumber='+str(i+1)
        url_iter = host+new_path
        response_iter = requests.get(url_iter, headers=headers, verify=False)
        json_data_iter = json.loads(response_iter.text)
        raw_ds_list.append(json_data_iter)
    
    for page in range(len(raw_ds_list)):
        for i in raw_ds_list[page]['datasources']['datasource']:
            output_list.append([i['name'],i['id']])
    
    run_end = time.perf_counter()
    
    print(f'N repeats: {repeats}')
    print(f'Runtime: {run_end - run_start:0.4f} seconds')
        
    return output_list

# download workbooks and datasources

def DowloadWorkbook(wb_id):
    run_start = time.perf_counter()
    path = '/api/'+api_version+'/sites/'+site+'/workbooks/'+wb_id+'/content?includeExtract=false'
    url = host+path
    
    response = requests.get(url,headers=headers,verify=False)
    
    if response.headers.get('Content-Type') == 'application/octet-stream':
        workbook_id = str(wb_id)+'.twbx'
    else:
        workbook_id = str(wb_id)+'.twb'
        
    #json_data = response
    run_end = time.perf_counter()
    runtime = run_end - run_start
    return response, workbook_id

def DownloadDatasource(ds_id):
    run_start = time.perf_counter()
    path = '/api/'+api_version+'/sites/'+site+'/datasources/'+ds_id+'/content?includeExtract=false'
    url = host+path
    
    response = requests.get(url,headers=headers,verify=False)
    
    if response.headers.get('Content-Type') == 'application/octet-stream':
        datasource_id = str(ds_id)+'.tdsx'
    else:
        datasource_id = str(ds_id)+'.tds'
        
    #json_data = response
    run_end = time.perf_counter()
    runtime = run_end - run_start
    return response, datasource_id

# write files to disk

def WriteWorkbookToDisk(wb, workbook_id):
    filename = str(workbook_id)
    with open(fr'{wb_output_path}'+filename, 'wb') as file:
        file.write(wb.content)
        
def WriteDatasourceToDisk(ds,datasource_id):
    filename = str(datasource_id)
    with open(fr'{ds_output_path}'+filename, 'wb') as file:
        file.write(ds.content)

### run api calls

# datasources
GernerateSession()

LogIn()
ds_list_obj = GetDatasources()

run_start = time.perf_counter()
for i in ds_list_obj:
    response = DownloadDatasource(i[1])
    ds = response[0]
    ds_id = response[1]
    WriteDatasourceToDisk(ds,ds_id)
run_end = time.perf_counter()
runtime = run_end - run_start
LogOut()
print('Runtime: '+str(runtime))

# workbooks
GernerateSession()
LogIn()
wb_list_obj = GetWorkbooks()

run_start = time.perf_counter()
for i in wb_list_obj:
    response = DowloadWorkbook(i[1])
    wb = response[0]
    wb_id = response[1]
    WriteWorkbookToDisk(wb,wb_id)
run_end = time.perf_counter()
runtime = run_end - run_start
LogOut()
print('Runtime: '+str(runtime))

