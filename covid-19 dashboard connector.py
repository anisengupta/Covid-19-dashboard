#INITIAL CONFIG
import requests
import datetime as dt
import pygsheets
import pandas as pd
import os
from bs4 import BeautifulSoup
import http.client

service_file = r"C:\Users\Aniruddha.Sengupta\Desktop\Covid-19\eastern-button-271616-6ce9bf4645df.json"

filepath = r"C:\Users\Aniruddha.Sengupta\Desktop\Covid-19"

#http.client.HTTPConnection._http_vsn = 10
#http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'

def update_covid_19_dashboard(service_file, filepath):
    #DOWNLOAD FILE FROM THE WEB
    #Get today's date
    today = dt.date.today().strftime('%Y-%m-%d')

    #Starting the download using today's date
    print('Download Starting...')

    url = f'https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-{today}.xlsx'

    r = requests.get(url)

    filename = url.split('/')[-1] # this will take only -1 splitted part of the url

    #Saving off the latest file in the project folder
    with open(filename,'wb') as output_file:
        output_file.write(r.content)

    print('Download Completed!!!')
    #-------------------------------------------------------------------------------------------------------------
    #IMPORT THE LATEST FILE AS A PD DATAFRAME AND PUTS INTO GOOGLE SHEETS
    #authorization
    gc = pygsheets.authorize(service_file=service_file)

    #Download the latest file
    data = pd.read_excel(os.path.join(filepath, f'COVID-19-geographic-disbtribution-worldwide-{today}.xlsx'))

    #Replace UK with GB
    data['geoId'].replace({'UK': 'GB'}, inplace=True)

    #Google sheets name
    gs_sheets_name = 'Latest Data on Covid-19 Cases'

    #Opens the specified Google sheet
    sh = gc.open(gs_sheets_name)

    #select the first sheet 
    wks = sh[0]

    #update the first sheet with df, starting at cell A1.
    print('Updating the Google sheets doc with timeseries data') 
    wks.clear()
    wks.set_dataframe(data,(1,1))

    #-------------------------------------------------------------------------------------------------------------
    #IMPORT RECOVERY CASE DATA AND PUTS INTO GOOGLE SHEETS
    #authorization
    gc = pygsheets.authorize(service_file=service_file)

    #Gets the recovery data url
    print('Getting the recovery data')
    recovery_data_url = 'https://www.worldometers.info/coronavirus/'

    #Puts the recovery data into a dataframe
    recovery_data = pd.read_html(recovery_data_url, header=0)[1]

    #Renames the country column
    recovery_data = recovery_data.rename(columns={'Country,Other': 'Country'})

    #Fill null values
    recovery_data = recovery_data.fillna(0)

    #Remove the total row
    recovery_data = recovery_data[:-1]

    #Google sheets name
    gs_sheets_name = 'Latest Data on Covid-19 Cases'

    #Opens the specified Google sheet
    sh = gc.open(gs_sheets_name)

    #select the second sheet 
    wks = sh[1]

    #update the first sheet with df, starting at cell A1.
    print('Updating the Google sheets doc with recovery data') 
    wks.clear()
    wks.set_dataframe(recovery_data,(1,1))

    #----------------------------------------------------------------------------------
    #IMPORT DEMOGRAPHIC DATA AND PUTS INTO GOOGLE SHEETS
    #authorization
    gc = pygsheets.authorize(service_file=service_file)

    #Gets the recovery data url
    print('Getting the fatality data')
    fatality_data_url = 'https://www.worldometers.info/coronavirus/coronavirus-age-sex-demographics/'

    #Gets the relevant tables
    fatality_age = pd.read_html(fatality_data_url, header=0)[0]
    fatality_sex = pd.read_html(fatality_data_url, header=0)[1]
    fatality_pre_condition = pd.read_html(fatality_data_url, header=0)[2]

    #Fill null values
    fatality_age = fatality_age.fillna(0)
    fatality_sex = fatality_sex.fillna(0)
    fatality_pre_condition = fatality_pre_condition.fillna(0)

    #Google sheets name
    gs_sheets_name = 'Latest Data on Covid-19 Cases'

    #Opens the specified Google sheet
    sh = gc.open(gs_sheets_name)

    #select the third sheet 
    wks_age = sh[2]
    wks_sex = sh[3]
    wks_conditions = sh[4]

    #update the first sheet with df, starting at cell A1.
    print('Updating the Google sheets doc with fatality data') 
    wks_age.clear()
    wks_age.set_dataframe(fatality_age,(1,1))

    wks_sex.clear()
    wks_sex.set_dataframe(fatality_sex,(1,1))

    wks_conditions.clear()
    wks_conditions.set_dataframe(fatality_pre_condition,(1,1))

#EXECUTION
url = 'https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide'
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')
results = str(soup.find('time')).split('"')[1].split('T', 1)[0]

website_date = dt.datetime.strptime(results, '%Y-%m-%d').date().strftime('%Y-%m-%d')
today = dt.date.today().strftime('%Y-%m-%d')

if today == website_date:
    update_covid_19_dashboard(service_file, filepath)
else:
    print('The website has not yet updated')