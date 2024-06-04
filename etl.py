#get the required libraries for the project
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import time

#the marks and spencer url where the data will be scrapped from:
url_ = 'https://www.marksandspencer.com/l/men/mens-blazers?page='

'''the scraped_web(url) function is used to scrape the marks and spencer's website (with different pages) for data relating to 
men's blazer on different web pages.'''
def scraped_web(url):
    urls = []
    numbers = [str(i) for i in range(1,5)] #the page number is converted to string
    for number in numbers:
        url_in_use = url+number #page number is appended to the static url address
        urls.append(url_in_use)
    scrapped_data = []
    for urlx in urls:
        response = requests.get(urlx) #the requests.get() helps determines the response if scrapping will be possible.
        soups = BeautifulSoup(response.text,'lxml')
        scrapped_data.append(soups) #the scrapped html data is appended into a list
    return scrapped_data

def extract_data():
    # an empty dataframe that will be used to append the extracted data.
    df = pd.DataFrame({'price_of_item': [''], 'designer': [''], 'product_description': ['']})
    file_name = datetime.now().strftime('%Y%m%dT%H%M') #file name format
    soups = scraped_web(url_) #the scraped data is read from the scraped_web(url)
    try:
        # Iterate through scrapped data, generate dataframes and combine into a unified dataframe.
        for soup in soups:
            prices = soup.find_all('span', class_ = 'media-0_textSm__Q52Mz price_original__Pu1V8')
            price = [item.text for item in prices] #the data for price is stacked.
            designers = soup.find_all('span', class_ = 'media-0_textXs__ZzHWu product-card_brand__FdfAD')
            designer = [item.text for item in designers] #the data for product designer is stacked.
            descriptions = soup.find_all('h2', class_='media-0_textSm__Q52Mz product-card_title__gA6_B')
            description = [item.text for item in descriptions] #the data for description is stacked.
            df = df._append({'price_of_item': price, 'designer': designer, 'product_description': description},ignore_index=True)
            df_ex = df.explode(['designer','price_of_item','product_description'])
                        
    except SyntaxWarning:
        print('data is not availabe')
    #extracted data is written in the local repository called 'staging/' used as a dataLake
    df_ex.to_csv(f'staging/{file_name}.csv', index=False)
    print('file has been staged in dataLake')
extract_data()