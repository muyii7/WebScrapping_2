#get the required libraries for the project
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

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
scraped_web(url_)