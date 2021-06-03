# prepare driver
# get all MF page links
# iterate over each page to get details and add it in the list of dict
# DONE: missed_list = []; surround with try catch, if misses append in missed_list and try again later
# DONE: do 20 requests and wait for 60 secs and then try again or let it SLEEP for 3 sec between each request
# DONE: bloom filter can be implemented

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from datetime import datetime
import pickle
import os
from bloom_filter import BloomFilter
import numpy as np

PRIMARY_PAGE = "https://coin.zerodha.com/funds"
options = Options()
options.headless = True

def get_primary_page(link):
    driver.get(link)
    time.sleep(3)
    link_tags = driver.find_elements_by_tag_name('a')
    hrefs = []
    for tag in link_tags:
        if "cursor-pointer" not in tag.get_attribute('class'):
            continue
        hrefs.append(tag.get_attribute('href'))
    print(len(hrefs))
    return hrefs


def summary(link):
    driver = webdriver.Chrome(options=options, executable_path="chromedriver.exe")

    temp = {}
    # get page
    driver.get(link)
    time.sleep(3)
    # MF Name
    mf_name = driver.find_elements_by_xpath("//h2[contains(@class, 'end')]")[0].text
    temp["mf_name"] = mf_name

    # Current Price
    curr_price = driver.find_elements_by_xpath("//span[contains(@class, 'fund_current_price')]")[0].text
    temp["curr_price"] = curr_price

    # get CAGR
    cagr = driver.find_elements_by_xpath("//div[contains(@class, 'units-row mobile-center returns-each')]")
    for tag in cagr:
        year = tag.find_elements_by_tag_name('h6')[0].get_attribute('innerHTML')
        returns = tag.find_elements_by_tag_name('div')[0].text
        temp[year] = returns

    # Other Details
    min_inv = driver.find_elements_by_xpath("//li[contains(@class, 'unit-20 fund-bottom-section')]")[:4]
    for tag in min_inv:
        attr_arr = tag.text.split("\n")
        temp[attr_arr[0]] = attr_arr[1]

    # Document link
    doc_link = driver.find_elements_by_xpath("//div[contains(@class, 'units-row end text-center amc-link')]")[
        0].find_elements_by_tag_name('a')[0].get_attribute('href')
    temp["doc_link"] = doc_link
    temp["link"] = link

    driver.quit()
    return temp


def format_df(df):
    if df.empty:
        return pd.DataFrame()
    df["curr_price_val"] = df["curr_price"].apply(lambda x: x.split(" ")[1]).astype(float)
    df["growth"] = df["curr_price"].apply(lambda x: x.split(" ")[2][1:-2]).astype(float)
    df["launch_date"] = df["Launch date"].apply(lambda x: datetime.strptime(x, '%d-%m-%Y'))
    df["1 year(%)"] = df["1 year:"].apply(lambda x: float(x[:-1]) if isinstance(x, str) else np.NAN)
    df["3 year(%)"] = df["3 years:"].apply(lambda x: float(x[:-1]) if isinstance(x, str) else np.NAN)
    df["5 year(%)"] = df["5 years:"].apply(lambda x: float(x[:-1]) if isinstance(x, str) else np.NAN)
    df["exit_load(%)"] = df["Exit load"].apply(lambda x: float(x[:-1]) if isinstance(x, str) else np.NAN)
    df["min_investment"] = df["Minimum investment"].apply(lambda x: x.split(" ")[1].replace(",", "")).astype(float)
    df["document_link"] = df["doc_link"]
    df = df.drop(columns=['curr_price', '1 year:', '3 years:', '5 years:', 'Launch date', 'Exit load',
                          'Minimum investment', 'doc_link'])
    return df


driver = webdriver.Chrome(options=options, executable_path="chromedriver.exe")
if os.path.isfile("primary_page_link.pkl"):
    outfile = open('primary_page_link.pkl', 'rb')
    hrefs_arr = pickle.load(outfile)
else:
    hrefs_arr = get_primary_page(PRIMARY_PAGE)
    outfile = open("primary_page_link.pkl", 'wb')
    pickle.dump(hrefs_arr, outfile)
    outfile.close()
driver.quit()

batch = hrefs_arr[:30]

df_dict = []
MISSED = []

# ###################### START ######################

if os.path.isfile("link_bloom.pkl"):
    outfile = open('link_bloom.pkl', 'rb')
    bloom = pickle.load(outfile)
else:
    bloom = BloomFilter(max_elements=50000, error_rate=0.0000001)

for link in batch:
    if link not in bloom:
        try:
            result = summary(link)
            print(link, ": DONE!!!")
            df_dict.append(result)
            bloom.add(link)
        except Exception as e:
            print(link, ": MISSED!!!", e)
            MISSED.append(link)

outfile = open("link_bloom.pkl", 'wb')
pickle.dump(bloom, outfile)
outfile.close()
print(MISSED)
if os.path.isfile("output.csv"):
    df = pd.read_csv("output.csv")
else:
    df = pd.DataFrame()

temp_df = format_df(pd.DataFrame(df_dict))
df = df.append(temp_df)
df.to_csv("output.csv", index=False)

