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
from selenium.webdriver.support.ui import Select
import sys

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
    # cagr = driver.find_elements_by_xpath("//div[contains(@class, 'units-row mobile-center returns-each')]")
    # for tag in cagr:
    #     year = tag.find_elements_by_tag_name('h6')[0].get_attribute('innerHTML')
    #     returns = tag.find_elements_by_tag_name('div')[0].text
    #     temp[year] = returns
    select = Select(driver.find_element_by_xpath("//select[contains(@ng-model, 'cagr_select')]"))
    options_dropdown = select.options
    for index, year in zip(range(len(options_dropdown)), ["1 year(%)", "3 year(%)", "5 year(%)"]):
        select.select_by_index(index)
        temp[year] = driver.find_element_by_xpath("//span[contains(@class, 'returns-value')]").text

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
    df["curr_price_val"] = df["curr_price"].apply(lambda x: x.split(" ")[1].replace(",", "")).astype(float)
    df["growth"] = df["curr_price"].apply(lambda x: x.split(" ")[2].replace(",", "")[1:-2]).astype(float)
    df["launch_date"] = df["Launch date"].apply(lambda x: datetime.strptime(x, '%d-%m-%Y'))
    df["1 year(%)"] = df["1 year:"].apply(lambda x: float(x[:-1].replace(",", "")) if isinstance(x, str) else np.NAN)
    df["3 year(%)"] = df["3 years:"].apply(lambda x: float(x[:-1].replace(",", "")) if isinstance(x, str) else np.NAN)
    df["5 year(%)"] = df["5 years:"].apply(lambda x: float(x[:-1].replace(",", "")) if isinstance(x, str) else np.NAN)
    df["exit_load(%)"] = df["Exit load"].apply(lambda x: float(x[:-1].replace(",", "")) if isinstance(x, str) else np.NAN)
    df["min_investment"] = df["Minimum investment"].apply(lambda x: x.split(" ")[1].replace(",", "")).astype(float)
    df["document_link"] = df["doc_link"]
    df = df.drop(columns=['curr_price', '1 year:', '3 years:', '5 years:', 'Launch date', 'Exit load',
                          'Minimum investment', 'doc_link'])
    return df


def dict_formatter(temp):
    temp_ = {}
    # try:
    curr_price_list = temp["curr_price"].split(" ")
    temp_["mf_name"] = temp["mf_name"]
    temp_["curr_price"] = float(curr_price_list[1].replace(",", ""))
    temp_["growth(%)"] = float(curr_price_list[2].replace(",", "")[1:-2])
    temp_["1 year(%)"] = float(temp["1 year(%)"][:-1].replace(",", "")) if isinstance(temp["1 year(%)"], str) else np.NAN
    temp_["3 year(%)"] = float(temp["3 year(%)"][:-1].replace(",", "")) if isinstance(temp["3 year(%)"], str) else np.NAN
    temp_["5 year(%)"] = float(temp["5 year(%)"][:-1].replace(",", "")) if isinstance(temp["5 year(%)"], str) else np.NAN
    temp_["launch_date"] = datetime.strptime(temp["Launch date"], '%d-%m-%Y')
    temp_["exit_load(%)"] = float(temp["Exit load"][:-1].replace(",", "")) if isinstance(temp["Exit load"], str) and temp["Exit load"] != "None" else np.NAN
    temp_["min_investment"] = float(temp["Minimum investment"].split(" ")[1].replace(",", ""))
    temp_["Last dividend payout"] = temp["Last dividend payout"]
    temp_["document_link"] = temp["doc_link"]
    temp_["link"] = temp["link"]
    del temp
    # except Exception as e:
    #     print(e)
    return temp_


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

batch = hrefs_arr[:50]

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
            result = dict_formatter(result)
            if result:
                df_dict.append(result)
                bloom.add(link)
                print(link, ": DONE!!!")
            else:
                print(link, ": MISSED in Formatting!!!")
                MISSED.append(link)
        except Exception as e:
            print(link, ": MISSED!!!", e)
            MISSED.append(link)
    else:
        print("Bloom Hit!!!")

outfile = open("link_bloom.pkl", 'wb')
pickle.dump(bloom, outfile)
outfile.close()
print(MISSED)
if os.path.isfile("output.csv"):
    df = pd.read_csv("output.csv")
else:
    df = pd.DataFrame()

temp_df = pd.DataFrame(df_dict)
df = df.append(temp_df)
df.to_csv("output.csv", index=False)
