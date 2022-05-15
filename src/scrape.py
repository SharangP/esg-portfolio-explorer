from contextlib import contextmanager
from enum import Enum
import os
import re
import time

from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from tenacity import retry, stop_after_attempt

import ipdb

class EsgColumns(Enum):
    Environmental = 'Environmental'
    Social = 'Social'
    Governance = 'Governance'
    Unallocated = 'Unallocated'
    Score = 'Score'
    FossilFuelInvolvement = 'FossilFuelInvolvement'
    CarbonRisk = 'CarbonRisk'
    BasedOnCorporateAUM = 'BasedOnCorporateAUM'
    BasedOnAUM = 'BasedOnAUM'

    @classmethod
    def getAll(cls):
        return [col for col in cls.__members__]

FUND_URL = "https://www.morningstar.com/{type}s/xnas/{ticker}/sustainability"
ETF_URL = "https://www.morningstar.com/{type}s/arcx/{ticker}/sustainability"
class_pillar = "sal-sustainability__esg-pillar-dp"
class_key = "sal-sustainability__esg-pillar-dp-value"
class_title = "sal-sustainability__esg-pillar-dp-title"
class_score = "sal-si__flag-text"

class_fossil_fuel_involvement = "sal-carbon-metrics__metric--fossil-fuel"
class_carbon_risk = "sal-carbon-metrics__metric--carbon-risk"
class_carbon_metric_span_text = "sal-tip-text"

def classify_investment(name):
    if 'etf' in name.lower():
        return 'etf'
    return 'fund'

def new_raw_investment(ticker, name, shares, cost, _, __, ___, total):
    cleaned = {
        k:v.strip() for k, v in
        dict(ticker=ticker, name=name, shares=shares, cost=cost, total=total).items()
        if v}
    cleaned['type'] = classify_investment(name)
    return cleaned

def read_raw_portfolio(filename):
    investments = []
    with open(filename, 'r') as f:
        while line:= f.readline():
            buff = [line]
            for i in range(7): #hacky shit for US Direct Indexing
                # if buff[0] == 'US Direct Indexing\n':
                    # print(line)
                if i >= 1 and (not buff[1][0].isalpha()):
                    buff.insert(0, None)
                    pass
                else:
                    buff.append(f.readline())
            # print(buff)
            investments.append(new_raw_investment(*buff))
        return investments

def save_portfolio(df, output_file):
    print("saving to {}".format(output_file))
    df.to_csv(output_file, index=False)

def load_portfolio(input_file, output_file, force_raw=False):
    if not force_raw and os.path.exists(output_file):
        return pd.read_csv(output_file)

    portfolio = pd.DataFrame(read_raw_portfolio(input_file))
    portfolio['total'] = portfolio['total'].apply(lambda x: float(x.replace(',','').replace('$','')))
    portfolio['cost'] = portfolio['cost'].apply(lambda x: float(x.replace(',','').replace('$','')))
    portfolio['shares'] = portfolio['shares'].astype(float)
    save_portfolio(portfolio, output_file)
    return portfolio

@contextmanager
def chrome_driver():
    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument("window-size=1920x1080")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage') # Not used but can be an option
    driver = webdriver.Chrome(options=options)

    try:
        yield driver
    finally:
        driver.quit()

def get_ticker_url_options(investment):
    if not investment['ticker']:
        raise Exception('not a ticker')

    if investment['type'] == 'etf':
        order = [ETF_URL, FUND_URL]
    elif investment['type'] == 'fund':
        order = [FUND_URL, ETF_URL]

    for url in order:
        yield url.format(type=investment['type'], ticker=investment['ticker'])

@retry(stop=stop_after_attempt(3))
def scrape_site(driver, sample_url):
    print("scraping {}".format(sample_url))
    driver.get(sample_url)

    time.sleep(5)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")

    src = driver.page_source
    parser = BeautifulSoup(src, "html.parser")
    return src, parser

def get_esg_info_from_parser(parser, url):
    esg_info = {k:None for k in EsgColumns.getAll()}
    errors = []
    try:
        for x in parser.find_all('div', {'class': class_pillar}):
            t = x.find('div', {'class': class_title}).getText().strip()
            v = x.find('div', {'class': class_key}).getText().strip()
            esg_info[t] = v

        score = parser.find_all('span', {'class': class_score})[0].getText().strip()
        esg_info[EsgColumns.Score.value] = score

    except Exception as e:
        errors.append(e)

    try:
        esg_info[EsgColumns.FossilFuelInvolvement.value] = parser.find(
            'div', {'class': class_fossil_fuel_involvement}
            ).find('span', {'class': class_carbon_metric_span_text}).getText().strip()
    except Exception as e:
        errors.append(e)

    try:
        esg_info[EsgColumns.CarbonRisk.value] = parser.find(
            'div', {'class': class_carbon_risk}
            ).find('span', {'class': class_carbon_metric_span_text}).getText().strip()
    except Exception as e:
        errors.append(e)

    try:
        for group in re.findall(
            '[B|b]ased\s*on\s*(\d+\.?\d+?)\%\s*of(\s*Corporate)?\s*AUM', parser.text):
            key = 'BasedOn{}AUM'.format(group[1].strip())
            esg_info[key] = group[0]
    except Exception as e:
        errors.append(e)

    if len(errors) < 3:
        return esg_info
    else:
        raise Exception(e)

def get_esg_info_dict(driver, investment, refresh=False):
    #try only if we don't have all the esg info for this ticker already
    already_there = []
    for e in EsgColumns.getAll():
        already_there.append(e in investment and investment[e] not in {None, ''}
            and pd.notna(investment[e]))

    if all(already_there) and not refresh:
        return investment[EsgColumns.getAll()].to_dict()

    print("getting esg info for {}".format(investment['ticker']))

    scraping_exceptions = []
    for url in get_ticker_url_options(investment):
        try:
            _, parser = scrape_site(driver, url)
            esg_info = get_esg_info_from_parser(parser, url)
            break
        except Exception as e:
            scraping_exceptions.append(e)

    #if there are issues every attempt and you can't even get 4 esg items, drop it
    if len(scraping_exceptions) > 1:
        print(scraping_exceptions)

    return esg_info


if __name__ == '__main__':
    raw_portfolio = './portfolio.txt'
    output_portfolio = './output_portfolio.csv'
    # output_portfolio = './short_portfolio.csv'

    portfolio = load_portfolio(raw_portfolio, output_portfolio, force_raw=False)
    portfolio = portfolio[portfolio['ticker'].notna()]

    with chrome_driver() as driver:
        esg_info = portfolio.apply(lambda x: get_esg_info_dict(driver, x), axis=1)
        esg_info_df = pd.DataFrame(esg_info.tolist(), columns=EsgColumns.getAll())
        for column in EsgColumns.getAll():
            portfolio[column] = esg_info_df[column]

        save_portfolio(portfolio, output_portfolio)

#TODO: add individual equities
