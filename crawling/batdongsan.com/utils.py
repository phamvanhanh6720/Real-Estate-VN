import time
import random
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
from bs4 import BeautifulSoup


import multiprocessing.pool
import functools


def timeout(max_timeout):
    """Timeout decorator, parameter in seconds."""
    def timeout_decorator(item):
        """Wrap the original function."""
        @functools.wraps(item)
        def func_wrapper(*args, **kwargs):
            """Closure for function."""
            pool = multiprocessing.pool.ThreadPool(processes=1)
            async_result = pool.apply_async(item, args, kwargs)
            # raises a TimeoutError if execution exceeds max_timeout
            return async_result.get(max_timeout)
        return func_wrapper
    return timeout_decorator


def scroll_down(driver, max_sleep_time: int):
    # time.sleep(random.randint(0, max_sleep_time))
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.randint(0, max_sleep_time))


@timeout(10)
def get_main_page(driver, url):
    driver.get(url)


# @timeout(7)
def crawl_each_news_item(driver, url, max_sleep_time, news_data: dict):

    get_main_page(driver, url)
    scroll_down(driver, int(max_sleep_time))
    html = driver.page_source
    soup_item = BeautifulSoup(html, features="html.parser")

    # location
    detail_loc_element = soup_item.find('span', class_='re__pr-short-description js__pr-address')
    detail_location = detail_loc_element.text  # if detail_loc_element else None
    if 'Dự án' in detail_location:
        detail_location = ','.join(detail_location.split(',')[1:])

    # description
    description_element = soup_item.find('div', class_='re__section-body re__detail-content js__section-body js__pr-description js__tracking')
    text_description = description_element.text  # if description_element else None

    # number of bedroom, price, price per m2, area=
    no_bedroom = None
    price_per_m2 = None
    price = None
    area = None

    short_info_items = soup_item.find_all('div', class_='re__pr-short-info-item js__pr-short-info-item')
    for element in short_info_items:
        if element.find('span', class_='title').text == 'Phòng ngủ':
            no_bedroom = element.find('span', class_='value')
            if no_bedroom is not None:
                no_bedroom = no_bedroom.text
                no_bedroom = int(no_bedroom.strip(' PN'))

        if element.find('span', class_='title').text == 'Mức giá':
            price = element.find('span', class_='value').text
            price_per_m2 = element.find('span', class_='ext')
            if price_per_m2 is not None:
                price_per_m2 = price_per_m2.text.strip('~')

            if 'triệu/m²' in price:
                temp_value = price
                price = price_per_m2
                price_per_m2 = temp_value

            if price is not None:
                if 'tỷ' in price:
                    price = float(price.strip(' tỷ').replace(',', '.'))
                elif 'triệu' in price:
                    print(price)
                    price = float(price.strip(' triệu').replace(',', '.')) / 1000

            if price_per_m2 is not None:
                price_per_m2 = price_per_m2.replace('~', '').replace(',', '.')
                price_per_m2 = price_per_m2.strip(' triệu/m²')

        if element.find('span', class_='title').text == 'Diện tích':
            area = element.find('span', class_='value')
            if area is not None:
                area = area.text
                area = float(area.strip(' m²').replace(',', '.'))

    # number of toilet
    no_toilet = None
    detail_info_items = soup_item.find_all('div', class_='re__pr-specs-content-item')
    if detail_info_items is not None:
        for ele in soup_item.find_all('div', class_='re__pr-specs-content-item'):
            if ele.find('span', class_='re__pr-specs-content-item-title').text == 'Số toilet':
                no_toilet = ele.find('span', class_='re__pr-specs-content-item-value').text
                no_toilet = int(no_toilet.strip(' phòng'))

    # project
    project = None
    if '-prj-' in url:
        project = url.split('-prj-')[-1].split('/')[0]

    # title
    title = soup_item.find('h1', class_='re__pr-title pr-title js__pr-title').text

    # information about publisher
    published_by_element = soup_item.find('div', class_='re__contact-name js_contact-name')
    published_by = published_by_element.get('title')  # if published_by_element else None

    phone_number_element = soup_item.find('div', class_='re__btn re__btn-cyan-solid--md phone js__phone phoneEvent js__phone-event').find('span')
    phone_number = phone_number_element.text.strip('\n ').strip(' · Hiện số')  # if phone_number_element else None

    news_data['published_by'] = published_by
    news_data['phone_number'] = phone_number
    news_data['no_bedroom'] = no_bedroom
    news_data['price'] = price
    news_data['price_per_m2'] = price_per_m2
    news_data['area'] = area
    news_data['detail_location'] = detail_location
    news_data['title'] = title
    news_data['project'] = project
    news_data['no_toilet'] = no_toilet
    news_data['description'] = text_description

    return news_data


def parser_log(log_file):
    with open(log_file, 'r') as file:
        data_log_lines = file.readlines()

    data_log_lines = [data.strip('\n') for data in data_log_lines]
    data_log_lines = [data for data in data_log_lines if data != '']

    real_estate_type_count = {
        'Fetch raw': {},
        'Already exist': {},
        'Fail': {}
        }
    for data in data_log_lines:
        items = data.split(' - ')
        if len(items) != 3:
            continue
        else:
            real_estate_type = items[1]
            status = items[2].split(':')[0].strip()

            if real_estate_type not in real_estate_type_count[status].keys():
                real_estate_type_count[status][real_estate_type] = 1
            else:
                real_estate_type_count[status][real_estate_type] += 1

    start_time = data_log_lines[0].split(' - ')[0]
    end_time = data_log_lines[-1].split(' - ')[0]
    total_time = (datetime.strptime(end_time, '%d-%b-%y %H:%M:%S') - datetime.strptime(start_time, '%d-%b-%y %H:%M:%S')).seconds / 3600

    return real_estate_type_count, round(total_time, 2)


def create_visualize_figure(counting_reports, save_file):
    df = pd.DataFrame.from_dict(counting_reports)
    figure = df.plot.barh().get_figure()
    figure.savefig(save_file, bbox_inches='tight', dpi=100)
