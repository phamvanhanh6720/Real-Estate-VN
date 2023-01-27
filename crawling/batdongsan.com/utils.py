import time
import random
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
from bs4 import BeautifulSoup


def scroll_down(driver, max_sleep_time: int):
    time.sleep(random.randint(0, max_sleep_time))
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
    time.sleep(random.randint(0, max_sleep_time))
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")


def crawl_each_news_item(driver, url, max_sleep_time, news_data: dict):
    driver.get(url)
    scroll_down(driver, int(max_sleep_time / 2))
    html = driver.page_source
    soup_item = BeautifulSoup(html, features="html.parser")

    detail_loc_element = soup_item.find('span', class_='re__pr-short-description js__pr-address')
    detail_location = detail_loc_element.text if detail_loc_element else None
    if 'Dự án' in detail_location:
        detail_location = ','.join(detail_location.split(',')[1:])
    news_data['detail_location'] = detail_location

    description_element = soup_item.find('div', class_='re__section-body re__detail-content js__section-body js__pr-description js__tracking')
    text_description = description_element.text if description_element else None
    news_data['description'] = text_description

    no_bedroom = None
    for element in soup_item.find_all('div', class_='re__pr-short-info-item js__pr-short-info-item'):
        if element.find('span', class_='title').text == 'Phòng ngủ':
            no_bedroom = element.find('span', class_='value').text
            no_bedroom = int(no_bedroom.strip(' PN'))

    if news_data['no_bedroom'] is None:
        news_data['no_bedroom'] = no_bedroom

    published_by_element = soup_item.find('div', class_='re__contact-name js_contact-name')
    published_by = published_by_element.get('title') if published_by_element else None

    phone_number_element = soup_item.find('span', class_='phoneEvent js__phone-event')
    phone_number = phone_number_element.text.strip('\n ').strip(' · Hiện số') if phone_number_element else None

    news_data['published_by'] = published_by
    news_data['phone_number'] = phone_number


def parser_log(log_file):
    with open(log_file, 'r') as file:
        data_log_lines = file.readlines()

    data_log_lines = [data.strip('\n') for data in data_log_lines]
    data_log_lines = [data for data in data_log_lines if data != '']

    real_estate_type_count = {
        'Done': {},
        'Already crawled': {},
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
