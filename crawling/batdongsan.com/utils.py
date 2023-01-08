import time
import random

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
