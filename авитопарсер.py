import requests
from bs4 import BeautifulSoup
import time
import schedule
from datetime import datetime
import sqlite3
from urllib.parse import urljoin
import re

# Настройки
AVITO_BASE_URL = "https://www.avito.ru"
REGION = "moskva"  # Измените на нужный регион
CATEGORY = "transport"  # Категория (например: транспорт, недвижимость и т.д.)
SUBCATEGORY = "avtomobili"  # Подкатегория (например: автомобили)
DB_NAME = "avito_ads.db"

# Подключение к базе данных
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        avito_id TEXT UNIQUE,
        title TEXT,
        price TEXT,
        description TEXT,
        address TEXT,
        seller_name TEXT,
        seller_type TEXT,
        publication_date TEXT,
        url TEXT,
        parse_date TEXT,
        phone TEXT,
        attributes TEXT
    )
    ''')
    
    conn.commit()
    conn.close()

# Функция для получения HTML страницы
def get_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Ошибка при запросе страницы: {e}")
        return None

# Парсер списка объявлений
def parse_ads_list(category, subcategory, region):
    url = f"{AVITO_BASE_URL}/{region}/{category}/{subcategory}"
    print(f"Парсинг списка объявлений: {url}")
    
    html = get_page(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    ads = []
    
    items = soup.find_all('div', {'data-marker': 'item'})
    
    for item in items:
        try:
            title_elem = item.find('h3', {'itemprop': 'name'})
            title = title_elem.text.strip() if title_elem else 'No title'
            
            price_elem = item.find('meta', {'itemprop': 'price'})
            price = price_elem['content'] if price_elem else 'No price'
            
            url_elem = item.find('a', {'data-marker': 'item-title'})
            ad_url = urljoin(AVITO_BASE_URL, url_elem['href']) if url_elem else None
            
            if ad_url:
                ads.append({
                    'title': title,
                    'price': price,
                    'url': ad_url
                })
        except Exception as e:
            print(f"Ошибка при парсинге элемента списка: {e}")
    
    return ads

# Парсер страницы объявления
def parse_ad_page(url):
    print(f"Парсинг объявления: {url}")
    html = get_page(url)
    if not html:
        return None
    
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        # Получаем ID объявления из URL
        avito_id = re.search(r'(\d+)$', url).group(1)
        
        # Заголовок
        title = soup.find('h1', {'class': 'title-info-title'}).text.strip()
        
        # Цена
        price = soup.find('span', {'class': 'js-item-price'}).text.strip()
        
        # Описание
        description_elem = soup.find('div', {'class': 'item-description'})
        description = description_elem.text.strip() if description_elem else 'No description'
        
        # Адрес
        address_elem = soup.find('span', {'class': 'item-address__string'})
        address = address_elem.text.strip() if address_elem else 'No address'
        
        # Продавец
        seller_name_elem = soup.find('div', {'class': 'seller-info-name'})
        seller_name = seller_name_elem.text.strip() if seller_name_elem else 'No seller name'
        
        # Тип продавца (частник/компания)
        seller_type_elem = soup.find('div', {'class': 'seller-info-value'})
        seller_type = seller_type_elem.text.strip() if seller_type_elem else 'No seller type'
        
        # Дата публикации
        date_elem = soup.find('div', {'class': 'title-info-metadata-item-redesign'})
        publication_date = date_elem.text.strip() if date_elem else 'No date'
        
        # Телефон (если доступен)
        phone = 'Not available'  # На Avito обычно требуется клик для получения телефона
        
        # Атрибуты (характеристики)
        attributes = {}
        attrs_elems = soup.find_all('li', {'class': 'item-params-list-item'})
        for attr in attrs_elems:
            try:
                key = attr.find('span', {'class': 'item-params-label'}).text.strip()
                value = attr.text.replace(key, '').strip()
                attributes[key] = value
            except:
                continue
        
        return {
            'avito_id': avito_id,
            'title': title,
            'price': price,
            'description': description,
            'address': address,
            'seller_name': seller_name,
            'seller_type': seller_type,
            'publication_date': publication_date,
            'url': url,
            'phone': phone,
            'attributes': str(attributes)  # Сохраняем как строку
        }
    except Exception as e:
        print(f"Ошибка при парсинге страницы объявления: {e}")
        return None

# Сохранение объявления в БД
def save_ad_to_db(ad_data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT OR IGNORE INTO ads (
            avito_id, title, price, description, address, seller_name, 
            seller_type, publication_date, url, parse_date, phone, attributes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            ad_data['avito_id'],
            ad_data['title'],
            ad_data['price'],
            ad_data['description'],
            ad_data['address'],
            ad_data['seller_name'],
            ad_data['seller_type'],
            ad_data['publication_date'],
            ad_data['url'],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ad_data['phone'],
            ad_data['attributes']
        ))
        
        conn.commit()
        print(f"Объявление {ad_data['avito_id']} сохранено в БД")
    except sqlite3.Error as e:
        print(f"Ошибка при сохранении в БД: {e}")
    finally:
        conn.close()

# Основная функция парсинга
def parse_avito():
    print(f"Начало парсинга в {datetime.now()}")
    
    # Получаем список объявлений
    ads_list = parse_ads_list(CATEGORY, SUBCATEGORY, REGION)
    
    # Парсим каждое объявление и сохраняем в БД
    for ad in ads_list:
        ad_data = parse_ad_page(ad['url'])
        if ad_data:
            save_ad_to_db(ad_data)
    
    print(f"Парсинг завершен в {datetime.now()}")

# Запуск парсера по расписанию
def run_scheduler():
    print("Парсер запущен. Ожидание новых объявлений...")
    schedule.every(1).minutes.do(parse_avito)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    init_db()
    parse_avito()  # Запустить сразу при старте
    run_scheduler()
