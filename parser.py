import requests
from bs4 import BeautifulSoup
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import os
from utils import ensure_dir, filename_ru
from datetime import datetime
import traceback

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; HalykParser/1.0)"}

def check_proxy(proxy, site_url="https://halykmarket.kz", timeout=8):
    '''
    Проверяет одну проксию. proxy в формате host:port или со схемой http://user:pass@host:port
    Возвращает True/False.
    '''
    proxies = {"http": proxy, "https": proxy}
    try:
        r = requests.get(site_url, headers=HEADERS, proxies=proxies, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False

def rotate_proxies(proxy_list):
    if not proxy_list:
        yield None
    else:
        i = 0
        n = len(proxy_list)
        while True:
            yield proxy_list[i % n]
            i += 1

def parse_product_card(html):
    '''
    Базовый парсинг карточки товара. Возвращает словарь полей.
    Для категорий 'Ювелирные украшения' добавляются специфичные поля.
    '''
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    # Title
    t = soup.select_one("h1")
    data["title"] = t.get_text(strip=True) if t else None
    # Price
    price_tag = soup.select_one("[data-test='price']") or soup.select_one(".price")
    data["price"] = price_tag.get_text(strip=True) if price_tag else None
    # Description
    desc = soup.select_one(".description") or soup.select_one("[data-test='description']")
    data["description"] = desc.get_text(strip=True) if desc else None

    # Seller prices placeholder - we attempt to extract up to 5 seller offers if available
    sellers = []
    sellers_tags = soup.select(".seller-offer")
    for s in sellers_tags[:5]:
        sellers.append(s.get_text(strip=True))
    for i in range(5):
        data[f"price_seller_{i+1}"] = sellers[i] if i < len(sellers) else None

    # Attempt to extract common attributes from a spec table
    specs = {}
    for row in soup.select("table.specs tr"):
        cols = row.select("td")
        if len(cols) >= 2:
            k = cols[0].get_text(strip=True)
            v = cols[1].get_text(strip=True)
            specs[k] = v

    # Map some expected jewelry fields heuristically
    data["metal_color"] = specs.get("Цвет металла") or specs.get("Цвет") or specs.get("Metal color")
    data["material"] = specs.get("Материал")
    data["inlay_country"] = specs.get("Страна вставки") or specs.get("Страна")
    data["length"] = specs.get("Длина")
    data["size"] = specs.get("Размер")
    data["weight_in_title"] = None
    data["weight_in_description"] = None
    if data.get("title") and ("г" in data["title"] or "гр" in data["title"]):
        data["weight_in_title"] = data["title"]
    if data.get("description") and ("г" in data["description"] or "гр" in data["description"]):
        data["weight_in_description"] = data["description"]

    # For general attributes add spec mapping into data
    for k,v in specs.items():
        data_key = k.lower().replace(" ", "_")[:60]
        data[f"spec_{data_key}"] = v

    return data

def get_category_count_and_pages(category_url, session, proxy=None):
    '''
    Пример — пытаемся получить общее число позиций и рассчитать страницы.
    Возвращаем (count, pages). В реальном сайте нужно менять селекторы.
    '''
    try:
        proxies = {"http": proxy, "https": proxy} if proxy else None
        r = session.get(category_url, headers=HEADERS, proxies=proxies, timeout=15)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")
        # heuristics: find total count
        total_tag = soup.select_one(".total-count") or soup.find(text=lambda t: "товар" in t if t else False)
        total = None
        if total_tag:
            import re
            found = re.findall(r"(\d[\d\s]*)", total_tag.get_text())
            if found:
                total = int(found[0].replace(" ", ""))

        # pages estimate: assume 48 items per page (adjustable)
        per_page = 48
        pages = (total // per_page) + (1 if total % per_page else 0) if total else None
        return total, pages
    except Exception:
        return None, None

def split_price_ranges(min_price, max_price, step=30000):
    '''
    Разбираем диапазон цен на шаги (0-30000, 30001-60000 и т.д.)
    Возвращаем список (low, high) tuples.
    '''
    ranges = []
    low = min_price
    while low <= max_price:
        high = low + step
        ranges.append((low, high))
        low = high + 1
    return ranges

def fetch_product_links_from_category(category_url, session, proxy=None, max_pages=None):
    '''
    Простейший скрейпинг списка товаров — возвращает набор ссылок.
    В реальной интеграции возможно требуется JS rendering.
    '''
    links = []
    proxies = {"http": proxy, "https": proxy} if proxy else None
    # naive pagination loop
    for page in range(1, (max_pages or 100) + 1):
        url = f"{category_url}?page={page}"
        try:
            r = session.get(url, headers=HEADERS, proxies=proxies, timeout=15)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("a.product-link")
            if not items:
                # try other selectors
                items = soup.select(".product-card a")
            if not items:
                break
            for it in items:
                href = it.get("href")
                if href and href.startswith("/"):
                    href = "https://halykmarket.kz" + href
                links.append(href)
            # safety throttle
            time.sleep(random.uniform(0.3, 0.9))
        except Exception:
            break
    return list(dict.fromkeys(links))  # unique

def run_stream(stream_id, params, update_state_callback=None, stop_flag=None):
    '''
    params is a dict:
     - name
     - categories: list of category URLs (strings)
     - cities: list
     - proxies: list
     - data_dir
     - max_workers
     - price_split (boolean)
    '''
    session = requests.Session()
    proxy_gen = rotate_proxies(params.get("proxies", []))
    all_rows = []
    collected_links = 0
    collected_items = 0

    try:
        for cat_url in params.get("categories", []):
            # check stop
            if stop_flag and stop_flag.is_set():
                break

            # get total and pages
            proxy = next(proxy_gen)
            total, pages = get_category_count_and_pages(cat_url, session, proxy=proxy)
            # If total is large or pages > 9996, split by price ranges
            if total and total > 10000 and params.get("price_split", True):
                # define an example max price; in real scenario need better approach
                max_price = 3000000
                ranges = split_price_ranges(0, max_price, step=30000)
                target_ranges = ranges
            else:
                target_ranges = [(None, None)]

            for low, high in target_ranges:
                if stop_flag and stop_flag.is_set():
                    break
                # build url with price filters if needed
                target_url = cat_url
                if low is not None:
                    # These query params are illustrative; change to actual site's params.
                    target_url = f"{cat_url}?price_min={low}&price_max={high}"
                # fetch links
                proxy = next(proxy_gen)
                links = fetch_product_links_from_category(target_url, session, proxy=proxy, max_pages=pages or 50)
                collected_links += len(links)
                # Now fetch each product in multithreaded way
                workers = min(params.get("max_workers", 6), 12)
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = []
                    for link in links:
                        if stop_flag and stop_flag.is_set():
                            break
                        proxy = next(proxy_gen)
                        futures.append(executor.submit(fetch_and_parse, link, proxy, session))
                    for fut in as_completed(futures):
                        if stop_flag and stop_flag.is_set():
                            break
                        try:
                            row = fut.result()
                            if row:
                                all_rows.append(row)
                                collected_items += 1
                        except Exception:
                            continue
                # update callback
                if update_state_callback:
                    update_state_callback(stream_id, collected_links, collected_items)
                # small break to avoid too many requests
                time.sleep(random.uniform(0.5, 1.2))

        # save to excel
        data_dir = params.get("data_dir", "data")
        os.makedirs(data_dir, exist_ok=True)
        base_name = params.get("name", "Результат")
        fname = filename_ru(base_name, collected_items)
        path = os.path.join(data_dir, fname)
        df = pd.DataFrame(all_rows)
        # ensure columns order - basic
        df.to_excel(path, index=False)
        return {"status": "finished", "filename": path, "collected_links": collected_links, "collected_items": collected_items}
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "error": str(e), "collected_links": collected_links, "collected_items": collected_items}

def fetch_and_parse(link, proxy, session):
    try:
        proxies = {"http": proxy, "https": proxy} if proxy else None
        r = session.get(link, headers=HEADERS, proxies=proxies, timeout=20)
        if r.status_code != 200:
            return None
        data = parse_product_card(r.text)
        data["link"] = link
        data["scraped_at"] = datetime.utcnow().isoformat()
        return data
    except Exception:
        return None
