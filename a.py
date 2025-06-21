# %%
import pandas as pd
import re
from datetime import date
from bs4 import BeautifulSoup
import time
from requests_html import HTMLSession
import json
import random
import numpy as np
import requests
import math
from datetime import datetime, timedelta
import openpyxl
from deep_translator import GoogleTranslator
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Maximum number of restarts allowed
MAX_RETRIES = 30

def setup_selenium(cookies, chromedriver_path):
    """Set up undetected Chrome WebDriver with stealth techniques, inject cookies, and then visit the page."""
    options = uc.ChromeOptions()

    # Enable headless mode for running without a display
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("window-size=1920,1080")
    options.add_argument("--remote-debugging-port=9222")

    print("ChromeDriver options configured successfully for headless mode.")

    try:
        driver = uc.Chrome(executable_path=chromedriver_path, options=options)
        time.sleep(5)  # Wait for ChromeDriver to initialize
        print("ChromeDriver initialized successfully.")
    except Exception as e:
        print(f"Error initializing ChromeDriver: {e}")
        return None

    driver.get("https://www.walmart.com/")  # Open Walmart homepage
    time.sleep(5)

    # Inject cookies for the target domain
    for cookie in cookies:
        driver.add_cookie(cookie)

    print("Cookies injected successfully before navigating to the URL.")

    # Additional stealth techniques
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        print("Stealth techniques applied successfully.")
    except Exception as e:
        print(f"Error applying stealth techniques: {e}")
        return None

    return driver

# Random sleep time for human-like behavior
def random_sleep(min_delay=2, max_delay=5):
    """Introduce random sleep times to simulate human behavior."""
    delay = random.uniform(min_delay, max_delay)
    print(f"Sleeping for {delay:.2f} seconds...")
    time.sleep(delay)

# Step 2: Extract Reviews from Page using Selenium and BeautifulSoup
def extract_reviews(page_html, url):
    """Extract reviews from a Walmart page using BeautifulSoup."""
    extracted_reviews = []
    soup = BeautifulSoup(page_html, 'html.parser')
    title = None

    review_elements = soup.find_all('div', class_=re.compile(r'overflow-visible b--none mt\d-l ma0 dark-gray'))
    title_all = soup.find('a', class_='w_x7ug f6 dark-gray')
    if title_all:
        title = title_all.get('href')
        pattern = r'(\d{4}[a-zA-Z]?)-'
        model = re.findall(pattern, title)
    if not review_elements:
        return None  # No reviews found, return None to trigger retry

    for li_tag in review_elements:
        product = {}
        product['Model'] = title
        review_rating_element = li_tag.select_one('.w_iUH7')
        product['Review rating'] = review_rating_element.text if review_rating_element else None
        verified_purchase_element = li_tag.select_one('.pl2.green.b.f7.self-center')
        product['Verified Purchase or not'] = verified_purchase_element.text if verified_purchase_element else None
        review_date_element = li_tag.select_one('.f7.gray')
        product['Review date'] = review_date_element.text if review_date_element else None
        review_title_element = li_tag.select_one('.w_kV33.w_Sl3f.w_mvVb.f5.b')
        product['Review title'] = review_title_element.text if review_title_element else None
        review_content_element = li_tag.select_one('span.tl-m.db-m')
        product['Review Content'] = review_content_element.text.strip() if review_content_element else None
        review_name_element = li_tag.select_one('.f7.b.mv0')
        product['Review name'] = review_name_element.text if review_name_element else None
        syndication_element = li_tag.select_one('.flex.f7 span.gray')
        if syndication_element and 'Review from' in syndication_element.text:
            product['Syndicated source'] = syndication_element.text.split('Review from ')[-1].strip()
        else:
            product['Syndicated source'] = None

        helpful_element = soup.select_one('button[aria-label^="Upvote ndmomma review"] span')
        people_find_helpful = int(helpful_element.text.strip('()')) if helpful_element else 0
        product['URL'] = url
        extracted_reviews.append(product)

    return extracted_reviews

# Step 3: Fetch Reviews for a specific page
def fetch_reviews_for_page(driver, url):
    """Fetch reviews for a specific page."""
    try:
        driver.set_page_load_timeout(120) 
        driver.get(url)
        random_sleep()  # Random delay to wait for the page to load
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        page_links = soup.find_all('a', {'data-automation-id': 'page-number'})
        page_numbers = []
        
        for link in page_links:
            text = link.get_text(strip=True)
            if text.isdigit():
                page_numbers.append(int(text))
        
        if page_numbers:
            last_page_number = max(page_numbers)
            print(f"Last page number detected: {last_page_number}")
        else:
            print("No page links found, assuming only 1 page.")
            last_page_number = 1

        reviews = extract_reviews(page_html, url)
        if reviews:
            print(f"Successfully extracted {len(reviews)} reviews.")
            return reviews, last_page_number
        else:
            print("No reviews found on this page.")
            return [], last_page_number

    except Exception as e:
        print(f"Error during review fetching: {e}")
        return [], 1

# Step 5: Fetch All Reviews
def fetch_all_reviews(url, cookies, chromedriver_path, retry_count=0):
    """Main function to scrape reviews from all pages and keep driver alive."""
    all_reviews = []
    page = 1

    driver = setup_selenium(cookies, chromedriver_path)
    if driver is None:
        return all_reviews

    _, last_page = fetch_reviews_for_page(driver, url)

    while page <= last_page:
        print(f"Fetching page {page}...")
        page_url = f"{url}?page={page}"
        reviews, _ = fetch_reviews_for_page(driver, page_url)

        if not reviews:
            if retry_count < MAX_RETRIES:
                print(f"Restarting script, attempt {retry_count + 1} of {MAX_RETRIES}...")
                driver.quit()
                return fetch_all_reviews(url, cookies, chromedriver_path, retry_count + 1)
            else:
                print(f"No more reviews found at page {page}. Max retries reached.")
                break

        all_reviews.extend(reviews)
        print(f"Reviews extracted from page {page}: {len(reviews)}")
        page += 1
        random_sleep()

    driver.quit()
    return all_reviews

# Step 6: Define Walmart URLs and Cookies
urls = [
    'https://www.walmart.com/reviews/product/5129928603',
    'https://www.walmart.com/reviews/product/435156039'
]

cookies = [
        {"name": "bsc", "value": "TGKx_Bz6wzL6j6csIMAPdw", "domain": "walmart.com"},
        {"name": "thx_guid", "value": "a8004c15bc4cfdfeae810e6f73c1f633", "domain": "walmart.com"},
        {"name": "_tap_path", "value": "/rum.gif", "domain": "walmart.com"},
        {"name": "_tap-criteo", "value": "1728231484866:1728231485619:1", "domain": "walmart.com"},
        {"name": "ACID", "value": "5cb46c38-188b-4ffa-8d95-850b1ab0fdab", "domain": "walmart.com"},
        {"name": "_intlbu", "value": "07ac68c5462a8c20891b91bc0e78fe714817cd8d14d40b527f9125d2c5567fe0", "domain": "walmart.com"},
        {"name": "if_id", "value": "false", "domain": "walmart.com"},
        {"name": "_shcc", "value": "US", "domain": "walmart.com"},
        {"name": "assortmentStoreId", "value": "3081", "domain": "walmart.com"},
        {"name": "auth", "value": "auth_value_here", "domain": "walmart.com"}
]

# Step 7: Scrape Reviews from All URLs
walmart_reviews = []
chromedriver_path = r"chromedriver.exe"  # Replace with the correct path to your chromedriver.exe

for url in urls:
    walmart_reviews.extend(fetch_all_reviews(url, cookies, chromedriver_path))

# Step 8: Convert Reviews to DataFrame and Save to CSV
walmart = pd.DataFrame(walmart_reviews)
walmart.to_csv("walmart_reviews.csv", index=False)
