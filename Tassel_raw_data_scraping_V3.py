# %%
import pandas as pd
import re
from datetime import date 
from bs4 import BeautifulSoup
import requests
import time 
from requests_html import HTMLSession
import json
import random
import numpy as np
import requests
import math
from datetime import datetime
from datetime import datetime, timedelta
import openpyxl
from deep_translator import GoogleTranslator
import undetected_chromedriver as uc
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
import re
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Get the current date and time
now = datetime.now()

# Format the timestamp as a string
timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

# Print the timestamp
print("Current Timestamp:", timestamp)

print('Running Tassel_raw_date_scraping.py')

# %%
excel_file_path = r"Star rating scrape URL and info - NPI.xlsx"
sheet_name = "data_new"

# Read the Excel sheet into a DataFrame
df_amazon = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')
df_amazon['HP Model Number'] = df_amazon['HP Model Number'].astype(str)
df_amazon['Comp Model number'] = df_amazon['Comp Model number'].fillna(0).round(0).astype(int).astype(str)
df_amazon

# %%
path = r"Star rating scrape URL and info - NPI.xlsx"
sheets = 'review_template'
review_template = pd.read_excel(path, sheet_name = sheets, engine='openpyxl')
review_template

def get_soup(url, retries=3):
    global global_cookies

    # Define a stronger header
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }

    session = requests.Session()

    # Retry mechanism
    for _ in range(retries):
        try:
            session.cookies.update(global_cookies)  # Use global cookies for subsequent requests
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error for non-2xx status codes
            soup = BeautifulSoup(response.content, 'html.parser')
            # file_name = f"{random.randint(5, 150)}.html"
            # with open(file_name, 'w', encoding='utf-8') as file:
            #     file.write(str(soup))
            return soup
        except requests.HTTPError as e:
            print(f"Error occurred: {e}")
            time.sleep(random.uniform(1, 5))  # Add a random delay before retrying
            continue
    else:
        print("Failed to retrieve the page after multiple retries.")
        return None

def get_soup_amazon(url, host):
    headers = {
        "Host": host,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }



    if(host == 'www.amazon.co.uk'):
       cookies = {
        "csm-sid": "903-9836176-6967009",
        "x-amz-captcha-1": "1726144856604350",
        "x-amz-captcha-2": "qEybXoxNsed3VxMGE+SE+A==",
        "id_pkel": "n0",
        "sp-cdn": "\"L5Z9:IN\"",
        "x-acbuk": "\"yuiOffCDUkJx6URQInGTJ6XlUQeW?LLFW@ncc752HDvrhB8fkYnPput0cMPxg0EX\"",
        "at-acbuk": "Atza|IwEBIOJtMeigSR4gCXGlTlCuhLTgNhVubQ8O2XychooYXDzNlzAIwvUNIA1H9Zbs5XoG0JF9h4Jvrs3ijJJgvfP55z197zpL87ls6TjIIje3iyVhtts6DbdJC6vA4kKDrqn6nJG4nHrl6d24Tn9UWbWk86EmVf-jQyP-dwer9n_q9JdqJFgfShB5hc_rgeVn-agSqkZq35K8l4QaOcluKKTEvSzXihQ3vXWoSnUSVTF9g14PKQ",
        "sess-at-acbuk": "\"Tweb+5/58q3Thu7TyShnr2SOz3X55HMLWo/zS1WYuOE=\"",
        "sst-acbuk": "Sst1|PQG3xWZaRJ0cBjL730W8I9--CbrxJB47zNixmDqU977lfvr8ouxMuhU5oREdiMWO_OOkdA6JqghqnG5ONKrAfz67rh64YZXpn3iAyqWrVbT7ZMPBwLBdskZThleZxTWVDTz2JuXiQbwQnMAGiJyj2aXq7DIgfWNIHlld_LuZssXyU_gqXfZk6po0t54FIOm-fuF-c0_F99l5ZVvvPNORVp-7DzLI7pRB9_ReVvHHIGZqrtu83UuaVpNANunIdxs2E0AlacQOBKyy2jDf0aMvc0XvaUaJC1pMjZKUsqy0jbwIWeI",
        "session-id": "262-8938359-6381428",
        "ubid-acbuk": "258-3471506-1778545",
        "session-id-time": "2082787201l",
        "i18n-prefs": "GBP",
        "lc-acbuk": "en_GB",
        "session-token": "SgGzvM1R1CODsgTJghK26IeqWqRWsmNSNe1IN0LjKrLslahRjIkjR3HgYcZ8ML+Jbq/pdsXAL9KIQNYYrR3nyTjw0rkNhyk77y+9eTVbJ2DrqH8Uuz8MFonWRdn/jEH0Y3iisIO7FjGHagDu61sjqXvBp2HD5k5MAmTjAg4kzjQGQwCda/uNRnZZrSYoizhPLA77PGER+iTbYjlNEOa4HSOXncLeyoKceWUWVAr0znP4WAnRGhhXlSJHAmySTgtuT8PtDkS6Phb5ktBls684WVXUG8viRChzQy1Ig12Ni51sjicqTnupDflMU9bug1pRv7wQ+0sgVpuwIp0bKwnCivudiJ6kwUWnDM7tqGj7zGJCUuurovD3cuNaYTFA5mFu"
    }
    elif(host == 'www.amazon.es'):
        cookies = {
        "id_pkel": "n0",
        "session-id": "257-5598184-5873262",
        "ubid-acbes": "258-5854927-0158657",
        "session-token": "\"+7oftLYwnh8EHMol3vfBI6jJ2TV6aFbg2grjvif+ogyH7X+fkVWduOC+zg804drGOSzTtxkE5PeBu0bbZjbAo+JQMYttPWD3QTSKlw59C1IlpIygdvk7QH9dKIuSgfuCkZWOh5y2iZ3y0DiyRX1Q+zbn7hTiXXFV3LhwDZTB8tsIqEae6oYKwygqTOxLlXLzVoPIQk+ecEg3c6pr13qLM+hnlgXVS9tVrbIS+6vA032G+tkjUj9EP2/37Uj52m5ZFgMfPFecMZjYKvjPYooXut3a1kPCQ9R+pCR9EI4H9GVkd/aQvV4jAxU69ugDrZ6+gGFtKAbG9JkTOA5KNAH5R5aNnLNNAtd8BFx/5x3y0hic+mradYAv5Q==\"",
        "x-acbes": "\"ddqjWg6xf@srOWMvkRnhx6d@Rbfd3jVwuvviv@CrOM?fr@I55XYacOGLiBCiBtkd\"",
        "at-acbes": "Atza|IwEBIILAcon2nBuMboH4I12LftjcdWL6_QTeBZAMtq09wzbMF6tHWIDCAsTb1fNYndEyQvpB6WDz7riwKAHM3sS5PWG2NOjqknvQGcXBME_DoS_fv5T37vVKqzfeBEPGlykMpESkuGVQwGAdUlIiZ-Ocok1I5wmvvE2d5nG92Os2v9OqxeRAqde4Qws6snM9jbEUxvJbERF-UKKpZUMmMgQrna4SOvFBrR1iHHk0ZZMkqay-iw",
        "sess-at-acbes": "\"B9PYBW+c3zxV2RtmYVJQIVSH624Xafu7AH4POuxetPQ=\"",
        "sst-acbes": "Sst1|PQFIZrA21csrcZqsrrMbIciMCbNQQ0SCX72wbZmJmVmjIMIvc5jEHk2Prq7gGl5gLHU063SPzp29c62OhqJiMwO7M_fjtLGmh96TjbV7PncHTOWQ77FKYYeKtaTUch6iQunuE2azLVn6jinuNjU1VRFO5C6kMTy48MJzqwfprbm9tY6mOH5zgU_nRtWg5sDwVw9Nb-BEyUNoBs48wbDB_WoiX5z1qwCvVAskroUY0TRIo5Xidu_2PJQXTlXKdCj8Wb7fScmcfbjTzJsS9Rw76_ThEZP0GH2T19LcB_MLj96L5ic",
        "session-id-time": "2082787201l",
        "i18n-prefs": "EUR",
        "lc-acbes": "es_ES",
        "csm-hit": "tb:s-4JQH4X82C92A432W46EC|1728318586057&t:1728318586700&adb:adblk_no"
    }
    else:
        cookies = {
        "at-main": "Atza|IwEBIF8fWp8N6wwKJntOIGx2Iz2Tzmp0jmlFToAsLpZyql2gf_tMuytB0Wq_9Q6eSfQckLBIoWEVn4pjZ_WMdDle2G7wLmfsrW3nnEmKMUA02jmAZpsXM5KEJwH0Hb8C04WZKpjfJYdhzyYBSz_T5gyHOw_FlXF_pCj3oquUHroknOq0G-ILvkZxvRbJLNkKk30UJt2O28Gi7VUcDib4Qpo31ltrWxt0eN8EzCdIZxVLAmNz-g",
        "sess-at-main": "\"xfy9iZnuMPNqiCnv39ubW/JVEoJMw6HX+jT+zpzijho=\"",
        "sst-main": "Sst1|PQENTri85vVuQqInRi4tn1sRCTJnoGB_U9PX9G7i6l9QYLTaHul93ueh3jEdoZdJPHouFM3VHOWwx4eNe2lRG-rWM3V17_1bEH0WaT59C0eZS-VZJ3800scFSPo_bOjGuT4Z6H1oBZTcH3pblvW2QxJOlBwm2zBYZOav6LRw319WRJDfYmmrvjE3Gqiyi1KdeCgGL6vrLVl5DHhDjBygvSDWNHYVUn7WC4gcvLW-qbNmVseZTnwShfY-r0shikWcVLo5K0QnjZppa1tQIWEi1RP2y4I1T8m0T7pBIHs3tHCEY4s",
        "x-main": "\"EhNPt8@KYq7hUHIQ1Qh6mEAdTrkwApsU2OL0whAniY?qsHfGS@@fHwWgmIjwV@4U\"",
        "i18n-prefs": "USD",
        "lc-main": "en_US",
        "session-token": "lNWWXnGpmO6H/Ngtu2aSRGaWec4m8rvxAFHkkp3oWLoqoqm/5UWej6oMXJ7yrYUdI+OdVIczGKJQWL82ftyFq4KIwwi5Ec8DqPTt1CRVZ5zmlunkiJc9JT9Hz7vB4Ko8KeAtmAtL1BsFTJcb3Oin3uYdbtUstW0ZmD0UNjJvoZVk035kC1UveGPv2Up9mC9Gdk672LMS8AyFpz3kuR/bKQTRkC7WfsdiCrW3vOMWVIkipm37dsnsTrPMCgLrdAYAShnfuZoF5/hr1grf2CxtctH/xlvV0Gm8K6++DnjVfWG4W/B4jt7IYppw8SvROoKzEOvgiHrcfejzOIU0y29I8BK8CAXcrsPWlPBaJmvLtqTCF+Dv9gXsNA3HstUH0FSu",
        "ubid-main": "135-8431336-5346218",
        "session-id": "143-9100146-8969639",
        "session-id-time": "2082787201l"
    }

        

    req = requests.get(url, headers=headers, cookies=cookies)
    soup = BeautifulSoup(req.content, "html.parser")
    return soup




def amazon_review(soup, url):    
    review = {}
    extracted_reviews = []   
    try:
        model = soup.title.text.replace("Amazon.com: Customer reviews: ","")    
    except AttributeError: 
        try:
            model = soup.find("a", attrs={"data-hook": "product-link"}).string.strip()  
        except AttributeError: 
            model = soup.find("div", attrs={"class": "a-row product-title"}).string.strip()  
  
    reviews = soup.find_all("div", {"data-hook": "review"})


    for item in reviews:    
        # review_date_string = item.find('span', {'data-hook': 'review-date'}).text.replace('Reviewed in', '').split('on')[1].strip()
        # review_date = datetime.strptime(review_date_string, "%B %d, %Y")

        review = {    
            'Model': model,    
            'Review date': item.find('span', {'data-hook': 'review-date'}).text.replace('Reviewed in', '').split('on')[
                1],
            "Review Content": item.find("span", {'data-hook': "review-body"}).text.strip(),  
            "URL" : url  
        }
        
        
  
        try:    
            review["Review rating"] = float(item.find("i", {"data-hook": "review-star-rating"}).text.replace("out of 5 stars", "").strip())    
        except AttributeError:    
            review["Review rating"] = float(item.find("span", {"class": "a-icon-alt"}).text.replace("out of 5 stars", "").strip())    
  
        try:    
            review['Review title']  = item.find("a", {'data-hook': "review-title"}).text.strip()    
        except AttributeError:    
            review['Review title']  = item.find("span", {'data-hook': "review-title"}).text.strip()    
  
        try:    
            review["Verified Purchase or not"] = item.find("span", {'data-hook': "avp-badge"}).text.strip()    
        except AttributeError:    
            review["Verified Purchase or not"] = None    
  
        try:      
            review["Review name"] = item.find("span", {'class': "a-profile-name"}).string.strip()  
        except AttributeError:        
            review["Review name"] = None  
  
        try:    
            review["People_find_helpful"] = item.find("span", {'data-hook': "helpful-vote-statement"}).text.strip()    
        except AttributeError:    
            review["People_find_helpful"] = None  
            
        try:
            seeding= item.find("span", {'class': "a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-normal c7y-badge-link c7y-badge-vine-voice a-text-bold"}).text.strip() 
            if seeding:
               review['Seeding or not'] = seeding
            else:
                raise AttributeError
        except AttributeError:  
            try: 
                review['Seeding or not'] = item .find('span', {'class': 'a-color-success a-text-bold'}, string='Vine Customer Review of Free Product')

            except AttributeError:
                review['Seeding or not'] = None

        try:
            review['Aggregation'] = item.find("a", {"data-hook": "format-strip"}).text.strip()
        except AttributeError:   
             review['Aggregation'] = None
    
  
        extracted_reviews.append(review)    
    
  
    return extracted_reviews

# %%
urls = ['https://www.amazon.co.uk/HP-DeskJet-Wireless-included-Reliable/product-reviews/B0CFFBXYSH/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&formatType=current_format']
       # 'https://www.amazon.co.uk/HP-DeskJet-Wireless-Included-Reliable/product-reviews/B0CFFC6LRR/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&formatType=current_format',
       # 'https://www.amazon.co.uk/HP-DeskJet-Wireless-Included-Reliable/product-reviews/B0CB722L39/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&formatType=current_format']

# %%
import datetime 
from datetime import datetime
star = ['one','two','three','four','five'] 
max_retry_attempts = 2
all_reviews = []
for link in urls:
    print(link)
    for y in star:
        found_reviews = True
        for x in range(1, 11):
            retry_attempts = 0
            while found_reviews is True:
                try:
                    url = f'{link}&pageNumber={x}&filterByStar={y}_star&sortBy=recent'  
                    print(url)
                    print('Page:',x, f'{y} star')
                    soup = get_soup_amazon(url,'www.amazon.co.uk')  # Get the soup object from the URL
                    extracted_reviews = amazon_review(soup, url)  # Extract reviews from the soup
                   
                    # if soup.find('div', {'class': 'a-section a-spacing-top-large a-text-center no-reviews-section'}):  
                    #         print('No review')  
                    #         found_reviews = False
                    #         break 
                    
                    if len(extracted_reviews) > 0:
                        all_reviews.extend(extracted_reviews)
                        print(f"Page {x} scraped {len(extracted_reviews)} reviews")
                    
                    # if (page == 1 and len(extracted_reviews) == 0):
                    #     print(f"Page {page} has no reviews, retry")
                    #     continue
                        
                    if soup.find('li', {'class': 'a-disabled a-last'}):  
                        print('No more pages left')  
                        found_reviews = False
                        break 
                    
                    if x >= 1 and len(extracted_reviews) == 0:
                        retry_attempts += 1
                        if retry_attempts == max_retry_attempts:
                            found_reviews = False
                            print(f"Page {x} has no reviews, moving to the next page")
                            break
                        else:
                            print(f"Page {x} has no reviews, retry")
                            continue 

                    
                            
                    else:
                        break  
        
                    

                except Exception as e:
                    print(f"Error encountered: {e}. Retrying in 3 seconds...")

                    # If any exception occurs, retry
                    retry_attempts += 1
                    if retry_attempts == max_retry_attempts:
                        break
                    else:
                        print(f"An error occurred, retrying")
                        continue  # Retry the loop
            else:
                # If all retry attempts failed, move to the next page
                continue
            
           



from datetime import date 
pd.set_option('display.max_columns', None)
amazon2= pd.DataFrame(all_reviews)
amazon2['Retailer']="Amazon"
amazon2['scraping_date'] = pd.to_datetime(date.today())
amazon2['Review date'] = pd.to_datetime(amazon2['Review date'])
amazon2['Review title'] = amazon2['Review title'].str.extract(r'out of 5 stars\n(.*)')
amazon2['HP Model Number'] = amazon2['Model'].str.extract(r'(\d+e?)')
amazon2['People_find_helpful'] = amazon2['People_find_helpful'].str.extract(r'(\d*) people found this helpful')
amazon_filter = amazon2[amazon2['Aggregation'] != 'Model name: Old Version']
amazon_hp_combine = pd.merge(amazon_filter, df_amazon, on = "HP Model Number", how = "left" )
amazon_hp_combine['Review Model'] = amazon_hp_combine['HP Model'] 
columns_to_drop = [  
    'Model', 'HP Model Number', 'Comp Model number','HP Model'
]  
# amazon_hp_combine['Aggregation'] = amazon_hp_combine['Aggregation'].fillna('',inplace = True) 
amazon_hp_combine = amazon_hp_combine.drop(columns_to_drop, axis = 1) 

amazon_hp_combine.drop_duplicates(inplace = True)
amazon_hp_combine


# %%
amazon_final = amazon_hp_combine 
amazon_final.drop_duplicates(inplace = True)
amazon_final['Review Content'] = amazon_final['Review Content'] .astype(str).apply(lambda x: re.sub(r'The media could not be loaded\.', '', x).strip())
amazon_final['Review Content'] = amazon_final['Review Content'].astype(str).apply(
    lambda x: re.sub(
        r'Video Player is loading\.Play VideoPlayMuteCurrent Time[\s\S]*?This is a modal window\.',
        '',
        x
    ).strip()
)

amazon_final['Competitor_flag'] = amazon_final['Review Model'].astype(str).apply(lambda x: 'No' if 'HP' in x else 'Yes')
amazon_final['Country'] = 'UK'
amazon_final.sort_values(by = ['Review date'],ascending = False)

amazon_final_df= amazon_final.rename(columns={
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Retailer': 'Retailer',
    'Comp Model': 'Comp_Model',
    'Review date': 'Review_Date',
    'Review name': 'Review_Name',
    'Review rating': 'Review_Rating',
    'Review title': 'Review_Title',
    'Review Content': 'Review_Content',
    'Verified Purchase or not': 'Verified_Purchase_Flag',
    'People_find_helpful': 'People_Find_Helpful',
    'Seeding or not': 'Seeding_Flag',
    'URL': 'URL',
    'scraping_date': 'Scraping_Date',
    'Segment': 'Segment',
    'Competitor_flag': 'Competitor_Flag',
    'Aggregation':'Aggregation_Flag',
    'Country': 'Country'
})

amazon_final_df['Review_Date'] = pd.to_datetime(amazon_final_df['Review_Date']).dt.date
amazon_final_df['Review_Rating'] = amazon_final_df['Review_Rating'].astype('int64')
amazon_final_df['People_Find_Helpful'] = amazon_final_df['People_Find_Helpful'].fillna(0).astype('int64')
amazon_final_df['Scraping_Date'] =  pd.to_datetime(amazon_final_df['Scraping_Date']).dt.date
amazon_final_df.reset_index(inplace = True,drop = True)
amazon_final_df.sort_values(['Review_Date'],ascending = False) 
amazon_final_df['Review_Rating_Label'] = amazon_final_df['Review_Rating'].apply(lambda x: '1-2-3-star' if x < 4 else '4-5-star')
amazon_final_df


# %%
final_review = pd.concat([review_template, amazon_final_df])
final_review.to_csv('uk.csv', index=False)

# %% [markdown]
## Spain

# %%
urls =  ['https://www.amazon.es/HP-DeskJet-2820e-Impresora-Multifunci%C3%B3n/product-reviews/B0CFFWJHMF/ref=cm_cr_arp_d_viewopt_fmt?formatType=current_format']
        # 'https://www.amazon.es/Impresora-Multifunci%C3%B3n-HP-impresi%C3%B3n-Fotocopia/product-reviews/B0CFG1PB4P/ref=cm_cr_arp_d_viewopt_fmt?formatType=current_format']

# %%
from datetime import datetime
translator = GoogleTranslator(source='auto', target='en')
def amazon_review(soup, url, translate_to=None):    
    review = {}
    extracted_reviews = []   
    try:
        model = soup.title.text.replace("Amazon.com: Customer reviews: ","")    
    except AttributeError: 
        try:
            model = soup.find("a", attrs={"data-hook": "product-link"}).string.strip()  
        except AttributeError: 
            model = soup.find("div", attrs={"class": "a-row product-title"}).string.strip()  
  
    reviews = soup.find_all("div", {"data-hook": "review"})


    for item in reviews:    
        # review_date_string = item.find('span', {'data-hook': 'review-date'}).text.replace('Reviewed in', '').split('on')[1].strip()
        # review_date = datetime.strptime(review_date_string, "%B %d, %Y")

        review = {    
            'Model': model,
            'Review date':
                item.find('span', {'data-hook': 'review-date'}).text.replace('Revisado en España', '').split('el')[
                    1],
            "Orginal Review": item.find("span", {'data-hook': "review-body"}).text.strip(), 
            "URL" : url  
        }
        
        
  
        if translate_to and review["Orginal Review"]:
            translated_review = translator.translate(review["Orginal Review"])
            review["Review Content"] = translated_review

        try:
            review["Orginal Title"] = item.find("a", {'data-hook': "review-title"}).text.strip()
        except AttributeError:
            review["Orginal Title"] = item.find("span", {'data-hook': "review-title"}).text.strip()

        if translate_to and review["Orginal Title"]:
            
            translated_review = translator.translate(review["Orginal Title"])
            test = translated_review

        review["Orginal Title"] = review["Orginal Title"].split('\n')[-1]    
        review["Review Title"] = test.split('\n')[-1]
        
        #print(review["Review Title"])
        
        review["URL"] = url

        try:
            review["Review rating"] = (
                item.find("i", {"data-hook": "review-star-rating"}).text.replace(",0 de 5 estrellas", "").strip())
        except AttributeError:
            review["Review rating"] = (
                item.find("span", {"class": "a-icon-alt"}).text.replace(",0 de 5 estrellas", "").strip())

        try:
            review["Verified Purchase or not"] = item.find("span", {'data-hook': "avp-badge"}).text.strip()
        except AttributeError:
            review["Verified Purchase or not"] = None

        try:
            review["Review name"] = item.find("span", {'class': "a-profile-name"}).string.strip()
        except AttributeError:
            review["Review name"] = None

        try:
            review["People_find_helpful"] = item.find("span", {'data-hook': "helpful-vote-statement"}).text.strip()
        except AttributeError:
            review["People_find_helpful"] = None
            
        try:
            seeding= item.find("span", {'class': "a-size-mini a-color-link c7yBadgeAUI c7yTopDownDashedStrike c7y-badge-text a-text-normal c7y-badge-link c7y-badge-vine-voice a-text-bold"}).text.strip() 
            if seeding:
               review['Seeding or not'] = seeding
            else:
                raise AttributeError
        except AttributeError:  
            try: 
                review['Seeding or not'] = item .find('span', {'class': 'a-color-success a-text-bold'}, string='Vine Customer Review of Free Product')

            except AttributeError:
                review['Seeding or not'] = None

        try:
            review['Aggregation'] = item.find("a", {"data-hook": "format-strip"}).text.strip()
        except AttributeError:   
             review['Aggregation'] = None
    
  
        extracted_reviews.append(review)    
    
  
    return extracted_reviews

# %%
import datetime 
from datetime import datetime
star = ['one','two','three','four','five'] 
max_retry_attempts = 1
all_reviews = []
for link in urls:
    print(link)
    for y in star:
        found_reviews = True
        for x in range(1, 11):
            retry_attempts = 0
            while found_reviews is True:
                try:
                    url =f'{link}&filterByStar={y}_star&pageNumber={x}&sortBy=recent'
                    print('Page:',x, f'{y} star')
                    soup = get_soup_amazon(url,'www.amazon.es')  # Get the soup object from the URL
                    extracted_reviews = amazon_review(soup, url,translate_to="en")  # Extract reviews from the soup
                    # if soup.find('div', {'class': 'a-section a-spacing-top-large a-text-center no-reviews-section'}):  
                    #         print('No review')  
                    #         found_reviews = False
                    #         break 

                    if len(extracted_reviews) > 0:
                        all_reviews.extend(extracted_reviews)
                        print(f"Page {x} scraped {len(extracted_reviews)} reviews")

                    # if (page == 1 and len(extracted_reviews) == 0):
                    #     print(f"Page {page} has no reviews, retry")
                    #     continue

                    if soup.find('li', {'class': 'a-disabled a-last'}):  
                        print('No more pages left')  
                        found_reviews = False
                        break 

                    if x >= 1 and len(extracted_reviews) == 0:
                        retry_attempts += 1
                        if retry_attempts == max_retry_attempts:
                            found_reviews = False
                            print(f"Page {x} has no reviews, moving to the next page")
                            break
                        else:
                            print(f"Page {x} has no reviews, retry")
                            continue 



                    else:
                        break  



                except Exception as e:
                    print(f"Error encountered: {e}. Retrying in 3 seconds...")

                    # If any exception occurs, retry
                    retry_attempts += 1
                    if retry_attempts == max_retry_attempts:
                        break
                    else:
                        print(f"An error occurred, retrying")
                        continue  # Retry the loop
            else:
                # If all retry attempts failed, move to the next page
                continue
            
           




from datetime import date 
amazon2 = pd.DataFrame(all_reviews)
#print(amazon2)

# Function to parse the date column
def parse_date(date_str):
    months = {
        "enero": "January",
        "febrero": "February",
        "marzo": "March",
        "abril": "April",
        "mayo": "May",
        "junio": "June",
        "julio": "July",
        "agosto": "August",
        "septiembre": "September",
        "octubre": "October",
        "noviembre": "November",
        "diciembre": "December"
    }
    date_str = date_str.strip()
    day, month, year = date_str.split(' de ')
    month = months[month]
    date = datetime.strptime(f"{day} {month} {year}", "%d %B %Y")
    return date

# Apply the function to the Date column
amazon2['Review date'] = amazon2['Review date'].apply(parse_date)

amazon2['Retailer'] = "Amazon"
amazon2['scraping_date'] = pd.to_datetime(date.today())
amazon2['Review date'] = pd.to_datetime(amazon2['Review date'])
#amazon2['Review Title'] = amazon2['Review Title'].str.extract(r'out of 5 stars\n(.*)')
amazon2['HP Model Number'] = amazon2['Model'].str.extract(r'(\d+e?)')
amazon2['People_find_helpful'] = amazon2['People_find_helpful'].str.extract(r'(\d*) people found this helpful')
amazon_filter = amazon2[amazon2['Aggregation'] != 'Model name: Old Version']
amazon_hp_combine = pd.merge(amazon_filter, df_amazon, on="HP Model Number", how="left")
amazon_hp_combine['Review Model'] = amazon_hp_combine['HP Model']

columns_to_drop = ['Model', 'HP Model Number', 'Comp Model number', 'HP Model']
amazon_hp_combine = amazon_hp_combine.drop(columns_to_drop, axis=1)
amazon_hp_combine.drop_duplicates(inplace=True)

amazon_final = amazon_hp_combine
amazon_final.drop_duplicates(inplace=True)

# Clean Review Content
amazon_final['Review Content'] = amazon_final['Review Content'].astype(str).apply(lambda x: re.sub(r'The media could not be loaded\.', '', x).strip())
amazon_final['Review Content'] = amazon_final['Review Content'].astype(str).apply(lambda x: re.sub(r'Video Player is loading\.Play VideoPlayMuteCurrent Time[\s\S]*?This is a modal window\.', '', x).strip())

amazon_final['Competitor_flag'] = amazon_final['Review Model'].astype(str).apply(lambda x: 'No' if 'HP' in x else 'Yes')
amazon_final['Country'] = 'Spain'
amazon_final.sort_values(by=['Review date'], ascending=False, inplace=True)

# Rename columns
amazon_final_df = amazon_final.rename(columns={
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Retailer': 'Retailer',
    'Comp Model': 'Comp_Model',
    'Review date': 'Review_Date',
    'Review name': 'Review_Name',
    'Review rating': 'Review_Rating',
    'Review Content': 'Review_Content',
    "Review Title": "Review_Title",
    'Verified Purchase or not': 'Verified_Purchase_Flag',
    'People_find_helpful': 'People_Find_Helpful',
    'Seeding or not': 'Seeding_Flag',
    'URL': 'URL',
    'scraping_date': 'Scraping_Date',
    'Segment': 'Segment',
    'Competitor_flag': 'Competitor_Flag',
    'Aggregation': 'Aggregation_Flag',
    'Country': 'Country',
    'Orginal Review': 'Orginal_Review'
})
amazon_final_df['Orginal_Title'] = amazon2['Orginal Title']
amazon_final_df['Orginal Title'] = ""
amazon_final_df['Review_Date'] = pd.to_datetime(amazon_final_df['Review_Date']).dt.date
amazon_final_df['Review_Rating'] = amazon_final_df['Review_Rating'].astype('int64')
amazon_final_df['People_Find_Helpful'] = amazon_final_df['People_Find_Helpful'].fillna(0).astype('int64')
amazon_final_df['Scraping_Date'] = pd.to_datetime(amazon_final_df['Scraping_Date']).dt.date

# Create Review_Rating_Label column
amazon_final_df['Review_Rating_Label'] = amazon_final_df['Review_Rating'].apply(lambda x: '1-2-3-star' if x < 4 else '4-5-star')

# Define the required columns
required_columns = [
    'Review_Model', 'Competitor_Flag', 'HP_Class', 'Segment', 'Retailer',
    'Comp_Model', 'Review_Date', 'Review_Name', 'Review_Rating',
    'Review_Rating_Label', 'Review_Title', 'Review_Content', 'Seeding_Flag',
    'Verified_Purchase_Flag', 'Promotion_Flag', 'Aggregation_Flag',
    'People_Find_Helpful', 'Syndicated_Source', 'Response_Date',
    'Response_Text', 'Response_Name', 'URL', 'Scraping_Date', 'Country', 'Orginal_Title','Orginal Title',
]

# Ensure all required columns are present
for col in required_columns:
    if col not in amazon_final_df.columns:
        amazon_final_df[col] = None  # or use an appropriate default value

# Select only the required columns
amazon_final_df = amazon_final_df[required_columns]

# Save to CSV
amazon_final_df.to_csv('es.csv', index=False)

# %% [markdown]
# # US

# %%
def amazon_review(soup, url):    
    review = {}
    extracted_reviews = []   
    try:
        model = soup.title.text.replace("Amazon.com: Customer reviews: ","")    
    except AttributeError: 
        try:
            model = soup.find("a", attrs={"data-hook": "product-link"}).string.strip()  
        except AttributeError: 
            model = soup.find("div", attrs={"class": "a-row product-title"}).string.strip()  
  
    reviews = soup.find_all("div", {"data-hook": "review"})
    
    # NPI launched on 2024-01-15
    date_string = "2024-01-15"
    min_date = datetime.strptime(date_string, "%Y-%m-%d")

    for item in reviews:    
        review_date_string = item.find('span', {'data-hook': 'review-date'}).text.replace('Reviewed in', '').split('on')[1].strip()
        review_date = datetime.strptime(review_date_string, "%B %d, %Y")
        if review_date < min_date:
            print('Review date is less than 2024-01-15')
            break
    
        review = {    
            'Model': model,    
            'Review date': review_date,     
            "Review Content": item.find("span", {'data-hook': "review-body"}).text.strip(),  
            "URL": url  
        }
        
        try:    
            review["Review rating"] = float(item.find("i", {"data-hook": "review-star-rating"}).text.replace("out of 5 stars", "").strip())    
        except AttributeError:    
            review["Review rating"] = float(item.find("span", {"class": "a-icon-alt"}).text.replace("out of 5 stars", "").strip())    
  
        try:    
            review['Review title'] = item.find("a", {'data-hook': "review-title"}).text.strip()    
        except AttributeError:    
            review['Review title'] = item.find("span", {'data-hook': "review-title"}).text.strip()    
  
        try:    
            review["Verified Purchase or not"] = item.find("span", {'data-hook': "avp-badge"}).text.strip()    
        except AttributeError:    
            review["Verified Purchase or not"] = None    
  
        try:      
            review["Review name"] = item.find("span", {'class': "a-profile-name"}).string.strip()  
        except AttributeError:        
            review["Review name"] = None  
  
        try:    
            review["People_find_helpful"] = item.find("span", {'data-hook': "helpful-vote-statement"}).text.strip()    
        except AttributeError:    
            review["People_find_helpful"] = None  
            
        try:
            seeding = item.find("span", {'class': "a-color-success a-text-bold"}).text.strip()
            if 'Vine Customer Review of Free Product' in seeding:
                review['Seeding or not'] = 'Vine Customer Review of Free Product'
            else:
                review['Seeding or not'] = None
        except AttributeError:
            review['Seeding or not'] = None

        try:
            review['Aggregation'] = item.find("a", {"data-hook": "format-strip"}).text.strip()
        except AttributeError:   
            review['Aggregation'] = None
    
        extracted_reviews.append(review)    
    
    return extracted_reviews



# %%

urls = ['https://www.amazon.com/HP-DeskJet-Wireless-included-588S5A/product-reviews/B0CT2R7199/ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=all_reviews&formatType=current_format']
       # 'https://www.amazon.com/HP-DeskJet-Wireless-Included-588S6A/product-reviews/B0CT2QHQVF/ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=all_reviews&formatType=current_format']

# %%
import datetime 
from datetime import datetime
star = ['one','two','three','four','five'] 
max_retry_attempts = 2
all_reviews = []
for link in urls:
    print(link)
    for y in star:
        found_reviews = True
        for x in range(1, 11):
            retry_attempts = 0
            while found_reviews is True:
                try:
                    url = f'{link}&pageNumber={x}&filterByStar={y}_star&sortBy=recent'  
                    print(url)
                    print('Page:',x, f'{y} star')
                    soup = get_soup_amazon(url,'www.amazon.com')  # Get the soup object from the URL
                    extracted_reviews = amazon_review(soup, url)  # Extract reviews from the soup
                   
                    # if soup.find('div', {'class': 'a-section a-spacing-top-large a-text-center no-reviews-section'}):  
                    #         print('No review')  
                    #         found_reviews = False
                    #         break 
                    
                    if len(extracted_reviews) > 0:
                        all_reviews.extend(extracted_reviews)
                        print(f"Page {x} scraped {len(extracted_reviews)} reviews")
                    
                    # if (page == 1 and len(extracted_reviews) == 0):
                    #     print(f"Page {page} has no reviews, retry")
                    #     continue
                        
                    if soup.find('li', {'class': 'a-disabled a-last'}):  
                        print('No more pages left')  
                        found_reviews = False
                        break 
                    
                    if x >= 1 and len(extracted_reviews) == 0:
                        retry_attempts += 1
                        if retry_attempts == max_retry_attempts:
                            found_reviews = False
                            print(f"Page {x} has no reviews, moving to the next page")
                            break
                        else:
                            print(f"Page {x} has no reviews, retry")
                            continue 

                    
                            
                    else:
                        break  
        
                    

                except Exception as e:
                    print(f"Error encountered: {e}. Retrying in 3 seconds...")

                    # If any exception occurs, retry
                    retry_attempts += 1
                    if retry_attempts == max_retry_attempts:
                        break
                    else:
                        print(f"An error occurred, retrying")
                        continue  # Retry the loop
            else:
                # If all retry attempts failed, move to the next page
                continue
            
           



from datetime import date 
pd.set_option('display.max_columns', None)
amazon2= pd.DataFrame(all_reviews)
amazon2['Retailer']="Amazon"
amazon2['scraping_date'] = pd.to_datetime(date.today())
amazon2['Review date'] = pd.to_datetime(amazon2['Review date'])
amazon2['Review title'] = amazon2['Review title'].str.extract(r'out of 5 stars\n(.*)')
amazon2['HP Model Number'] = amazon2['Model'].str.extract(r'(\d+e?)')
amazon2['People_find_helpful'] = amazon2['People_find_helpful'].str.extract(r'(\d*) people found this helpful')
amazon_filter = amazon2[amazon2['Aggregation'] != 'Model name: Old Version']
amazon_hp_combine = pd.merge(amazon_filter, df_amazon, on = "HP Model Number", how = "left" )
amazon_hp_combine['Review Model'] = amazon_hp_combine['HP Model'] 
columns_to_drop = [  
    'Model', 'HP Model Number', 'Comp Model number','HP Model'
]  
# amazon_hp_combine['Aggregation'] = amazon_hp_combine['Aggregation'].fillna('',inplace = True) 
amazon_hp_combine = amazon_hp_combine.drop(columns_to_drop, axis = 1) 

amazon_hp_combine.drop_duplicates(inplace = True)
amazon_hp_combine


# %%
amazon_final = amazon_hp_combine 
amazon_final.drop_duplicates(inplace = True)
amazon_final['Review Content'] = amazon_final['Review Content'] .astype(str).apply(lambda x: re.sub(r'The media could not be loaded\.', '', x).strip())
amazon_final['Review Content'] = amazon_final['Review Content'].astype(str).apply(
    lambda x: re.sub(
        r'Video Player is loading\.Play VideoPlayMuteCurrent Time[\s\S]*?This is a modal window\.',
        '',
        x
    ).strip()
)

amazon_final['Competitor_flag'] = amazon_final['Review Model'].astype(str).apply(lambda x: 'No' if 'HP' in x else 'Yes')
amazon_final['Country'] = 'US'
amazon_final.sort_values(by = ['Review date'],ascending = False)

amazon_final_df= amazon_final.rename(columns={
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Retailer': 'Retailer',
    'Comp Model': 'Comp_Model',
    'Review date': 'Review_Date',
    'Review name': 'Review_Name',
    'Review rating': 'Review_Rating',
    'Review title': 'Review_Title',
    'Review Content': 'Review_Content',
    'Verified Purchase or not': 'Verified_Purchase_Flag',
    'People_find_helpful': 'People_Find_Helpful',
    'Seeding or not': 'Seeding_Flag',
    'URL': 'URL',
    'scraping_date': 'Scraping_Date',
    'Segment': 'Segment',
    'Competitor_flag': 'Competitor_Flag',
    'Aggregation':'Aggregation_Flag',
    'Country': 'Country'
})

amazon_final_df['Review_Date'] = pd.to_datetime(amazon_final_df['Review_Date']).dt.date
amazon_final_df['Review_Rating'] = amazon_final_df['Review_Rating'].astype('int64')
amazon_final_df['People_Find_Helpful'] = amazon_final_df['People_Find_Helpful'].fillna(0).astype('int64')
amazon_final_df['Scraping_Date'] =  pd.to_datetime(amazon_final_df['Scraping_Date']).dt.date
amazon_final_df.reset_index(inplace = True,drop = True)
amazon_final_df.sort_values(['Review_Date'],ascending = False) 
amazon_final_df['Review_Rating_Label'] = amazon_final_df['Review_Rating'].apply(lambda x: '1-2-3-star' if x < 4 else '4-5-star')
amazon_final_df


# %%
final_review = pd.concat([review_template, amazon_final_df])
final_review

# Save to CSV
final_review.to_csv(r'us.csv', index=False)


#marge amazon

es_df = pd.read_csv('es.csv')
header = es_df.columns

# Read the data from es.csv, uk.csv, and us.csv without headers
es_data = pd.read_csv('es.csv', header=0)  # Include header only for es.csv
uk_data = pd.read_csv('uk.csv', header=0)  # Exclude header for uk.csv
us_data = pd.read_csv('us.csv', header=0)  # Exclude header for us.csv

# Combine the data
combined_df = pd.concat([es_data, uk_data, us_data], ignore_index=True)

# Save the combined data with the header from es.csv
combined_df.to_csv('amazon.csv', index=False, header=header)

#marge amazon



#marge amazon

es_df = pd.read_csv('es.csv')
header = es_df.columns

# Read the data from es.csv, uk.csv, and us.csv without headers
es_data = pd.read_csv('es.csv', header=0)  # Include header only for es.csv
uk_data = pd.read_csv('uk.csv', header=0)  # Exclude header for uk.csv
us_data = pd.read_csv('us.csv', header=0)  # Exclude header for us.csv

# Combine the data
combined_df = pd.concat([es_data, uk_data, us_data], ignore_index=True)

# Save the combined data with the header from es.csv
combined_df.to_csv('amazon.csv', index=False, header=header)

#marge amazon

# Best buy hp


def get_review_bestbuy(url):
    extracted_reviews = []
    retry_count = 0
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Cookie': 'SID=5dd8d974-1010-4705-9db0-0091b9be90eb; bby_rdp=l; CTT=422cf77c62f741992b73b7eb194dd19d; intl_splash=false; intl_splash=false; vt=d36b7cc9-70f1-11ee-af65-0a4fc06e3e21; rxVisitor=169798943988975DRVD09AP9VHNKB488A7AMQ2ITCSNQ3; COM_TEST_FIX=2023-10-22T15%3A44%3A00.270Z; __gads=ID=6d604286666986e7:T=1697989449:RT=1697989449:S=ALNI_Mb_Z6tWUAT9d1smc0S2VYNtEXVnJQ; __gpi=UID=00000c6de2768122:T=1697989449:RT=1697989449:S=ALNI_MY8b96wWX_3ahxWOvsLcoQi2kpHIA; s_ecid=MCMID%7C51499273735922173403879288947271341352; AMCVS_F6301253512D2BDB0A490D45%40AdobeOrg=1; dtCookie=v_4_srv_5_sn_UKGS61LHKE95F58CKCJ5JTTUHNJV2N7D_app-3A1b02c17e3de73d2a_1_ol_0_perc_100000_mul_1; _cs_mk=0.5500628905410729_1697989446664; s_cc=true; AMCV_F6301253512D2BDB0A490D45%40AdobeOrg=1585540135%7CMCMID%7C51499273735922173403879288947271341352%7CMCAID%7CNONE%7CMCOPTOUT-1697996646s%7CNONE%7CMCAAMLH-1698594246%7C3%7CMCAAMB-1698594246%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCCIDH%7C1907712470%7CvVersion%7C4.4.0; aam_uuid=56460070521806806704392296716542884874; locDestZip=96939; locStoreId=1760; sc-location-v2=%7B%22meta%22%3A%7B%22CreatedAt%22%3A%222023-10-22T15%3A44%3A06.975Z%22%2C%22ModifiedAt%22%3A%222023-10-22T15%3A44%3A07.381Z%22%2C%22ExpiresAt%22%3A%222024-10-21T15%3A44%3A07.381Z%22%7D%2C%22value%22%3A%22%7B%5C%22physical%5C%22%3A%7B%5C%22zipCode%5C%22%3A%5C%2296939%5C%22%2C%5C%22source%5C%22%3A%5C%22G%5C%22%2C%5C%22captureTime%5C%22%3A%5C%222023-10-22T15%3A44%3A06.975Z%5C%22%7D%2C%5C%22destination%5C%22%3A%7B%5C%22zipCode%5C%22%3A%5C%2296939%5C%22%7D%2C%5C%22store%5C%22%3A%7B%5C%22storeId%5C%22%3A1760%2C%5C%22zipCode%5C%22%3A%5C%2299504%5C%22%2C%5C%22storeHydratedCaptureTime%5C%22%3A%5C%222023-10-22T15%3A44%3A07.380Z%5C%22%7D%7D%22%7D; __gsas=ID=43dc00dcffeab34e:T=1697989465:RT=1697989465:S=ALNI_MYLHkniZY8kqCiAFOeNu1jnR4mz0w; dtSa=-; cto_bundle=2D7FnF9ZMHJPQlFCbkdTMktUSFREZ2pVJTJGajJMRFFsd2lINnRNRkZxY0dFU1lqJTJCN0glMkZMU0FqRTR0UyUyRmZRa1FscDdyV0tQUTNZdzVBM1g2WkJHUENTUEdlaGtUdWtiZWU4allOYlc2dyUyRm1VeiUyRlVBZVZkdVRmSFElMkJZQ0ExRk9mZzZNV1VNd1ZYSXZ5RWZSeUFQdkJXZ3VxZzZJZyUzRCUzRA; blue-assist-banner-shown=true; _cs_c=1; _gcl_au=1.1.1372174147.1697989479; dtLatC=1; _abck=2025C1ED2DAE1BA19B91708C91F51C0F~0~YAAQHLQRYGhMakWLAQAAjTYLWgqyxn7G2wIoFoVC+4nrsT1cxJIaO1O5ytS58DrifnksxvYxu7oOIuZmBDszkeEGLUk/7ekIvtGFO7u2yogmIcW17juPvPSDc1XdGYIVbijt6PbXvKVWeAB+8ZIF6voDPAwIN8H+QKpGl7va06mSquCsIXDORvQ1fz6MaHlKajkG/g9N8gGFlrsBxnMpRA0vk4b7Xv9obYx0wvld8KvntBNHHmpIs0djlSe17djNQz57X3JJHstt9/StCh7Jo00MTiV93eKEGVBoMzoq4+PxnTdsrKg5PkI1bneUzJMSGuV43ZaXWfbm7uJ5sVfxdvHl0uQOQUh7ClSLpjFxe7sR9F6ZRsJ1uTIjK2Ab7WfvjLZd5C8V7/qZhg/oMP3pF0Dt09LThXO7tonFOvt8UhAETsU0Hw6+K/m4mS0wH46V+5rfa+qmNcM=~-1~-1~-1; bm_sz=DC447A131B862AC781959292B401C641~YAAQHLQRYGlMakWLAQAAjTYLWhWe27kjreKQmsKd+a2iqr9yFDHU3maKKvHTexZicnoFjIsx0OiZ03lAbfGOl2IZo7UNsbeBjNT3emSu3sSR0HUl0ddFd8LjnFGqQISSIw7upSTqhbE/Ccdgbo842X0fWkxXLQCXe7eIC5cgVWU1GMRdWc34I/WgCiVwaRV2v6j3I93rIKuMA5dYvCv2yQykBKCPPN4sbyl8TEvfZ+XgvWuziGVpb4G+3OBohzrz8/j7ZnhXQ1U0WZARKye28p1zLuSDfDk4mInPZlvumI5oeG13Z+CjpYEKf7D5iAjzcWRGlsQ32gejCk7aPI6RC1dkVBh/DL00bGUor4wdKjruVwQNpz0v3hop17nvb4BKkQIqQAfEL6zMaGHLj9ycBq93U+2b2AXxNlcKMzEYuQ0cL/PuJIlwGBiqjQ==~4601667~3422276; dtPC=5$589618119_27h-vVFAAHBSMMCTLEHFRWVQLIOPDRRSURPFR-0e0; rxvt=1698031868366|1698030068366; _cs_id=3fe9d270-9876-ad3a-cae4-0084c344a27c.1697989478.5.1698030169.1698030169.1645469968.1732153478774; _cs_s=1.0.0.1698031970357; c2=pdp%3A%20ratingsreviews',  # Replace with the actual Cookie
        'Downlink': '10',
        'Dpr': '1',
        'Referer': url,
        'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }

    response = requests.get(url, headers=header)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup 

def bestbuy_review(soup, url):    
    bestbuy = {}
    bestbuy_reviews = []  
    Model = soup.find("h1", {'class':"heading-5 v-fw-regular"})
    if not Model:
        Model = soup.find("h2", {"class": "heading-6 product-title mb-100"})
    Model = Model.text if Model else None
    
        
    npi = soup.find('span',{'class':'c-reviews order-2'} ).text
    review_session = soup.find_all("div", {"class": "review-item-content col-xs-12 col-md-9"})
    if review_session:
        for item in review_session:    
            bestbuy = {    
                 'Model':Model,
                # 'Review date': item.find("div", {"class": "posted-date-ownership disclaimer v-m-right-xxs"}).text.replace('Posted','')  
                'URL':url 
            }
            try:    
                bestbuy['Review title']  = item.find("h4", {"class": "c-section-title review-title heading-5 v-fw-medium"}).text  
            except AttributeError:    
                bestbuy['Review title']  = None

            try:
                bestbuy['Review_Name']  = item.find("div", {"class": "ugc-author v-fw-medium body-copy-lg"}).text  
            except AttributeError:    
                bestbuy['Review_Name']  = None
                

            try:    
                bestbuy['Review rating']  = item.find("div", {"class": "c-ratings-reviews flex c-ratings-reviews-small align-items-center gap-50"}).text.replace(' out of 5 stars','').replace('Rated ','')  
            except AttributeError:    
                bestbuy['Review rating']  = None

            review_date_element = item.find("time", {"class": "submission-date"})
            if review_date_element:
                review_date_string = review_date_element['title']
                review_date_datetime = datetime.strptime(review_date_string, '%b %d, %Y %I:%M %p')
                formatted_review_date = review_date_datetime.strftime('%Y-%m-%d')
                bestbuy['Review_Date'] = formatted_review_date
            else:
                bestbuy['Review_Date'] = ""

            try:    
                bestbuy['Review promotion']  = item.find("div", {"class": "body-copy-sm pt-50"}).text
            except AttributeError:    
                bestbuy['Review promotion']  = None

            try:    
                bestbuy['Review aggregation']  = item.find("p", {"class": "body-copy ugc-related-product"}).text
            except AttributeError:    
                bestbuy['Review aggregation']  = None

            try:    
                bestbuy['Review Content']  = item.find("div", {"class": "ugc-review-body"}).text  
            except AttributeError:    
                bestbuy['Review Content']  = None

            try:    
                bestbuy['Review Recommendation']  = item.find("div", {"class": "ugc-recommendation"}).text  
            except AttributeError:    
                bestbuy['Review Recommendation']  = None

            try:    
                network_badge  = item.find("div", {"class": "ugc-network-badge"})
                if network_badge:
                    bestbuy['Seeding or not'] = network_badge.get("data-track")
                else:
                    bestbuy['Seeding or not']  = ""
            except AttributeError:    
                bestbuy['Seeding or not']  = ""

            try:    
                bestbuy['People_find_helpful']  = item.find("button", {"data-track": "Helpful"}).text
            except AttributeError:    
                bestbuy['People_find_helpful']  = None

            try:    
                bestbuy['People_find_unhelpful']  = item.find("button", {"data-track": "Unhelpful"}).text
            except AttributeError:    
                bestbuy['People_find_unhelpful']  = None


            bestbuy_reviews.append(bestbuy)    
        
    
  
    return npi, bestbuy_reviews 

urls = ['https://www.bestbuy.com/site/reviews/hp-deskjet-2855e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6574145?variant=A']
       # 'https://www.bestbuy.com/site/reviews/hp-deskjet-4255e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6575024?variant=A']

# %%
max_attempts = 5
bestbuy_reviews = []

for link in urls:
    print(link)
    should_continue = True
    attempt_count = 0  # Counter for attempts
    for x in range(1, 100):
        if not should_continue:
            break
        while True:
            url = f'{link}&page={x}'
            try:
                soup = get_review_bestbuy(url)
                npi, reviews = bestbuy_review(soup, url)
                if npi == 'Be the first to write a review':
                    should_continue = False
                print(f'Extracted reviews on page {x}: {len(reviews)}')
                bestbuy_reviews.extend(reviews)

                next_page_link = soup.find("a", {"aria-disabled": "true"})  # Note: Use lowercase "true" for attribute value
                if x > 1 and next_page_link and next_page_link.get("aria-disabled") == "true":
                    should_continue = False
                    print('No more pages left')
                    break

                if len(reviews) < 20:
                    should_continue = False
                    print('Only 1 page')
                    break
                else:
                    break 
            except Exception as e:
                attempt_count += 1
                print(f"Error encountered: {e}. Retrying in 3 seconds... (Attempt {attempt_count}/{max_attempts})")
                if attempt_count >= max_attempts:
                    print("Maximum number of attempts reached. Exiting loop.")
                    should_continue = False
                    break
                time.sleep(3)



# %%
from datetime import date  
pd.set_option('display.max_columns', None)
review = pd.DataFrame(bestbuy_reviews)
review['Retailer']="Best Buy"
review['scraping_date'] = pd.to_datetime(date.today())

review['HP Model Number'] = review['Model'].str.extract(r'(\d+e*)')

hp_combine = pd.merge(review, df_amazon, on = "HP Model Number", how = "left" )

hp_combine['Review Model'] = hp_combine['HP Model'] 
hp_combine['People_find_helpful'] = hp_combine['People_find_helpful'].fillna(0).astype(str).str.extract(r'(\d+)').astype(int)
hp_combine['People_find_unhelpful'] = hp_combine['People_find_unhelpful'].fillna(0).astype(str).str.extract(r'(\d+)').astype(int)


columns_to_drop = [  
    'Model', 'HP Model Number', 'Comp Model number','HP Model'
]  
  
hp_combine_bestbuy = hp_combine.drop(columns_to_drop, axis = 1) 

hp_combine_bestbuy = hp_combine_bestbuy.drop_duplicates()

hp_combine_bestbuy




# %%
final_review = pd.DataFrame()
bestbuy_final = hp_combine_bestbuy
bestbuy_final.drop_duplicates(inplace = True)

bestbuy_final = bestbuy_final.sort_values(by = ['Review Model', 'Review title', 'Review Content', 'scraping_date'])

bestbuy_final['Competitor_Flag'] = bestbuy_final['Review Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
bestbuy_final['Country'] = 'US'

bestbuy_final_version = bestbuy_final.rename(columns={
    'Review date': 'Review_Date',
    'review_text': 'Review_Content',
    'Review rating': 'Review_Rating',
    'url': 'URL',
    'review_title': 'Review_Title',
    'Verified Purchase or not': 'Verified_Purchase_Flag',
    'reviewer_name': 'Review_Name',
    'syndication': 'Syndicated_Source',
    'stars': 'Review_Rating',
    'Retailer': 'Retailer',
    'scraping_date': 'Scraping_Date',
    'Comp Model': 'Comp_Model',
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Review title': 'Review_Title',
    'Review Content': 'Review_Content',
    'Review date': 'Review_Date',
    'URL': 'URL',
    'Seeding or not': 'Seeding_Flag',
    'Review name': 'Review_Name',
    'People_find_helpful': 'People_Find_Helpful',
    'Syndicated source': 'Syndicated_Source',
    'Comp Model': 'Comp_Model',
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Competitor_Flag': 'Competitor_Flag'
})

# Drop unnecessary columns
bestbuy_final_version.drop(columns=['Review Recommendation', 'People_find_unhelpful'], inplace=True)

# Save the DataFrame to a CSV file
bestbuy_final_version.to_csv('Bestbuy_NPI_review.csv', index=False)

# Concatenate the DataFrames
Final_review = pd.concat([final_review, bestbuy_final_version], ignore_index=True)

# Convert data types
Final_review['Review_Date'] = pd.to_datetime(Final_review['Review_Date']).dt.date
Final_review['Review_Rating'] = Final_review['Review_Rating'].astype('int64')
Final_review['People_Find_Helpful'] = Final_review['People_Find_Helpful'].fillna(0).astype('int64')
Final_review['Scraping_Date'] = pd.to_datetime(Final_review['Scraping_Date']).dt.date

# Create Review_Rating_Label column
Final_review['Review_Rating_Label'] = Final_review['Review_Rating'].apply(lambda x: '1-2-3-star' if x < 4 else '4-5-star')

# Define the required columns
required_columns = [
    'Review_Model', 'Competitor_Flag', 'HP_Class', 'Segment', 'Retailer',
    'Comp_Model', 'Review_Date', 'Review_Name', 'Review_Rating',
    'Review_Rating_Label', 'Review_Title', 'Review_Content', 'Seeding_Flag',
    'Verified_Purchase_Flag', 'Promotion_Flag', 'Aggregation_Flag',
    'People_Find_Helpful', 'Syndicated_Source', 'Response_Date',
    'Response_Text', 'Response_Name', 'URL', 'Scraping_Date', 'Country',
    'Orginal_Title', 'Orginal Title'
]

# Ensure all required columns are present
for col in required_columns:
    if col not in Final_review.columns:
        Final_review[col] = None  # or use an appropriate default value

# Select only the required columns
Final_review = Final_review[required_columns]

# Save the final DataFrame to a CSV file
Final_review.to_csv('bestbuy.csv', index=False)

# Display the unique Scraping_Date values (optional)
print(Final_review['Scraping_Date'].unique())

# # Walmart
# Maximum number of restarts allowed
# Maximum number of restarts allowed
MAX_RETRIES = 30

# Step 0: Set up virtual display for headless mode
def setup_virtual_display():
    """Set up a virtual display using Xvfb."""
    display = Display(visible=0, size=(1920, 1080))  # Invisible display
    display.start()  # Start the virtual display
    print("Virtual display started.")
    return display

# Step 1: Set up undetected ChromeDriver with stealth techniques and inject cookies before loading URL
def setup_selenium(cookies):
    """Set up undetected Chrome WebDriver with stealth techniques, inject cookies, and then visit the page."""
    options = uc.ChromeOptions()

    # Set headless mode to run without showing any GUI
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("window-size=1920,1080")
    options.add_argument("--remote-debugging-port=9222")

    print("ChromeDriver options configured successfully for headless mode.")

    # Start the Chrome WebDriver using undetected-chromedriver
    try:
        driver = uc.Chrome(options=options)
        time.sleep(5)  # Wait for ChromeDriver to initialize
        print("ChromeDriver initialized successfully.")
    except Exception as e:
        print(f"Error initializing ChromeDriver: {e}")
        return None

    # Open a blank page to inject cookies before loading the actual URL
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
    # Finding all review blocks (adjust class based on actual Walmart page structure)
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
        # Extracting the review rating
        review_rating_element = li_tag.select_one('.w_iUH7')
        product['Review rating'] = review_rating_element.text if review_rating_element else None

        # Checking if it's a verified purchase
        verified_purchase_element = li_tag.select_one('.pl2.green.b.f7.self-center')
        product['Verified Purchase or not'] = verified_purchase_element.text if verified_purchase_element else None

        # Extracting the review date
        review_date_element = li_tag.select_one('.f7.gray')
        product['Review date'] = review_date_element.text if review_date_element else None

        # Extracting the review title
        review_title_element = li_tag.select_one('.w_kV33.w_Sl3f.w_mvVb.f5.b')
        product['Review title'] = review_title_element.text if review_title_element else None

        # Extracting the review content
        review_content_element = li_tag.select_one('span.tl-m.db-m')
        product['Review Content'] = review_content_element.text.strip() if review_content_element else None

        # Extracting the reviewer's name
        review_name_element = li_tag.select_one('.f7.b.mv0')
        product['Review name'] = review_name_element.text if review_name_element else None

        # Extracting syndicated source,
        syndication_element = li_tag.select_one('.flex.f7 span.gray')
        if syndication_element and 'Review from' in syndication_element.text:
            product['Syndicated source'] = syndication_element.text.split('Review from ')[-1].strip()
        else:
            product['Syndicated source'] = None  # Assign None if no syndicated source is found

        # Correctly specify the button's aria-label as it appears in your HTML snippet
        helpful_element = soup.select_one('button[aria-label^="Upvote ndmomma review"] span')
        
        # Extract the number of people who found the review helpful
        people_find_helpful = int(helpful_element.text.strip('()')) if helpful_element else 0

        # Adding the URL of the review
        product['URL'] = url

        # Append the extracted product information to the list of reviews
        extracted_reviews.append(product)

    return extracted_reviews
# Step 3: Fetch Reviews for a specific page
def fetch_reviews_for_page(driver, url):
    """Fetch reviews for a specific page."""
    try:
        driver.set_page_load_timeout(120) 
        driver.get(url)
        random_sleep()  # Random delay to wait for the page to load

        # Get the page source after it has fully loaded
        page_html = driver.page_source

        # Parse the page using BeautifulSoup
        soup = BeautifulSoup(page_html, 'html.parser')

        # Extract the last page number from the pagination section
        page_links = soup.find_all('a', {'data-automation-id': 'page-number'})
        
        page_numbers = []
        
        # Loop through the links and extract the page number text
        for link in page_links:
            text = link.get_text(strip=True)
            if text.isdigit():  # Check if the text is a number
                page_numbers.append(int(text))  # Convert the text to an integer and add to list
        
        if page_numbers:
            last_page_number = max(page_numbers)  # Get the largest page number
            print(f"Last page number detected: {last_page_number}")
        else:
            print("No page links found, assuming only 1 page.")
            last_page_number = 1  # Default to 1 if no pages are found

        # Extract reviews using BeautifulSoup
        reviews = extract_reviews(page_html, url)

        if reviews:
            print(f"Successfully extracted {len(reviews)} reviews.")
            return reviews, last_page_number  # Return the extracted reviews and last page number
        else:
            print("No reviews found on this page.")
            return [], last_page_number

    except Exception as e:
        print(f"Error during review fetching: {e}")
        return [], 1  # Default to page 1 if an error occurs




# Step 5: Fetch All Reviews (with pagination handling and restart logic)
def fetch_all_reviews(url, cookies, retry_count=0):
    """Main function to scrape reviews from all pages and keep driver alive. Restarts if no reviews are found."""
    all_reviews = []
    page = 1

    driver = setup_selenium(cookies)
    if driver is None:
        return all_reviews  # Exit if the driver could not be set up

    # Fetch the last page number using the driver
    _, last_page = fetch_reviews_for_page(driver, url)

    while page <= last_page:
        print(f"Fetching page {page}...")

        # Construct the URL for the current page
        page_url = f"{url}?page={page}"

        # Fetch reviews for the current page
        reviews, _ = fetch_reviews_for_page(driver, page_url)

        # If no reviews were found, attempt a restart
        if not reviews:
            if retry_count < MAX_RETRIES:
                print(f"Restarting script, attempt {retry_count + 1} of {MAX_RETRIES}...")
                driver.quit()  # Close the current driver
                return fetch_all_reviews(url, cookies, retry_count + 1)  # Restart from page 1
            else:
                print(f"No more reviews found at page {page}. Max retries reached.")
                break

        # Add reviews to the total list
        all_reviews.extend(reviews)
        print(f"Reviews extracted from page {page}: {len(reviews)}")

        # Increment the page counter
        page += 1

        # Random sleep to avoid detection
        random_sleep()

    driver.quit()  # Close the driver when all reviews have been scraped
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

# Step 0: Start virtual display
display = setup_virtual_display()

for url in urls:
    walmart_reviews.extend(fetch_all_reviews(url, cookies))

# Step 8: Convert Reviews to DataFrame and Save to CSV
walmart = pd.DataFrame(walmart_reviews)
walmart['Retailer'] = "Walmart"
walmart['scraping_date'] = date.today().strftime('%Y/%m/%d')
walmart['scraping_date'] = pd.to_datetime(walmart['scraping_date']).dt.date
walmart['Review date'] = pd.to_datetime(walmart['Review date']).dt.date
walmart['Review rating'] = walmart['Review rating'].astype(str).str.replace(' out of 5 stars review', '').astype(int)
walmart.drop_duplicates(inplace=True)

walmart['HP Model Number'] = walmart['Model'].str.extract(r'(\d+e?)')

walmart['Review date'] = pd.to_datetime(walmart['Review date'])

walmart_hp_combine = pd.merge(walmart, df_amazon, on="HP Model Number", how="left")
walmart_hp_combine['Review Model'] = walmart_hp_combine['HP Model']

columns_to_drop = ['Model', 'HP Model Number', 'Comp Model number', 'HP Model']
walmart_hp_combine = walmart_hp_combine.drop(columns_to_drop, axis=1)

walmart_hp_combine = walmart_hp_combine.drop_duplicates()
walmart_hp_combine['Competitor_Flag'] = walmart_hp_combine['Review Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
walmart_hp_combine['Country'] = 'US'

column_mapping = {
    'Review date': 'Review_Date',
    'review_text': 'Review_Content',
    'Review rating': 'Review_Rating',
    'url': 'URL',
    'review_title': 'Review_Title',
    'Verified Purchase or not': 'Verified_Purchase_Flag',
    'reviewer_name': 'Review_Name',
    'syndication': 'Syndicated_Source',
    'stars': 'Review_Rating',
    'Retailer': 'Retailer',
    'scraping_date': 'Scraping_Date',
    'Comp Model': 'Comp_Model',
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Review title': 'Review_Title',
    'Review Content': 'Review_Content',
    'Review date': 'Review_Date',
    'URL': 'URL',
    'Seeding or not': 'Seeding_Flag',
    'Review name': 'Review_Name',
    'People_find_helpful': 'People_Find_Helpful',
    'Syndicated source': 'Syndicated_Source',
    'Comp Model': 'Comp_Model',
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Competitor_Flag': 'Competitor_Flag'
}

# Rename columns in the original DataFrame
walmart_hp_combine = walmart_hp_combine.rename(columns=column_mapping)

# Concatenate with an empty DataFrame
Final_review = pd.concat([pd.DataFrame(), walmart_hp_combine], ignore_index=True)

# Add default values for some columns
Final_review['Country'] = 'US'
Final_review['Review_Date'] = pd.to_datetime(Final_review['Review_Date']).dt.date
Final_review['Review_Rating'] = Final_review['Review_Rating'].astype('int64', errors='ignore')
Final_review['Review_Rating_Label'] = Final_review['Review_Rating'].apply(lambda x: '1-2-3-star' if x <4 else '4-5-star') 
# Handle missing 'People_Find_Helpful' column
if 'People_Find_Helpful' in Final_review.columns:
    Final_review['People_Find_Helpful'] = Final_review['People_Find_Helpful'].fillna(0).astype('int64')
else:
    Final_review['People_Find_Helpful'] = 0

Final_review['Scraping_Date'] = pd.to_datetime(Final_review['Scraping_Date']).dt.date



# Fill NaN values in string columns with empty string
string_columns = Final_review.select_dtypes(include='object').columns
Final_review[string_columns] = Final_review[string_columns].fillna('')

# Ensure all required columns are present
required_columns = [
    'Review_Model', 'Competitor_Flag', 'HP_Class', 'Segment', 'Retailer',
    'Comp_Model', 'Review_Date', 'Review_Name', 'Review_Rating',
    'Review_Rating_Label', 'Review_Title', 'Review_Content', 'Seeding_Flag',
    'Verified_Purchase_Flag', 'Promotion_Flag', 'Aggregation_Flag',
    'People_Find_Helpful', 'Syndicated_Source', 'Response_Date',
    'Response_Text', 'Response_Name', 'URL', 'Scraping_Date', 'Country',
    'Orginal_Title', 'Orginal Title'
]

for col in required_columns:
    if col not in Final_review.columns:
        Final_review[col] = None

# Reorder columns to match the required_columns list
Final_review = Final_review[required_columns]

previous = pd.read_csv(r'Tassel_EMEA_Review_Raw.csv')
previous['Review_Date'] = pd.to_datetime(previous['Review_Date'], errors='coerce')
# Convert 'Scraping_Date' to datetime, specifying the correct format
previous['Scraping_Date'] = pd.to_datetime(previous['Scraping_Date'], format='%Y-%m-%d', errors='coerce').dt.date
previous ['Review_Rating'] = previous['Review_Rating'].astype(int)



def clean_review(text):
    text = str(text)

    cleaned_text = re.sub(r'Media(?: content)? could not be loaded\.?', ' ', text).strip()
    return cleaned_text

#previous['Review_Content'] = previous['Review_Content'].apply(clean_review)
Final_review['Review_Content'] = Final_review['Review_Content'].apply(clean_review)

# def extract_first_ten_words(row):
#     words = row.split()
#     return ''.join(words[:10])


def clean_text(text):
    text = str(text)

    # Remove non-English characters and punctuations
    cleaned_text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    # Remove extra whitespaces and convert to lowercase
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip().lower()
    cleaned = re.sub(r'Media(?: content)? could not be loaded\.?', ' ', cleaned_text)
    english_words = re.findall(r'\b[a-z]+\b', cleaned)
    first_ten_words = ''.join(english_words[:10])
    return first_ten_words


# Print the total number of reviews
print('Total walmart review:', len(Final_review))



# Save to CSV
Final_review.to_csv('walmart.csv', index=False)

# Read the CSV file into a DataFrame
df = pd.read_csv('walmart.csv')

# Drop rows where the 'Review_Content' column is blank
df_cleaned = df.dropna(subset=['Review_Content'])

# Save the cleaned DataFrame back to a CSV file
df_cleaned.to_csv('walmart.csv', index=False)

# Read es.csv, uk.csv, and us.csv
es_data = pd.read_csv('es.csv')
uk_data = pd.read_csv('uk.csv')
us_data = pd.read_csv('us.csv')

# Add columns "Orginal_Review" and "Orginal_Title" to uk_data
uk_data['Orginal_Review'] = ""
uk_data['Orginal_Title'] = ""

# Concatenate es_data, uk_data, and us_data
combined_df = pd.concat([es_data, uk_data, us_data], ignore_index=True)

# Remove the column "Orginal_Review"
combined_df.drop(columns=['Orginal_Review'], inplace=True)

# Save the combined DataFrame to a new CSV file
combined_df.to_csv('amazon.csv', index=False)



# Function to read CSV without header
def read_csv_without_header(file_path, column_names):
    df = pd.read_csv(file_path, skiprows=1, header=None)
    df.columns = column_names[:len(df.columns)]  # Assign column names based on the number of columns in the file
    return df

# List of files to merge
file_paths = ['amazon.csv', 'bestbuy.csv', 'walmart.csv']

# Read the CSV files without headers
dfs = [read_csv_without_header(file_path, required_columns) for file_path in file_paths]

# Concatenate the DataFrames
final_df = pd.concat(dfs, ignore_index=True)

# Handle missing 'People_Find_Helpful' column
if 'People_Find_Helpful' in final_df.columns:
    final_df['People_Find_Helpful'] = final_df['People_Find_Helpful'].fillna(0).astype('int64')
else:
    final_df['People_Find_Helpful'] = 0


# Ensure all required columns are present and in the correct order
for col in required_columns:
    if col not in final_df.columns:
        final_df[col] = None

final_df = final_df[required_columns]

# Save the final DataFrame to a CSV file
final_df.to_csv('merged_reviews.csv', index=False)

# Load the two CSV files
merged_reviews = pd.read_csv('merged_reviews.csv')
tassel_emea_review_raw = pd.read_csv(r'Tassel_EMEA_Review_Raw.csv')

# Combine the data, keeping the header of Tassel_EMEA_Review_Raw.csv
combined_data = pd.concat([tassel_emea_review_raw, merged_reviews], ignore_index=True)

# Save the combined data to Tassel_EMEA_Review_Raw.csv
combined_data.to_csv(r'Tassel_EMEA_Review_Raw.csv', index=False)

# # # Remove the original CSV files
# # for file_path in file_paths:
# #     os.remove(file_path)

df = pd.read_csv(r'Tassel_EMEA_Review_Raw.csv')

# Function to get the first character of review content
def first_character(content):
    if pd.isna(content):  # Check if content is NaN
        return content
    return content[:10]

# Apply the function to create a new column with the first character
df['Review_content_first_char'] = df['Review_Content'].apply(first_character)

# Identify duplicates based on 'Review_Name' and 'Review_content_first_char'
duplicates = df.duplicated(subset=['Review_Name', 'Review_content_first_char'], keep='first')

# Keep the first occurrence of duplicates and rows with blank 'Review_Content'
df_no_duplicates = df[~(duplicates & ~df['Review_Content'].isnull())]

# Drop the temporary column
df_no_duplicates = df_no_duplicates.drop(columns=['Review_content_first_char'])

# Save the result to a new CSV file
df_no_duplicates.to_csv(r'Tassel_EMEA_Review_Raw.csv', index=False)

print('Tassel_raw_data_scraping completed. Tassel_raw file saved')