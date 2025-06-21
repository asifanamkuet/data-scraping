# %%
import pandas as pd
import re
from datetime import date 
from datetime import datetime
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
import openpyxl
import urllib.parse
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlparse



# Disable urllib3 warnings
urllib3.disable_warnings(InsecureRequestWarning)

# Get the current date and time
now = datetime.now()

# Format the timestamp as a string
timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

# Print the timestamp
print("Current Timestamp:", timestamp)

print('Running MMK_raw_date_scraping.py')

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

# %% [markdown]
# # Amazon

# %% [markdown]
# ## Function

# %%
### Not use docker

# header = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#         'Accept-Encoding': 'gzip, deflate, br',
#         'Accept-Language': 'en-US,en;q=0.9',
#         'Cache-Control': 'max-age=0', 
#         'Downlink': '10',
#         'Dpr': '1',
#         'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
#         'Sec-Ch-Ua-Mobile': '?0',
#         'Sec-Ch-Ua-Platform': '"Windows"',
#         'Sec-Fetch-Dest': 'document',
#         'Sec-Fetch-Mode': 'navigate',
#         'Sec-Fetch-Site': 'same-origin',
#         'Sec-Fetch-User': '?1',
#         'Upgrade-Insecure-Requests': '1'
#     }

# url ='https://www.amazon.com/HP-DeskJet-2755e-Wireless-Printer/product-reviews/B08XYP6BJV/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
# response = requests.get(url, headers=header)
# response.raise_for_status()
# soup = BeautifulSoup(response.text, 'html.parser')
# soup.find_all("div", {"data-hook": "review"})

# %%
from datetime import datetime

#docker section
# def get_soup(url):   
#     r = requests.get('http://localhost:8050/render.html', params={'url': url, 'wait': 2})  
#     soup = BeautifulSoup(r.text, 'html.parser')  
#     return soup  

def get_soup_amazon(url):
    parsed_url = urlparse(url)
    host = parsed_url.netloc
    headers = {
        "Host": host,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    if host == 'www.amazon.co.uk':
        cookies = {
            "id_pkel": "n0",
            "session-id": "260-4298792-7658758",
            "ubid-acbuk": "258-6971084-0540716",
            "at-acbuk": "Atza|IwEBIP_rct34sEzoIRoVA3yCl9iNVj1vAkz4UScATDbZbC-hmPsIBEWMVfxD5DK4JdS4e0q-B3snMwPGDAZiSNULMr19y8lh0JspbVm_IlbF3ansTJ7ocxWUxcMeBXFZeg80bpT8FJalwoGBUfEN4RTParyxnZFPtfYCI7qk53UkgAfXJYqlWpxVVYNjihc38BEOBWWYwU2Jqqw4LMoWayecXYwZxragFHKmjCApjny15SChsA",
            "sess-at-acbuk": "\"U0kkcZUX2VEJfWcYvHDR+3X7nAkcpnfTMO6xUCoGZPw=\"",
            "sst-acbuk": "Sst1|PQF7uChzfDOz9iDl5ODdyi0jCceK_CQiKZcZ3Wbz-GiUtWitVPEKjUjMFSmItS6pnSHcV8mraZvRzfSgJnkzhno8Ae1vwqF-cUJR_967Ldn2X42v7IqdxlPpCQT7kZVe4AYaLP7_rmpojkWo0ro13PqK8R0k8Lmps_HZuiLc2y60SnVSE9jhrGd6GYZrNzgXlr7-IVZGn9xw-0s_U3RNJkagUPYraLKUty7Ij8vUc4YTjy6v9Pqunw-jxK3n4_FLehumir8j0ZDyH1mbjlMk3pRb0ROqDgbZT8sO3My5P3QxvMo",
            "session-id-time": "2082787201l",
            "x-acbuk": "\"7tyUa0SXz2iP93WG9IBc5lnA2RSDxZS3GfL2ZeriqVieDKMLCztGBUgquMV0axF@\"",
            "i18n-prefs": "GBP",
            "lc-acbuk": "en_GB",
            "sp-cdn": "\"L5Z9:IN\"",
            "session-token": "/l9KYAtgAfZIZDTr0iecZc/IpcgnmXUKW6DzLGsEoaXqRucpzG9ZNA4HmwO2ZhVWusplDqSCMtsshKKUMQk3USzit3k3s283i1UDZk4PQMpF2UzdTDWyMGmcpgy5lR0yemgFEYAO8ODnMEmqYGOKwCxTKwRDzbk4auO/MkyaPvJAcJrC17YgER44Q141xQKSepf7Wr6s2S9DnZXiPMujW2iZcquia0U7HRGhg29r18/+Cz5Zng6R5lf7XEJZDoaMtbKKps+XbgwS+YD8uRB9OJP42eVWxx0Mh5MWHw6pQBt3bj77U4/RmeNbdnTgMx+/a2SmfHXsS8EH6j3WrX34DusILMs/15fkMLJ9NQVv0UzKYg2A2BcM9pzS3G/+x6gu"
        }
    elif(host == 'www.amazon.es'):
        cookies = {
        "id_pkel": "n0",
        "session-id": "259-2556844-3879143",
        "ubid-acbes": "259-0682713-2972732",
        "session-token": "\"j0uY88Xhi3bqvARsrwoCvUwDXSsk54Z6KXEC0p3nqqn+oWm93ZVq8aM6svPCzGU5a02BCCyHxQmkYjQXaJCbeBkjPoXQ9UOq3QvR7uNQGyqfW5cmNxwMTJzcbvd/WeJOF88+K4tFofxgJwuGZfWOpL//bgQlgMWokVW26WZ9Z3Bv6eIG7rdB8VwDqpWOqKSZ5FWmcHm5LjxNDPE3L7mXRUtSkQWgwdjLhl3aeH44THDWO6+2XuwB2qMJwYbHS1fR1PhQ+0GTzDxM2ZlW0WaOA6T5r9UiUekPaEigH3mynOAz3RdiS29woM/TeoF64DQi/NONY2BApK9QNPHyEAip+bI2MqPqBpcXrpOlg2RuZOoEapaEO9OR2g==\"",
        "x-acbes": "\"JyDSBpmaQLvMnoNd@xnPdLsgiVJliu7cBhz0K9xA9dNJhky09MN?MqaZllflC?Cm\"",
        "at-acbes": "Atza|IwEBILVWijl6PCPAFk_wimanCI5HB5wcswC7umy_12IO9b1h9YN2rTAvxrruf0B7sBgniKfalFE3TDPNRUN9XaNqXRZd7I2k47QkgPs7LP3bsIr9-7CtvbhzH7wKkNVa7pPxzryb4Clz-sgH2F9KIqdF-Vwu2BNxCYhsUZmP8Al40bIggb897UHf9CoSy3VKqeRDSn6gUWCSCgICPEJUuyOVi19qRMdXVbpSlsEVunTUxuobVQ",
        "sess-at-acbes": "\"8hl/o7uIRofZyD1wGq5JYBUvlMaEn1e1oPpL+PEz6ZU=\"",
        "sst-acbes": "Sst1|PQHaKUE6V8mvs7u4faCwHDpzCTMgNC6CLHInD6RsB9MmRoPjEZSWOdl7sQEK7hZ6gOA88j-M_nfWejx60k9FFKLC6m3Z06mzfzoIR3w4tXMJ69SWudOqIEzmCavvPsYj6RvDW8bqP3gphKuh4GIXWFyOfZ0Nz_XXyCCzOH5RI3RRuL_tCps4AkwuzHRAGIpD4ZJFWf5fF2zJNhAMdhQ8DGyLvoLDqa6cVU_9qSKwyFXREPzPtyNLu89-bQJAOX_qJXOZPI1t5qG2bmf7cogZnyeFtPqgokvvitYPswOuv_l-PqI",
        "session-id-time": "2082787201l",
        "i18n-prefs": "EUR",
        "lc-acbes": "es_ES"
    }
    else:
        cookies = {
        "at-main": "Atza|IwEBILCkGKez-bDPkyK0rOA-46d-88vlSz7iC8zSWdEiSLs_r5NlyuNiCo4NoYo8pOtMLyWrOq-4gOs7rl-gHuz8xNhr2ce2isTgfDLm10yWkv3Bb8wnAkCDO8B9otdWa6lc-4p95eJOVtueLtz8Vw_XWyZ5hJXZzphOM5UAoZZ5x3EueI56ClWXr04WmGHygiFwaekYbNqau7gLyZQBFeg4fXsN2bUsumMCc8K25AmBv3wg0A",
        "sess-at-main": "\"VtSfvkuHHDzklDLJNIJWnTv1vX0k8K0IlItvYEb0GLo=\"",
        "sst-main": "Sst1|PQFkpgtkYvuh30UDcQDVUmdRCbB-NbcfJ3Et1IJ7zvhMFDgbqZbrmApcoR4Zy3DEA4yEwiCduu26P9aZNYkAY8be_yT3HMRe6flKd499jfNQ5DVTWWi9BcRY6nACHh-qDtc74kvKfmHa6qWfafwhRuYUfbT316Z6qHKFSgpS1Kz7Ta3PDbe1YWaWdEwc-AIKPu2497ympmZcZn89OU4BdFX71KDDhCENXZNRzYw6xdASs40P7iUL3Y69QFI-mr6wN_E5ZybkNsf9iHMpBqYJt9kukTRWg1lXrn0uaMqZPP1hDZg",
        "x-main": "\"Fa?@jbVer5IDB1p5XKuF7HGYTxWyoJuSGsOMWOuZd4Pcq0TBbYAMXT7OXi2zhmCt\"",
        "i18n-prefs": "USD",
        "lc-main": "en_US",
        "session-token": "\"RRt0+TjE+kscTjhb80ckzUFtCrD34JY8El2Fg84cTIa4CFWwXrhNH1EoCr/2fAnUSGb5n2G07+yQaaq3URgY7Q8HNkLsNgw4BDvPTHnSoipearc8BXsIRlH+YbLz1wYDbduxJKasPwJ6+sKJZ2Gzuxrfj9b0R86ygc6kdz/7ZRJPexHbZ6V+HTwsMhxhIqGyhINbg06EJlHq8rXtWnqwR854bBimHoPbbtzTErY47sEnvYyPjBWwPQqt7umXkHNkAp0BbcWQOA8NghFsAvvxLWjmxC8zn8KTT8zqHe1slAWGfv95XgKhzOxrxQnBw3t2iE/qEB3lzS58dQIEujFfnsxqyESp66vLwfDbQKiY7bFfcJAKxsO3pQ==\"",
        "ubid-main": "132-4010737-0455008",
        "session-id": "139-2733986-5232362",
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
    

    # NPI lanched in 2024-01-15
    date_string = "2024-01-15"
    min_date = datetime.strptime(date_string, "%Y-%m-%d")

    for item in reviews:    
        review_date_string = item.find('span', {'data-hook': 'review-date'}).text.replace('Reviewed in', '').split('on')[1].strip()
        parsed_url = urlparse(url)
        host = parsed_url.netloc
        
        if(host == 'www.amazon.co.uk'):
            review_date = datetime.strptime(review_date_string, "%d %B %Y")
        elif(host == 'www.amazon.es'):
            review_date = datetime.strptime(review_date_string, "%d %B %Y")
        else:
            review_date = datetime.strptime(review_date_string, "%B %d, %Y")
        if review_date < min_date:
            print('Review date is less than 2024-01-15')
            break
    
        review = {    
            'Model': model,    
            'Review date': review_date,     
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
                review['Seeding or not'] = item .find('span', {'class': 'a-color-success a-text-bold'}, text='Vine Customer Review of Free Product')

            except AttributeError:
                review['Seeding or not'] = None

        try:
            review['Aggregation'] = item.find("a", {"data-hook": "format-strip"}).text.strip()
        except AttributeError:   
             review['Aggregation'] = None
    
  
        extracted_reviews.append(review)    
    
  
    return extracted_reviews

# %% [markdown]
# ## HP Review

# %%
urls = ['https://www.amazon.co.uk/product-reviews/B0CJY274VN/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format', 
        'https://www.amazon.com/product-reviews/B0CFM7BTW8/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format',
        'https://www.amazon.com/product-reviews/B0CFM82NS2/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format',
        'https://www.amazon.com/product-reviews/B0CFM7VJNK/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format',
        'https://www.amazon.com/product-reviews/B0CFM8KL9G/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format',
        'https://www.amazon.com/product-reviews/B0CFM94G5H/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format',
        'https://www.amazon.co.uk/product-reviews/B0CJY274VN/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format']



# %%
import datetime 
from datetime import datetime
star = ['one', 'two', 'four','five'] 
max_retry_attempts = 2
all_reviews = []

for link in urls:
    print(link)
    for y in star:
        found_reviews = True
        for page in range(1, 11):
            retry_attempts = 0
            while found_reviews is True:
                try:
                    url = f'{link}&filterByStar={y}_star&pageNumber={page}&sortBy=recent'  
                    print('Page:',page, f'{y} star')
                    soup = get_soup_amazon(url)  # Get the soup object from the URL
                    extracted_reviews = amazon_review(soup, url)  # Extract reviews from the soup
                   
                    if soup.find('div', {'class': 'a-section a-spacing-top-large a-text-center no-reviews-section'}):  
                            print('No review')  
                            found_reviews = False
                            break 
                    
                    if len(extracted_reviews) > 0:
                        all_reviews.extend(extracted_reviews)
                        print(f"Page {page} scraped {len(extracted_reviews)} reviews")
                    
                    # if (page == 1 and len(extracted_reviews) == 0):
                    #     print(f"Page {page} has no reviews, retry")
                    #     continue
                        
                    if soup.find('li', {'class': 'a-disabled a-last'}):  
                        print('No more pages left')  
                        found_reviews = False
                        break 
                    
                    if page >= 1 and len(extracted_reviews) == 0:
                        retry_attempts += 1
                        if retry_attempts == max_retry_attempts:
                            found_reviews = False
                            print(f"Page {page} has no reviews, moving to the next page")
                            break
                        else:
                            print(f"Page {page} has no reviews, retry")
                            continue 

                    
                            
                    else:
                        break  
        
                    

                except Exception as e:
                    print(e)
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
            
           



# %%
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


# %% [markdown]
# ## Convert to dataframe

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

amazon_final['Competitor_flag'] = amazon_final['Review Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
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
amazon_final_df.head()

# amazon_final_df.to_csv(r'amazon_review.csv',index = False)

# %%
final_review = pd.concat([review_template, amazon_final_df])
final_review.head()

# %%
# # Query previous amazon review
# from sqlalchemy import create_engine

# server = 'SQL-Cluster01.ijp.sgp.rd.hpicorp.net'
# database = 'STAR_Rating'
# schema = 'dbo'
# driver = 'ODBC Driver 17 for SQL Server'

# # dataframe = amazon_final_df
# table = "Ink_web_reviews"

# engine = create_engine(f"mssql+pyodbc://{server}/{database}?driver={driver}", echo=True)


# existing_rows_query = f"""
#     SELECT *
#     FROM {schema}.{table}
#     WHERE Retailer in ('Amazon')
# """
# result_df = pd.read_sql_query(existing_rows_query, engine)



# %%
# result_df['Review_Date'] = result_df['Review_Date'].dt.date

# non_duplicated_df = amazon_final_df[(~amazon_final_df['Review_Date'].isin(result_df['Review_Date']))&
#                                    (~amazon_final_df['Review_Content'].isin(result_df['Review_Content']))].drop_duplicates()
# non_duplicated_df

# %%
# from sqlalchemy import create_engine, text
 
# server = 'SQL-Cluster01.ijp.sgp.rd.hpicorp.net'
# database = 'STAR_Rating'
# schema = 'dbo'
# driver = 'ODBC Driver 17 for SQL Server'

# dataframe = non_duplicated_df
# table = "Ink_web_reviews"

# engine = create_engine(f"mssql+pyodbc://{server}/{database}?driver={driver}", echo=True)
 
# chunk_size = 10000
# total_rows = len(dataframe)
# num_chunk = (total_rows + chunk_size - 1) // chunk_size

# for i in range(num_chunk):
#     start_index = i * chunk_size
#     end_index = (i + 1) * chunk_size
#     chunk = dataframe.iloc[start_index:end_index]
    
#     chunk.to_sql(table, engine, index=False, if_exists="append", schema="dbo")
#     print(f"Chunk {i+1}/{num_chunk} saved to SQL.")

# %%
# ## Change column setting (Rating to one decimal place)
# update_query = f'''
#  ALTER TABLE dbo.Ink_web_reviews
#  ALTER COLUMN Review_Date DATE
#  '''
# with engine.connect() as connection:
#         connection.execute(update_query)

# print("Precision updated in SQL table.")

# %% [markdown]
# # Bestbuy

# %%
from datetime import datetime
def get_review_bestbuy(url):
    extracted_reviews = []
    retry_count = 0
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.61',
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

# %% [markdown]
# ## Best buy hp

# %%
# ### Novellie
# urls = [
# 'https://www.bestbuy.com/site/reviews/hp-envy-inspire-7255e-wireless-all-in-one-inkjet-photo-printer-with-3-months-of-instant-ink-included-with-hp-white-sandstone/6492187?variant=A&sort=MOST_RECENT',
# 'https://www.bestbuy.com/site/reviews/hp-envy-inspire-7955e-wireless-all-in-one-inkjet-photo-printer-with-3-months-of-instant-ink-included-with-hp-white-sandstone/6478251?variant=A&sort=MOST_RECENT'

# ]

# %%
urls = ['https://www.bestbuy.com/site/reviews/hp-officejet-pro-8135e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6565476?variant=A'
# 'https://www.bestbuy.com/site/reviews/hp-officejet-pro-9125e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6565475?variant=A',
# 'https://www.bestbuy.com/site/reviews/hp-officejet-pro-9135e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6565473?variant=A',
#         'https://www.bestbuy.com/site/reviews/hp-officejet-pro-8139e-wireless-all-in-one-inkjet-printer-with-12-months-of-instant-ink-included-with-hp-white/6565474?variant=A',
#         'https://www.bestbuy.com/site/reviews/hp-officejet-pro-9730e-wireless-all-in-one-wide-format-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6578444?variant=A'
]

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
bestbuy_final = hp_combine_bestbuy
bestbuy_final.drop_duplicates(inplace = True)

bestbuy_final = bestbuy_final.sort_values(by = ['Review Model', 'Review title', 'Review Content', 'scraping_date'])

bestbuy_final['Competitor_Flag'] = bestbuy_final['Review Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
bestbuy_final['Country'] = 'US'

bestbuy_final_version= bestbuy_final.rename(columns={
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model',
    'Retailer': 'Retailer',
    'Comp Model': 'Comp_Model',
    'Review_Date': 'Review_Date',
    # 'Review name': 'Review_Name',
    'Review rating': 'Review_Rating',
    'Review title': 'Review_Title',
    'Review Content': 'Review_Content',
    'Seeding or not': 'Seeding_Flag',
    'People_find_helpful': 'People_Find_Helpful',
    'URL': 'URL',
    'scraping_date': 'Scraping_Date',
    'Review promotion': 'Promotion_Flag',
    'Review aggregation': 'Aggregation_Flag'
})

bestbuy_final_version.drop(columns = ['Review Recommendation',  'People_find_unhelpful'],inplace = True)
# bestbuy_final_version.to_csv('Bestbuy_NPI_review.csv',index = False)


# %%
Final_review = pd.concat([final_review, bestbuy_final_version], ignore_index = True)
# Final_review = pd.concat([review_template, bestbuy_final_version], ignore_index = True)
Final_review

# %%
Final_review['Review_Date'] = pd.to_datetime(Final_review['Review_Date']).dt.date
Final_review['Review_Rating'] = Final_review['Review_Rating'].astype('int64')
Final_review['People_Find_Helpful'] = Final_review['People_Find_Helpful'].fillna(0).astype('int64')
Final_review['Scraping_Date'] =  pd.to_datetime(Final_review['Scraping_Date']).dt.date
Final_review.info()

# %% [markdown]
# # Staple

def max_pages(sku, url):
    base_url = 'https://www.staples.com/sdc/ptd/api/reviewProxy/getReviews'

    # Create payload as a dictionary
    payload = {
        'tenantType': 'StaplesDotCom',
        'sku': sku,
        'offset': 0,
        'limit': 20,
        'includeRelated': 'false',
        'filterByRating': 0,
        'relatedOnly': 'false',
        'includeRatingOnlyReviews': 'true',
        'filterByPhotos': 'false',
        'sortBy': 'date',
        'sortOrder': 'desc'
    }

    # Convert the payload dictionary to a query string
    query_string = urllib.parse.urlencode(payload)

    # Construct the full URL with the query string
    full_url = f"{base_url}?{query_string}"

    # Set headers for the GET request
    headers = {
        'Referer': url
        # Add any other necessary headers here
    }

    # Make the GET request
    response = requests.get(full_url, headers=headers, verify=False)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON content
        data = response.json()
        review_count = data['reviewList']['total']
        max_pages = math.ceil(review_count / 20)
        return max_pages
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None

def staple_review(url, sku, max_pages):
    base_url = 'https://www.staples.com/sdc/ptd/api/reviewProxy/getReviews'

    headers = {
        'Referer': url
        # Add any other necessary headers here
    }

    all_data = []  # List to store data from all pages

    for page in range(1, max_pages + 1):
        payload = {
            'tenantType': 'StaplesDotCom',
            'sku': sku,
            'offset': (page - 1) * 20,
            'limit': 20,
            'includeRelated': 'false',
            'filterByRating': 0,
            'relatedOnly': 'false',
            'includeRatingOnlyReviews': 'true',
            'filterByPhotos': 'false',
            'sortBy': 'date',
            'sortOrder': 'desc'
        }

        # Convert the payload dictionary to a query string
        query_string = urllib.parse.urlencode(payload)

        # Construct the full URL with the query string
        full_url = f"{base_url}?{query_string}"

        # Make the GET request with SSL verification disabled
        response = requests.get(full_url, headers=headers, verify=False)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON content
            data = response.json()
            reviews = data['reviewList']['reviews']
            df = pd.DataFrame(reviews)
            all_data.append(df)
            # print('Reviews count on page', page, ':', len(df))
        else:
            print(f"Request failed with status code: {response.status_code} on page {page}")

    # print('Total Reviews count:', sum(len(df) for df in all_data))

    # Concatenate all dataframes into a single dataframe
    result_df = pd.concat(all_data, ignore_index=True)

    return result_df

# %%
def extract_sku_from_url(url):
    # Use regular expression to extract numeric value from the end of the URL
    match = re.search(r'/(\d+)$', url)
    if match:
        return match.group(1)
    return None

# ### Novellie
# urls = ['https://www.staples.com/ptd/review/24514342',
# 'https://www.staples.com/ptd/review/24503811']

urls = ['https://www.staples.com/ptd/review/24583383',
'https://www.staples.com/ptd/review/24583386',
'https://www.staples.com/ptd/review/24583387',
       'https://www.staples.com/ptd/review/24583384'
       'https://www.staples.com/ptd/review/24583385']

### 3M 
      # 'https://www.staples.com/ptd/review/24532269',
      #   'https://www.staples.com/ptd/review/24455371',
      #  'https://www.staples.com/ptd/review/24455373',
      #  'https://www.staples.com/ptd/review/24455374' ]

# %%
staples_df_hp = pd.DataFrame()

for url in urls:
    sku = extract_sku_from_url(url)
    print('Get reviews from', url)
    print('Total pages',max_pages(sku,url))
    page = max_pages(sku,url)
    data = staple_review(url, sku, page)
    print('Total Reviews scraped:', len(data))
    if data is not None:
        staples_df_hp = pd.concat([staples_df_hp, data], axis=0)
        

# %%
def extract_source_name(user_dict):
    if isinstance(user_dict, dict):
        return user_dict.get('sourceName', '')
    else:
        return ''
if 'syndication' in staples_df_hp.columns:    
    staples_df_hp['syndication'] = staples_df_hp['syndication'].apply(extract_source_name)
staple_final = staples_df_hp[staples_df_hp['published'] == True]
staple_final['Model'] = staple_final['catalogItems'].apply(lambda x: x[0]['title'] if x else None)
if 'syndication' in staple_final.columns:
    staple_final = staple_final[['id', 'dateCreated', 'title', 'text', 'rating',  'user', 'syndication', 'incentivized', 'Model']]
else:
    # If the condition is not met, exclude the 'syndication' column
    staple_final = staple_final[['id', 'dateCreated', 'title', 'text', 'rating', 'user', 'incentivized', 'Model']]

def tidy_up_user(user):
    if isinstance(user, dict) and 'nickName' in user:
        return user['nickName']
    else:
        return 'blank'
    

    
staple_final['user'] = staple_final['user'].apply(tidy_up_user)

from datetime import date  
pd.set_option('display.max_columns', None)
staple_final['Retailer']="Staples"
staple_final['scraping_date'] =pd.to_datetime(date.today())
staple_final['HP Model Number'] = staple_final['Model'].str.extract(r'(\d+e?)')

# staple_final['Review date'] = pd.to_datetime(staple['Review date'])

staple_hp_combine = pd.merge(staple_final, df_amazon, on = "HP Model Number", how = "left" )
staple_hp_combine['Review Model'] = staple_hp_combine['HP Model'] 
columns_to_drop = [  
    'Model', 'HP Model Number', 'Comp Model number','HP Model','id']  
  
staple_hp_combine.drop(columns_to_drop, axis = 1,inplace = True) 


staple_hp_combine['Country'] = 'US'

column_mapping = {
    'dateCreated': 'Review_Date',
    'text': 'Review_Content',
    # 'URL': 'URL',
    'title': 'Review_Title',
    # 'Response name': 'Response_Name',
    # 'Response text': 'Response_Text',
    # 'Response date': 'Response_Date',
    'incentivized': 'Seeding_Flag',
    'user': 'Review_Name',
    # 'People_find_helpful': 'People_Find_Helpful',
    'syndication': 'Syndicated_Source',
    'rating': 'Review_Rating',
    'Retailer': 'Retailer',
    'scraping_date': 'Scraping_Date',
    # 'Segment': 'Segment',
    'Comp Model': 'Comp_Model',
    'HP Class': 'HP_Class',
    'Review Model': 'Review_Model'
    # 'reviewedDate': 'Review_Date'
    # 'Competitor_Flag': 'Competitor_Flag'
}

# Rename columns
staple_final = staple_hp_combine.rename(columns=column_mapping)

staple_final['Competitor_Flag'] = staple_final['Review_Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
# staple_final.drop('id', axis = 1) 
staple_final.drop_duplicates(inplace = True)
staple_final['Review_Date'] = pd.to_datetime(staple_final['Review_Date']).dt.date

# %%
Final_review_all = pd.concat([Final_review, staple_final], ignore_index=True)
pattern = r'(\w+ \d{1,2}, \d{4})'
Final_review_all['Response_Date'] = Final_review_all['Response_Date'].fillna('').str.extract(pattern)
Final_review_all['Response_Date'] = pd.to_datetime(Final_review_all['Response_Date'],errors = 'coerce').dt.date.fillna('')
Final_review_all['Review_Date'] = pd.to_datetime(Final_review_all['Review_Date']).dt.date
Final_review_all['Review_Rating'] = Final_review_all['Review_Rating'].astype('int64')
Final_review_all['People_Find_Helpful'] = Final_review_all['People_Find_Helpful'].fillna(0).astype('int64')
Final_review_all['Scraping_Date'] =  pd.to_datetime(Final_review_all['Scraping_Date']).dt.date
Final_review_all.sort_values(by = ['Review_Date'], ascending=False)
Final_review_filter = Final_review_all
Final_review_filter ['Review_Date'] = pd.to_datetime(Final_review_filter['Review_Date']).dt.date
Final_review_filter ['Review_Rating'] = Final_review_filter['Review_Rating'].astype(int)
Final_review_filter['Review_Rating_Label'] = Final_review_filter['Review_Rating'].apply(lambda x: '1-2-3-star' if x <4 else '4-5-star') 
Final_review_filter

# %%
previous = pd.read_csv(r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\MMK\MMK_web_review_raw data.csv")
previous['Review_Date'] = pd.to_datetime(previous['Review_Date']).dt.date
previous['People_Find_Helpful'] = previous['People_Find_Helpful'].fillna(0).astype('int64')
previous['Scraping_Date'] =  pd.to_datetime(previous['Scraping_Date']).dt.date
previous ['Review_Rating'] = previous['Review_Rating'].astype(int)

def extract_first_ten_words(row):
    words = row.split()
    return ''.join(words[:10])

# Apply the function to create a new column
# previous['FirstTenWords'] = previous['Review_Content'].fillna("").apply(extract_first_ten_words)

def clean_text(text):
    text = str(text)

    # Remove non-English characters and punctuations
    cleaned_text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    # Remove extra whitespaces and convert to lowercase
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip().lower()
    english_words = re.findall(r'\b[a-z]+\b', cleaned_text)
    first_ten_words = ''.join(english_words[:10])
    return first_ten_words

previous['FirstTenWords'] = previous['Review_Content'].fillna(0).apply(clean_text)

previous['FirstTenWords'] = previous['Review_Content'].fillna(0).apply(clean_text)

Final_review_filter['FirstTenWords'] = Final_review_filter['Review_Content'].fillna(0).apply(clean_text)
df_concat = pd.concat([previous, Final_review_filter],ignore_index=True)
df_concat.sort_values(by = ['Review_Date','Review_Model','Retailer','FirstTenWords','Scraping_Date'],inplace = True)
df_concat_final = df_concat.drop_duplicates(subset=['Review_Model', 'Retailer',  'Review_Date','Review_Rating', 'FirstTenWords'], keep='first')
df_concat_final.sort_values(by = ['Scraping_Date','Review_Date'], ascending=False)
df_concat_final['Scraping_Date'] = pd.to_datetime(df_concat_final['Scraping_Date']).dt.date
# df_concat_final.loc[:,'Scraping_Date'] = pd.to_datetime(df_concat_final['Scraping_Date']).dt.date
df_concat_final.drop(columns = 'FirstTenWords',inplace = True)
df_concat_final.head()

# %%
df_concat_final.to_csv(r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\MMK\MMK_web_review_raw data.csv", index = False)

print('MMK_raw_data_scraping completed. MMK_raw file saved')
