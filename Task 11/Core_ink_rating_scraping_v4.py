#!/usr/bin/env python
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date 
import regex as re 
from requests_html import HTMLSession
import time
import json
from urllib.parse import urlparse
import traceback

# In[2]:


print('Running Core_ink_rating_scraping_wout_SMSS.py')

excel_file_path = r"C:\Users\tayu430\anaconda3_remote\envs\webscrapper\My Scripts\Star rating scrape URL and info - NPI.xlsx"
sheet_name = "data_new"


# Read the Excel sheet into a DataFrame
df_amazon = pd.read_excel(excel_file_path, sheet_name=sheet_name)
df_amazon['HP Model Number'] = df_amazon['HP Model Number'].astype(str)
df_amazon['Comp Model number'] = df_amazon['Comp Model number'].fillna(0).round(0).astype(int).astype(str)
df_amazon


# In[3]:


final_review = pd.DataFrame()
final_review 


# In[4]:


# HP rating 
def get_review(soup):
    extracted_reviews = [] 

    try:
        # First, attempt to extract the title using the new structure
        title_element = soup.find("div", attrs={"class": "a-row product-title"})
        
        if title_element:
            # Find the 'a' tag inside the title element
            review["title"] = title_element.find("a", attrs={"class": "a-link-normal"}).text.strip()
        else:
            # Fallback to using the page's title element
            review["title"] = soup.title.text.replace("Amazon.com: Customer reviews: ","")
        
        # Final fallback in case none of the above methods work
        if not review["title"]:
            review["title"] = "Title not found"  # Default if no title is found
    except AttributeError as e:
        print(f"Error in extracting title: {e}")
        traceback.print_exc()  # This will print the detailed traceback


    try:
        review['rating'] = soup.find("i", attrs={'class':'a-icon a-icon-star a-star-4-5 cm-cr-review-stars-spacing-big'}).string.strip()
    except AttributeError:
        try:
            review['rating'] = soup.find("span", attrs={'class':'a-icon-alt'}).string.strip()
        except AttributeError:
            try:
                review['rating'] = soup.find("span", attrs={'class':'a-size-base a-color-base'}).string.strip()
            except:
                review['rating'] = None

    try:
        review['review_count'] = soup.find("span", attrs={'class':'a-size-base a-color-secondary'}).string.strip()
        if not review['review_count']:
            soup.find("span", attrs={'class':'a-size-base'}).string.strip()  
    except AttributeError:
        try:
            review['review_count'] = soup.find("span", attrs={'id':'acrCustomerReviewText'}).string.strip()
        except:
            review['review_count'] = None
            
    overview = soup.find_all("div",{"class":"a-text-left a-fixed-left-grid-col reviewNumericalSummary celwidget a-col-left"})
    
    for item in overview:
        try:
            each_rating = item.find("div", {"class": "a-section histogram"}).text.strip().replace('\n', '').replace(' ', '')
            ratings = {}

            pattern = r'(\d+star)(\d+%)'
            matches = re.findall(pattern, each_rating)

            for match in matches:
                rating = match[0]
                percentage = match[1]
                ratings[rating] = percentage

            review["each_rating"] = ratings
        
        except AttributeError:
            review["each_rating"] = None
    

    extracted_reviews.append(review)
#     print(extracted_reviews)

    return extracted_reviews


# In[5]:

# docker
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
    elif(host == 'www.amazon.com'):
        cookies = {
    "at-main": "Atza|IwEBIFdXPow1fvkVbojTvlVIM-dNVcQdDQRStQmhxmc8k7Mswb8j3lwfVJyaJhyoywsh2QKPF-na1mSxsiByyNOV7e9U2B2fLZUjSGX3aC6R1sz60Xi7sVCb4wzXU3W7Ij2lstjLNqXkrC8nYsEld8Z3OixskIEeib9DgkGpLz5lpu0tOyFEeqyMSKCX2Yck4zgwncZuJVOx_VBDVyJg2cpibduzE2WoqTqVIl1YdSKmRSxxbA",
    "sess-at-main": "\"SIThg1afNNueixwLjk5RB4nfumtcFznr/u1cX4Gb/4Y=\"",
    "sst-main": "Sst1|PQGa7kED_JEwvuvou0BKGrhECXMN-2QVBygFjKgitNqEGlR7CvIauX1KERwhI6v9ghJh5xyV6lQyUzfOR4-MpSWUHQFV3rq_lzlqkGYTxImIxv_nXMTSrPIB3U1iZhgbvnHfykketDs952kPJWzxmTM9NAPn07YDdGo269MbpG85Z9kZoKjIrQzIVL5b1lydTvplm4aJW0D_UnLlhvYJ2o-n9BuqTY0BADKPQISu9eKiRW6iEZIXdOsV_Dza6IfYxis0xEjC5KN1qPAh8EcuevTFF5OQoZaSUaDXUZOrDMyYYzE",
    "x-main": "\"TahtVjrG5HWcakcCUpzTS?s84eLpFFQ7Z4QkdTxeKM4EHauz5WuFy12jVu@kdgNB\"",
    "i18n-prefs": "USD",
    "lc-main": "en_US",
    "session-token": "\"QMkqZ9kfg769xhCBxnZZg8C4FiIILkfbu0GLsruXlkseyQNKQ3wJbaM6opShr5kM7nheuF3Ea59pn5f6+M3bR/2t269afVoExfLJb0iUzAf24XuwJR/3Xwi2FNsil9h273FuTYQD2LEPQcMgCw3MgjQkYaD9sPfMB0RmpcfrFXMAwyLhA2TWy9uI3iBloNmKeq4XHouiL1ioyr+obo1DwI/oEGIDIgrr/Hyf/+KwpmDD0XlJEaysNDR5uL6Ko8KWfidEnSGYIuVE501JwDfLCXa+Jhcynj+S+xd0bzbAbdGgrDrnIs4roUSCdSFyMeuxlasH0E2K8bThbwuOb6uh+CwvdhuDeXCntEtHLSrbE4DGOCoy3PSARA==\"",
    "ubid-main": "135-1181866-0955538",
    "session-id": "131-6046728-9902119",
    "session-id-time": "2082787201l"
}
    elif(host == 'www.amazon.in'):
        cookies = {
    "session-id": "261-8598938-5492309",
    "ubid-acbin": "260-8621620-3278732",
    "session-token": "\"jEg9m1LYhuFeywm4hIwWCjMaIBUU5D84xvtUcPgneyeBhY3e3g/822ow4+UNWPNVTIttXqTCi4Mz1nAOB49KQ8dUAW0kGo5jgOHCXNzIZ090c9F0eAeHpK6/pK5/kk/f7FU0dGQuatgSyOD6b7WO2zfyItJ8bGr4jQOAMFXV2mNN4l/YjZJcRrvHvS1EBuuypSyKoXib2nJC9hFvB6KUBeMtlq9MsIn0LTjXLqpOrIVRkmZKTsGJ/MrMDRG6oESJaJkWySokOE9TjGGxTxCpVeoKZf8h6P18nMOWkcN9HiykYKUciKi8Jp0wzF4CjM0LfljVRhLed1ayIIBALPF4/agM5+IFsQn+FQLuQU8auzSEJXsNq/l3og==\"",
    "x-acbin": "\"5XAkDzZ@M3oovJeU?PiDE9zCmlipYmHA2r?DPUW3CvQL7LKSZotysRVaJu0X6F0r\"",
    "at-acbin": "Atza|IwEBIAmWQV9UiWH3kUErbj5tzuy61BTpUWd9jZwntJ6hco-ML9YZp8qNsHvb8n20bkE1GJO0_ZCGxITk70-4rLwKFOb3ObAPqyQl8vPmMPE5qWvVXY8BXElZjkYiHznOe07cLVmGGn_yf72OVXwybUqttPFgmxzduHFIDhNCxfGR0Co9xv-2F0EVURqrezqEgpM7IM9_G_ieV2XWzDQSQjcte1RXrGQuWXCDLsAeo8le-W0USA",
    "sess-at-acbin": "\"TZaZ6o3WhetwKKMRi7ZgbaIxAQE8PxCN4q4txnOhFGI=\"",
    "sst-acbin": "Sst1|PQHJ7U1GdM9Ur75gha7gesaaCV4WZZ1ChWRtEqRBR2vqMEkiNgQ79fzXbvGN13gL8x1tlQvuEWhT0mQS-Blc27z4nn6PIbyCmYiL4BIbne5GXmVBtqFNxdhtRx4lM-QO1UP91vCuT6eUVlMrXmlUcaM0o24UO4ZBAHkJJauRpFgayQkdJxfcC5MRgoE_Xfo13s-zhHBvE0lkNFafa2ObTRwppPm14zgQ_DZE7bDKdWWgCoMj7b7QLcj3nnghlpzDLFe56QQbs1riLwIPndrb0w3ooVWvGWYH3xSrdc_dODCv6H8",
    "session-id-time": "2082787201l",
    "i18n-prefs": "INR",
    "lc-acbin": "en_IN",
    "csm-hit": "tb:63XZ9YZE899GAER0767P+s-C4V23JG4Z5AV8D802MS5|1730885455102&t:1730885455102&adb:adblk_no"
}


    req = requests.get(url, headers=headers, cookies=cookies)
    soup = BeautifulSoup(req.content, "html.parser")
    return soup

# In[6]:


# ## US

# %%
sheets = "Amazon"
amazon_url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_list = amazon_url['HP URL'].to_list()

link_list = []
for value in all_list:
    if value is not None and value not in link_list:
        link_list.append(value)
print(len(link_list))

# %%
# need to run about 10-15 minutes
start_time = time.time() 

review_list = []
number = ['five', 'four', 'three', 'two', 'one']

for link in link_list:
    review = {} 
    while True:
        try: 

            soup = get_soup_amazon(link)
            print(link)
            review_data = get_review(soup)
            review.update(review_data[0]) 
            review['HP url 1'] = link

            time.sleep(3)

            for star in number:  

                target_url_star = f'{link}/ref=cm_cr_arp_d_viewopt_sr?formatType=current_format&filterByStar={star}_star'
                soup_star = get_soup_amazon(target_url_star)
                review[star] = soup_star.find("div", {"data-hook": "cr-filter-info-review-rating-count"}).text.strip()  
            print(review)

            review_list.append(review)

            time.sleep(3)
            break
        
        except Exception as e:  
            traceback.print_exc() 
            print(f"Error encountered {e}. Retrying in 5 seconds...")  
            time.sleep(3)  # Pause for 5 seconds before retrying 

end_time = time.time()  
  
# Calculate the elapsed time  
elapsed_time = end_time - start_time  
  
# print(f"The code chunk has been executed in {elapsed_time} seconds")

# %%
pd.set_option('display.max_columns', None)
from datetime import date 

amazon= pd.DataFrame(review_list)

amazon['scraping_date'] = date.today().strftime('%Y-%m-%d')
amazon['Retailer']="Amazon"
amazon['HPModel'] = amazon['title'].str.split('Wireless', n=1, expand=True)[0].str.strip()  
amazon['HP Rating'] = amazon['rating'].str.replace(' out of 5 stars', '').astype(float)
amazon['HP Rating Count'] = amazon['review_count'].str.replace(' global ratings', '')
amazon['HP Rating Count'] = amazon['HP Rating Count'].str.replace(',', '')
amazon['each_rating'] = amazon['each_rating'].apply(lambda x: [(k, v) for k, v in x.items()])  

  
amazon = amazon.explode('each_rating')  
amazon[['Ratings', 'Amazon HP Ratings breakdown']] = pd.DataFrame(amazon['each_rating'].tolist(), index=amazon.index)  

column_mapping = {  
    'one': '1star',  
    'two': '2star',  
    'three': '3star',  
    'four': '4star',  
    'five': '5star'  
}  
  
amazon1 = amazon.rename(columns=column_mapping)  
amazon1 = amazon1.melt(id_vars=['HPModel'],  
                    value_vars=['1star','2star','3star','4star','5star'],  
                    var_name='Ratings',  
                    value_name = 'HP Ratings breakdown') 
amazon_review = pd.merge(amazon, amazon1, on = ['HPModel','Ratings'], how = 'left')

amazon_review[['HP Rating breakdown', 'HP Reviews breakdown']] = amazon_review['HP Ratings breakdown'].str.extract(r'(\d+,?\d*) total? ratings?, (\d+,?\d*) with? reviews?')

# amazon = amazon.sort_values(['HP Model'])
selected_columns = [  
    'HPModel',  
    'HP Rating',  
    'HP Rating Count',  
    'Retailer',  
    'Ratings',
    'Amazon HP Ratings breakdown',  
    'HP Rating breakdown', 'HP Reviews breakdown',  
    'HP url 1',
    'scraping_date'  
]  
  
amazon_review = amazon_review[selected_columns]  

amazon_review = amazon_review.drop_duplicates()
amazon_review['Country'] = 'US'


# In[ ]:


# %% [markdown]
# ### Comp rating

# %%
sheets = "Amazon"
amazon_url =  pd.read_excel(excel_file_path, sheet_name = sheets)
all_list = amazon_url['Competitor URL'].to_list()
link_list = []
for value in all_list:
    if value is not None and value not in link_list:
        link_list.append(value)
print(len(link_list))

# %%
start_time = time.time() 
review_list_comp = []
number = ['five', 'four', 'three', 'two', 'one']

for link in link_list:
    review = {}  
    while True:
        try: 

            soup = get_soup_amazon(link)
            print(link)
            review_data = get_review(soup)
            review.update(review_data[0]) 
            review['Comp url 1'] = link


            for star in number: 
                target_url_star = f'{link}/ref=cm_cr_unknown?filterByStar={star}_star&pageNumber=1'
                soup_star = get_soup_amazon(target_url_star)

                review[star] = soup_star.find("div", {"data-hook": "cr-filter-info-review-rating-count"}).text.strip()  
            
            print(review)

            review_list_comp.append(review)

            break
        
        except Exception:  
            print(f"Error encountered. Retrying in 5 seconds...")  
            time.sleep(5)  

end_time = time.time()  

elapsed_time = end_time - start_time  
  
# print(f"The code chunk has been executed in {elapsed_time} seconds")

# %%
amazon_comp= pd.DataFrame(review_list_comp)
amazon_comp['Competitor Brand'] = amazon_comp['title'].str.extract(r'^(.*?) ')
amazon_comp['Competitor Model'] = amazon_comp['title'].str.extract(r' (.*?\d+a*)')
amazon_comp['Comp Rating'] = amazon_comp['rating'].str.replace(' out of 5 stars', '').astype(float)
amazon_comp['Comp Rating Count'] = amazon_comp['review_count'].str.replace(' global ratings', '')
amazon_comp['Comp Rating Count'] = amazon_comp['Comp Rating Count'].str.replace(',', '').astype(int)
amazon_comp['each_rating'] = amazon_comp['each_rating'].apply(lambda x: [(k, v) for k, v in x.items()])  
amazon_comp  
amazon_comp = amazon_comp.explode('each_rating')  
amazon_comp[['Ratings', 'Amazon Comp Ratings breakdown']] = pd.DataFrame(amazon_comp['each_rating'].tolist(), index=amazon_comp.index)  

column_mapping = {  
    'one': '1star',  
    'two': '2star',  
    'three': '3star',  
    'four': '4star',  
    'five': '5star'  
}  
  
amazon1 = amazon_comp.rename(columns=column_mapping)  
amazon1 = amazon1.melt(id_vars=['Competitor Model'],  
                    value_vars=['1star','2star','3star','4star','5star'],  
                    var_name='Ratings',  
                    value_name = 'Comp Ratings breakdown') 
amazon_review2 = pd.merge(amazon_comp, amazon1, on = ['Competitor Model','Ratings'], how = 'left')

amazon_review2[['Comp Rating breakdown', 'Comp Reviews breakdown']] = amazon_review2['Comp Ratings breakdown'].str.extract(r'(\d+,?\d*) total? ratings?, (\d+,?\d*) with? reviews?')

# amazon = amazon.sort_values(['HP Model'])
selected_columns = [ 
    'Competitor Brand',
    'Competitor Model',  
    'Comp Rating',  
    'Comp Rating Count',  
    'Ratings',
    'Amazon Comp Ratings breakdown',  
    'Comp Rating breakdown', 'Comp Reviews breakdown',  
    'Comp url 1'
]  
  
amazon_review2 = amazon_review2[selected_columns]  

amazon_review2 = amazon_review2.drop_duplicates()
amazon_review2['Country'] = 'US'




# %%
amazon_review['HP Model Number'] = amazon_review['HPModel'].str.extract(r'(\d+e?b?\d*)')

amazon_review['HP Model Number'] 
amazon_review2['Comp Model number'] = amazon_review2['Competitor Model'].str.extract(r'(\d+)')
amazon_final1 = pd.merge(amazon_review, df_amazon, on = 'HP Model Number', how = 'left')
amazon_final2 =  pd.merge(amazon_final1, amazon_review2, on = ['Comp Model number','Ratings','Country'], how = 'left')


selected_column_final = [
    'HP Class',
    'HP Model',  
    'Retailer',
    'Comp Model',
    'HP Rating',  
    'HP Rating Count',  
    'Comp Rating', 'Comp Rating Count',
    'Ratings', 
    'HP Rating breakdown',
    'Comp Rating breakdown',
    'HP url 1',
    'Comp url 1',
    'scraping_date',
    'Amazon HP Ratings breakdown',
    'Amazon Comp Ratings breakdown',
    'HP Reviews breakdown',
    'Comp Reviews breakdown',
    'Country'
]  

amazon_final = amazon_final2[selected_column_final] 
amazon_final


# In[ ]:


final_review = pd.concat([final_review,amazon_final])

# %%
final_review 

# file_path = r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\amazon_star_ratings.csv"
# final_review.to_csv(file_path, index = False)


# In[ ]:


# # Staples

# %%
import random

User_Agent = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    , 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0',
    'Mozilla/5.0 (Linux; Android 11; SAMSUNG SM-G973U) AppleWebKit/537.36 (KHTML, like Gecko)',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; U; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.5399.183 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0'
]

cookie = [
    'ACID=e743918f-9c01-4185-9889-01b383f39a46; hasACID=true; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwMDI5Mjg0MjMwNCwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzAwMjkyODQyMzA0LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MDAyOTI4NDIzMDMsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJzZWxlY3Rpb25Tb3VyY2UiOm51bGx9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNzAwMjkyODQyMzA0LCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6ZTc0MzkxOGYtOWMwMS00MTg1LTk4ODktMDFiMzgzZjM5YTQ2In0%3D; vtc=TPvkWCN79GksgVjpd8lQp4; btc=TPvkWCN79GksgVjpd8lQp4; bsc=XjMXQ-W4dVqYGkP1JSNWEs; _pxvid=d8404a5f-85e4-11ee-a839-f5e2c99825fc; pxcts=dd47a175-85e4-11ee-b2aa-79f32486f419; _tap_path=/rum.gif; _tap-criteo=1700292852331:1700292852725:1; _tap-Ted=1700292852724:1700292852725:1; _tap-lrV=1700292862974:1700292862975:1; _tap-lrB=1700292878531:1700292878532:1; _tap-appnexus=1700292879419:1700292879718:1; _gcl_au=1.1.258469864.1700294686; bstc=XgTuQhQdHEhnMqLxEbRYgw; mobileweb=0; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=; ak_bmsc=928D3BE7B227834B8EF4992840D6AFA1~000000000000000000000000000000~YAAQT0Dfb2Hq79qLAQAAWF0E7xW+cNY2y9+fVR2/0QrvmtyiqK/i/uEwVaXReQYp0Z0y6FV2m0EQCZomlg6PikSMdPE5Yx+Al074WilmM5mGxgz2kYLAXQy0aPKfmDgo9ohHooJNwq7VcMShtmCmJ1ARCqwfhpgMsbNFrR7LgHL3FRZJhrg9alhKuKLZRfToimKXZ0sKxSm5rLcO6L57KGoWlZX3NmA0ddatFrENafTH0mQVlKWpVPKHtgOZZh4EsGHOO90Z1fy0P2hIJNmpHpoDLk1w+0tBu9Upo+8L5KPS0K1LcuVDuaXEx4+n6o2fMg+HrkC1AF5T7cKLiLDZlbL/C5kSesKN3Rp2RaJjt8YzY2QTkaxrmX9TYd6omcLPWSqHLvjJK0/gyfE=; xptc=assortmentStoreId%2B3081; xpm=3%2B1700522123%2BTPvkWCN79GksgVjpd8lQp4~%2B0; b30msc=XgTuQhQdHEhnMqLxEbRYgw; _tap-li=1700522132702:0:2; _uetsid=0cce940087ff11eea88661d0630688d7; _uetvid=237553a085e911ee9eaec99bfa867851; auth=MTAyOTYyMDE4TzUL0tBWqaWlPLiIWSvGIIqP44I9XeKX8N5bcOOhCkNGFqMJTLhMlajVoxZh6%2F64P%2Bx3aAqHkDo7tLTswFgLqhE9gq7sWtCKMSvMMtAlsDnj17kGZ7Mu4K7gp6blyLzB767wuZloTfhm7Wk2KcjygsAEeU%2BeKCMhfP9XV060SY%2Fspww18DSfg4loIXetO33HWWxCKdp%2B8UHdguRD9DC%2FlTyW2FeTzNUdxbN2aHvb8W0UMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHWs0k33oHkBmchRaU9fj5kF7pZ6JaDMzmiWlGlRQ4nUMw5XFK3QDKKcB%2BPe5gKPHMTJomOWP4NHaOZmjOE06S78R4yUE7XpP0usJVgwSa5Hg5dejwrW41QOfpHzdmIzkekjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9DVVJCU0lERSIsIlBJQ0tVUF9JTlNUT1JFIl0sInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQifV0sInNoaXBwaW5nQWRkcmVzcyI6eyJsYXRpdHVkZSI6MzguNDc0NSwibG9uZ2l0dWRlIjotMTIxLjM0MzgsInBvc3RhbENvZGUiOiI5NTgyOSIsImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSIsImNvdW50cnlDb2RlIjoiVVNBIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJ0aW1lWm9uZSI6IkFtZXJpY2EvTG9zX0FuZ2VsZXMifSwiYXNzb3J0bWVudCI6eyJub2RlSWQiOiIzMDgxIiwiZGlzcGxheU5hbWUiOiJTYWNyYW1lbnRvIFN1cGVyY2VudGVyIiwiaW50ZW50IjoiUElDS1VQIn0sImluc3RvcmUiOmZhbHNlLCJkZWxpdmVyeSI6eyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJhY2Nlc3NQb2ludHMiOlt7ImFjY2Vzc1R5cGUiOiJERUxJVkVSWV9BRERSRVNTIn1dLCJodWJOb2RlSWQiOiIzMDgxIiwiaXNFeHByZXNzRGVsaXZlcnlPbmx5IjpmYWxzZSwic3VwcG9ydGVkQWNjZXNzVHlwZXMiOlsiREVMSVZFUllfQUREUkVTUyJdLCJzZWxlY3Rpb25UeXBlIjoiREVGQVVMVEVEIn0sInJlZnJlc2hBdCI6MTcwMDU0NTYxMjY4MiwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOmU3NDM5MThmLTljMDEtNDE4NS05ODg5LTAxYjM4M2YzOWE0NiJ9; _tap-googdsp=1700524013260:1700524013261:1; bm_mi=6D08B7F70C2629CB7335921BD0A476C4~YAAQVkDfbybwhNCLAQAAiz8h7xVv+k/UWG2WIDe/gPQ8AOZTnjAhiIOZ0bLj8CiGeNePuQWuPNwhDXS1oNp15tCC1PJSEhUXHiX+NSzkvUL7leDWiBNTKpbglVNmWH1NuG/LvNHQA2LBVlOMNtN+pa4w85WJ5WffbcN5bm4oXQMtRoZmDiaP9LejTQc7RtRDzb/UBiKxSj7B7tWFZWvoaMl7Byh7Zw98qmlF62SYLz85rGSvABr10DN471JChbH4Q/G82+KTkYcWus+IMdzcowTy7Z+Q8iX5zSmhRA2ELuD5Ie109BCzItjTb03IbEwxaZaoK0uGP1lkf74XjjcvevsgHZqUduo=~1; _tap-wmt-dw=1700522135009:0:2; _px3=5ea3ebae35f07f1831578749c875dce471cd3c238e9b1d48cc4d8ac7ed6ef5be:/ONT+vAVQYHGOAb5jG51c6bmUcQjbaW1zryLfFf3ZEFrqWRTeciC0Y4hk/ovL1XeinngRvREckraA7RQaqwaeA==:1000:tUHSmj1FtO2NPUlmIukbTE9SnWpD2T21PLjZBMihCAM89elMZOLre7HSRlFqK71v0f8yL9k4n8Sg8C5OWCQ9mrtXRYNZ7J2cHZIgW/Xriz9FwISvvFB2qUm7ermjWRrawUo7rSOqC6UHNjyG4cNmWupEtwDC+dn8hy+VsaMiY64jQxqICji2ZH+yxxSM8rRGtusit4qn5KaOrVpNR2530EK/u3KBOrrKvimdzwbEfh4=; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1700524027817; xptwj=qq:856f56eac45d86299598:ZRnl9M8BUfAYvTYtX+QCNLhEzqw6K+xHmpEjeoxuN7UABA+EM4HcK1WV764ZOO5/6syK6HJNzQ+7oCA1dX0quNAOWtTaQIDcRQupz54KdhbyjvwwBjDPRRo9S45zB814KqqVnZB4xW5GOyMmpEBETfwct40JrJ9yQKgEdWfokI/A+gc=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1700524028000@firstcreate:1700292842259"; xptwg=3735093285:1780D552F836C50:3B5C8D6:477BA478:6AAF8424:7C4EFE87:; TS01a90220=01419f1d62bb3082af49e4e290c9e4f7b7d4d09f7a4d1f20d21c60c4e1a7da9ee45eaa7b433f2f9eae578eda14efecc83c7473e1aa; bm_sv=FBAB233769AD51E9AA3317FB35DBDC6D~YAAQVkDfb73yhNCLAQAA5XMh7xUG5PTSMRUK4v2TvEXPwemnWtVYlvkgiCKbkuxNXqf+vVZB8rZcQemv+ydTIl18pO1qMpban0Uf+/ci+ZkYfinA4Fd+fcZ1uHAyQxRGF/plo2v0Gq9TxZgp4pp/YFFuxSnGTqGRKl4RkFJbdxQSVM0CBeOhtUQWXZ0y9L4TE3TF7jw/A9Nu++2UrVU5zLqdE22Fo4Pdw6Yn6RnKbC9OZ7gJsF2DhCgy6+IkYxmg/SE=~1; _pxde=8e9d44f04024aa1db6ed77fcc7bbf79de1d603492bbaba0843e90b5cd14038cf:eyJ0aW1lc3RhbXAiOjE3MDA1MjQwMjg5MTR9',
    'ACID=7cff0725-9085-4be0-bf3c-6839f8621f69; hasACID=true; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwMjYzMDM3NDkxNiwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzAyNjMwMzc0OTE2LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MDI2MzAzNzQ5MTUsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJzZWxlY3Rpb25Tb3VyY2UiOm51bGx9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNzAyNjMwMzc0OTE2LCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6N2NmZjA3MjUtOTA4NS00YmUwLWJmM2MtNjgzOWY4NjIxZjY5In0%3D; vtc=c_lPqL_NYDC6UXPeh9MF7U; _pxvid=5644c6ff-9b27-11ee-ac94-b6ae3dc3013d; pxcts=578a7ded-9b27-11ee-b1db-83fbf11618da; thx_guid=f069aa2ed76a293b146f2f45273ffc5e; QuantumMetricUserID=86dbb1aaa4c0846b6472227c8127c115; bm_mi=A9B761149346E34C2336993088CBA838~YAAQpCLHF0tB41iMAQAAT6lrexYSWryWW33ix0ebW0gEwR9R9Q3T0mju/pC7V7LqjuwHJrGseRkgFSZZiZVHV/zjTu4nCYLngf6698jYoiODJd20Ah32C8vCA7fNXyHUhtfSgpdKNkkOpAYXO8oYiiVKF1l5IHZeILy9fj41t1W9tF9DODYuxTNU8QPPnZr4w70z23lcK+3EBKd8IfqBgPT9kjLT3pKPY5P1EjKBk7Wvn9d44E3YtdQOdIlnmZMFqcniDHXEd0TgXXrvQ7NVo1BkEMaGynnbyeApl64EsIYaBB5nCsrN2NFOY0C1ZumHLPsxg5pg~1; ak_bmsc=28DD2AB37291D5EE54D348A14FFB7027~000000000000000000000000000000~YAAQpCLHF1FB41iMAQAA3atrexY3RQO5uSrgAn4o71OTeCY3Kb4oklPOhzO4ob+ZppkPcKbMuYwUEtBs6vEVlp9CoeIcig2wNR+YFPAMLag0FgMkBgPTNS7DAEU7mXMzqG7Zxes7pw+1iCYIJKVm0KAso3OE50SIiVGnpFKhhL7E2/cDwpWbUdrNVY30TGqqMk/uCOYcvkqNQE0wzVpTYkK2hWIeztHPT5fNqkdtRUylfN9FBTzfaUxix+emgkOw4S/zZxgkw4lNSo/YbiRZDpT409NuUxV4ht5j47adbQ1Q8F08pAvMkK3vGzOypF53bdu9Iw3g3D4PUZDOdQ2FFArxdUalCDwieOVrUbY5Xu55+30+fnRIFgzDM+aa72JMrHteO5326b1oz/QcUX1+EgWPmeSXJTk6/0EJvYS6yU9fnzsad5XVkCe4cRcS0AP/ZVfbCDorHjsVEachEwRjpKmMWx8q6DOuGFw00I1vRusHKdW1nrCknd+TjPY2u042+mVSAQCo0JoYGB8VZlNhvUof6KzcVkfB4ZG5; auth=MTAyOTYyMDE45uQqkShnPmrFyDklTStmr7wc0IXA0uhqKhIjidzMeOlor0FoZrStjvQEbiRm5ZqSmQEJ%2FPunCsfySE6LPKFrLFOCiFQAqcgu3n0iVe3t%2BsYE2L20v13oIDsvWtwF1M8S767wuZloTfhm7Wk2Kcjygi5k0VvBM%2FJjwcKWWhCnBS%2FsNVBmy9J1bR2VHO%2FdV8LIpQmp8XOq309QoW%2BZviaSOv8sqUVU54sFd4Bd6dus2ZEUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHVeQqO76rPmdncwFjpe%2BDY%2BOi7voHCqN3McdaIPwoybp22ykAEJaZ9rj2LqLTXsr9v3%2FEocn3Z%2BtLU09JmG1TrtCvNQVjLp%2Ffv%2BF%2BvOqp4HL4oel5ASvElr1Cex8QK9UZEjyrOXbKKhH072NS%2FW0j%2FU%3D; bstc=VDdH8jJ4m0c1jwDTa0PzoE; mobileweb=0; xptc=assortmentStoreId%2B3081; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=1mX_Y|7C2Eg|ERspM|HhvdQ|IS-p_|LPh6f|XbHrX|csP8O|gUDh7|oY0CV|qKfBf|yamTG; exp-ck=IS-p_1LPh6f1XbHrX1gUDh71qKfBf1; xpm=1%2B1702877712%2Bc_lPqL_NYDC6UXPeh9MF7U~%2B0; QuantumMetricSessionID=fb9f81cd396976e7c5f3318d82cf9aca; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1702878308583; xptwj=qq:1076adbafd040b695fce:7QGLRUcehov3qVy4qSYDvW4xgk6Uac2yX7HX1tE4u646HYe83hDIeMje3Thc/1gvh8z8MrbJNvYdlJL4OmUTikr/fII5bRfC3yxM6l2D8uZIkuA2LrEs6TpjeeKaJ8Uu78B/EOBkj1BzXZ7N6125enlMLSzG; xptwg=3146300320:19444130883AE60:3FBA1BE:1196AC61:D2F58FD2:22F02742:; _px3=6c9349297a823e7545b893fcfa8e6bea6eecb57dc1b11b7cf64522744342fc05:q5wbXoTzZgQ7QP9YgqzSywMmiYqoqw/ZrncImDY3CrTPeEkOqVzPo2zJ3AHIc+s/Ltf7IlS8awVgjpNXM98pxQ==:1000:cn/ohrnROd+TGJA88Xeo4/KYIo/UsLvu6vSoOqMnmEug9QezGcx/WE0rfSuHGKZjjjaYrqXq8pBrJ5nlgEeADLsTsoKQiFm1rc/m/6Q+wMMNleiOZAl9VZy3wpYUymfUCrFwGU+qqgFlXPCfQNN9o6vjljQksDltmcFQIof95UjvNs7UMihymHVA7zlP5BOXCURkIHiiJbM73dOLjmbFMFnxGHKXULDB8u2cb1oECR0=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1702878315000@firstcreate:1702630374882"; TS01a90220=014e9abc5b76ea37289d24aa7bf6872327f3a801259b614de94e2e0e90c1b0f94c88be0edeb3c58cf3cba5b3568b2b79f66cc4dc9c; bm_sv=ADF605AFD1AA1E340A3C57CE2E012D0E~YAAQJPN0aPSze2mMAQAAbQJ1exaqQAmPQEugdiG3MQAUrup+aS98eOHJhIKWYEY4cQ16kvxr5PjD68+TG/q08uWKgkntuhl4BsnOGqcEXY7GjWBoB0neW+58ysLmfhee1rCKsVjS3iaBCoFZwcqIgkO3eDhiRFyKZXfNTkKDX7JBTyy63S1DAg2SrohZrD0KlpE5E2rFPlHxbpWohjTWhvXGlDIHA0lk8uQB+NvqN5R8yDL6bMxSm0YrJ6/p6h5kxvQ=~1; _pxde=3cb689d88ebd75dfd5e5c64a8389623aecf6bac16e69f216cf01a4e825c18d52:eyJ0aW1lc3RhbXAiOjE3MDI4NzgzMTUzNDl9',
    'ACID=6ff15283-acba-4fe2-89c5-ac59a9b887d1; hasACID=true; thx_guid=0c376c1283b8b14be6de36c7c9897b80; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1696941267921; _m=9; vtc=WYxqCClwKT085jYR_DbfQI; _pxvid=5a44b304-6769-11ee-a5a3-89fdf7f6e6ba; auth=MTAyOTYyMDE4IIJ07VmsLMduZmu3wWCW8eIqM67RZKVCKQnP%2BJhyprb8mcLFoQ86xyheUk7V1wBI53SlNalpIeiZEAnwibnwBxAuKnHyan446S83cruRp5IqJEEhjLzfZ9dTkdF%2F5mvs767wuZloTfhm7Wk2Kcjygt6CFmh5hT8BoAhiLFQG8TM4tK7YyL%2Bjr93Ekvm3gtoWXd1A1TJkrpfzbS%2B%2BXZ2ssMBCHcdxxw3SP0Sy3y18bhsUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHfClvOkjwxW8L0euuWDrN9AOTwJZ5k6XdH2IzYdedb%2BeKcBnww%2BqCeKjSX3bV3tRPjMVnRfkQSZ38Y3kHRhf5YWMEw3bsV%2BTWUdfiZ61nY1rm2KT4Gr0iVCCeIJhV8GhuUjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9JTlNUT1JFIiwiUElDS1VQX0NVUkJTSURFIl0sInNlbGVjdGlvblR5cGUiOiJMU19TRUxFQ1RFRCJ9XSwic2hpcHBpbmdBZGRyZXNzIjp7ImxhdGl0dWRlIjozOC40NzQ1LCJsb25naXR1ZGUiOi0xMjEuMzQzOCwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeUNvZGUiOiJVU0EiLCJnaWZ0QWRkcmVzcyI6ZmFsc2UsInRpbWVab25lIjoiQW1lcmljYS9Mb3NfQW5nZWxlcyJ9LCJhc3NvcnRtZW50Ijp7Im5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJpbnRlbnQiOiJQSUNLVVAifSwiaW5zdG9yZSI6ZmFsc2UsImRlbGl2ZXJ5Ijp7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsIm5vZGVUeXBlIjoiU1RPUkUiLCJhZGRyZXNzIjp7InBvc3RhbENvZGUiOiI5NTgyOSIsImFkZHJlc3NMaW5lMSI6Ijg5MTUgR2VyYmVyIFJvYWQiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5IjoiVVMiLCJwb3N0YWxDb2RlOSI6Ijk1ODI5LTAwMDAifSwiZ2VvUG9pbnQiOnsibGF0aXR1ZGUiOjM4LjQ4MjY3NywibG9uZ2l0dWRlIjotMTIxLjM2OTAyNn0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImFjY2Vzc1BvaW50cyI6W3siYWNjZXNzVHlwZSI6IkRFTElWRVJZX0FERFJFU1MifV0sImh1Yk5vZGVJZCI6IjMwODEiLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJzdXBwb3J0ZWRBY2Nlc3NUeXBlcyI6WyJERUxJVkVSWV9BRERSRVNTIl0sInNlbGVjdGlvblR5cGUiOiJMU19TRUxFQ1RFRCJ9LCJyZWZyZXNoQXQiOjE2OTg0MDA0ODkwNzgsInZhbGlkYXRlS2V5IjoicHJvZDp2Mjo2ZmYxNTI4My1hY2JhLTRmZTItODljNS1hYzU5YTliODg3ZDEifQ%3D%3D; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIn0sInNoaXBwaW5nQWRkcmVzcyI6eyJ0aW1lc3RhbXAiOjE2ODU3ODc2OTkwNzksInR5cGUiOiJwYXJ0aWFsLWxvY2F0aW9uIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJkZWxpdmVyeVN0b3JlTGlzdCI6W3sibm9kZUlkIjoiMzA4MSIsInR5cGUiOiJERUxJVkVSWSIsInRpbWVzdGFtcCI6MTY5ODM5Njg4OTA3Miwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIiwic2VsZWN0aW9uU291cmNlIjpudWxsfV19LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwiYmFzZSI6Ijk1ODI5In0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOjZmZjE1MjgzLWFjYmEtNGZlMi04OWM1LWFjNTlhOWI4ODdkMSJ9; bstc=RDjQg_AX5lxPJuFgq3Nu74; mobileweb=0; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=92YHy|YnYws|yUqGy; exp-ck=YnYws4; pxcts=7ca9260b-74a6-11ee-b9ba-cadc77ff3193; xptc=assortmentStoreId%2B3081; xpm=1%2B1698396890%2BWYxqCClwKT085jYR_DbfQI~%2B0; TS01a90220=016ea84bd28377d28f6c5f8c825a73acff43907723c0c62c3b0f4412526365096a8d6e578fadcc7b7c5313bbb50b7e1d6a09790715; xptwj=qq:8c955083c3f2d971e73b:/AUbzj+G4qHiFzoBmTqw7sTxNCjaAsdlFcEBElDEMhfux6fBocZ2XuU9J43MZqv9xFdQR6jCZn1NHMlGARop+dGCrSwqG/FMfkjiqrzMhk6qDRXnGwdXkmVMkY17cQBnEuy2yF31oDbLB2VcFQm/6GzBoQa2xx4u4jyJ; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1698397083000@firstcreate:1696941267921"; xptwg=2654488666:13D200536871A40:321F50E:2AE8C8EB:145FE951:E3F58838:; _px3=2259e2ff82c009ea11f3276f67eeec8c7933f2015f7c4f4f537a810e135667b2:WTyoBm43yjHnl6DaKRhXrBuw1dVxFeMmNao4s1nyzyzay9AT/JNRr2njauNA3Q05CZCkyHcGpiOlACvkOPaeLA==:1000:eqWykI7N/3Nxz6qb3xQH+stpzInFVztcX104+VKDHUoglkWCA2mLjEu+Zknx3FqHY6MZskTdWOhU7b/cxkcRsZzn2v7xd2d0SIFOVaBuJFFm4ddlo4ejUXkO/Ta7SH2GcvS0zq5pIVgCSlg9SjcmMala24rEipeabfBpgSjY1DFP4u3vQ5vVD9nlh6dHTGDAN6J86YkunMWOWKq/mltB5LLWDt5U+hsHxwlUQFOl53E=; _pxde=67376920231ea198f43cb2a2fd4e3570e6f0c4527d7e9406b6848b481178e72e:eyJ0aW1lc3RhbXAiOjE2OTgzOTcwODU3NDh9',
    'ACID=6ff15283-acba-4fe2-89c5-ac59a9b887d1; hasACID=true; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1696941267921; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIn0sInNoaXBwaW5nQWRkcmVzcyI6eyJ0aW1lc3RhbXAiOjE2ODU3ODc2OTkwNzksInR5cGUiOiJwYXJ0aWFsLWxvY2F0aW9uIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJkZWxpdmVyeVN0b3JlTGlzdCI6W3sibm9kZUlkIjoiMzA4MSIsInR5cGUiOiJERUxJVkVSWSIsInRpbWVzdGFtcCI6MTY5Njk0MTI2ODAwMSwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIiwic2VsZWN0aW9uU291cmNlIjpudWxsfV19LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwiYmFzZSI6Ijk1ODI5In0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOjZmZjE1MjgzLWFjYmEtNGZlMi04OWM1LWFjNTlhOWI4ODdkMSJ9; userAppVersion=us-web-1.102.0-0f3d752097f13fd03499487f7cfc0f9ff879d809-1005; abqme=true; vtc=WYxqCClwKT085jYR_DbfQI; _pxhd=5a7ffd639284c9b62b5b6953d2b6554b5e4fb23e72bdea13cb0d60c5e9cb2592:5a44b304-6769-11ee-a5a3-89fdf7f6e6ba; TBV=7; _pxvid=5a44b304-6769-11ee-a5a3-89fdf7f6e6ba; pxcts=5b1312c1-6769-11ee-9d4b-928400606778; xptwj=qq:19ae55e85ed74ecb934a:FzwixoJNbjsJTKIVOxs2Y3BCAjYnbpEJ9QAEPF+vcgu7rou9eHViyjDPVj+jQqEQsDVe8eLUcM9yr4bzIXF5/EpE+3GBy+nQfjIux03VKMmH4uP0zvUVBAnki5gXoud346PderEXI4ZdwzI5dEw9RZpxrSE=; _astc=dd455cd93be2a8805fa78a0c5637c0bc; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1696943665000@firstcreate:1696941267921"; xptwg=3827596973:8C0849C3A8C7E8:1626C14:C8D91831:9FD2BC9F:ED33D50A:; TS012768cf=0178545c900bf5c440f69b21bbdea7b97f1bb93829c83cdf12d8a829eaa1f335330ed161b9a95404539366655b7d1af2acfeaa823d; TS01a90220=0178545c900bf5c440f69b21bbdea7b97f1bb93829c83cdf12d8a829eaa1f335330ed161b9a95404539366655b7d1af2acfeaa823d; TS2a5e0c5c027=0881c5dd0aab20006278b8f1c282bae24adaf3370d4de18765bc5b96e8045bf269a5218310a2b5f308691a3aad113000c4a62f799fcd6ee383055c96bd53eb55adecf63f1102cc5dd537a8a8e81ee1756147c51df92a6c584fb9a07c447c0309',
    'ACID=13f858d3-9165-43cb-bab4-a63c55e6a6a8; hasACID=true; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwMjk1NTMwNTIxNSwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzAyOTU1MzA1MjE1LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MDI5NTUzMDUyMTQsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJzZWxlY3Rpb25Tb3VyY2UiOm51bGx9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNzAyOTU1MzA1MjE1LCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6MTNmODU4ZDMtOTE2NS00M2NiLWJhYjQtYTYzYzU1ZTZhNmE4In0%3D; vtc=bqXm-Yjh0fiyfC30O9pl40; pxcts=e0a91fc6-9e1b-11ee-b6a5-16e7f3c0c35f; _pxvid=dfcedab4-9e1b-11ee-852d-cc6ba9195b8b; thx_guid=d45cea97b66ab1e12333b05cf300756a; auth=MTAyOTYyMDE4eh0UBw4CZsMoITCUpLy%2FJSKZAhO1G27GRMvlySSJu45p1%2FLh4CYkzkJMDMayVQPkr4cn%2Flzfu3Cnm2UosjL24mxfJqc48PMpEXdaud0aTiSYybEajj235v6w6v39wTDg767wuZloTfhm7Wk2KcjygobRHThsmZk%2BGcqTfIab85Qi91RLvjJ4oWxX7pdsgCM7kaNdhw7fWS2J7XYV98BtLp94fDX6wtiILXdT4QaPibQUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHYAmxWm2QCSM81oB%2BzgtGh7GRphnRVqhmKz4T4aeRpfPdIrk6V7SOwO2Q2sHD6RhS27h8BprVsmSYkJBi2ZANdkS%2BmkqvUibwJ%2ByNBdR4lDAXTGRW5wSkfxBkI28si3Kp0jyrOXbKKhH072NS%2FW0j%2FU%3D; bstc=eiIdhbpg62SIN7bXqMg3z8; mobileweb=0; xptc=assortmentStoreId%2B3081; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=1mX_Y|M65NP|WuHSe|XmyU7|j21L4|oY0CV|qKfBf|qMQpD|r97uO; exp-ck=WuHSe1XmyU71j21L41qKfBf1r97uO1; xpm=1%2B1702962355%2BbqXm-Yjh0fiyfC30O9pl40~%2B0; bm_mi=549FCFD96C94678551FEF51A3881B212~YAAQX/N0aH0UHFmMAQAAD7F9gBYxuYoIp+e5g1nqvEyexI6hSo7fx80xYZvYxOzX42gMoKUiODVyjrfCTZxzUPZoY9b4p074yFBo9qtr3dvqcrWK+Ax3QnilXHYN/O3i9uGgMgaYzDbvFsofcB+UXepCydTAGXEwlgTjcuhCK/WHSSkwgTYZBCgoH4ZvNuWxbDqSZJVHpneNw6xdnWnxgDwCiwgTl9vrLyjdg09e4T2smzNssFE245sbVMdbiBsLplNv/n7syfFu/VVXuZyCtMvNaORC85FiXoDfVzz0LARwtlqQLr1WRjGOEklBkKaf65Mxcoy8~1; ak_bmsc=2EB05928C59E3D09719A985EA17D031E~000000000000000000000000000000~YAAQX/N0aIcUHFmMAQAAg7N9gBbfI4TKoREfgSIqsmk7VQxNHd1hZRptwqQOz86rLpb4Dm8hwXmGCtpOkNohVRQHY/oG6ctA+y0cssVoASKpTSd1fEYyHDqbC+DrYiN6LJ0q930S+F4z3j6ktqalEJm6Bh4BtsGU6d5h43XeXgGRDzTLgZf+D5taWBpWzCUAb/xXmehSq77svJniJ4HmGoQYRn4fH8UeuB958xg08dR7TF3qVNpEUpqlv1WV4x9fcXDK35s9YBcHcZJUpGXMIx0+3rA/JVyuVqSLJnTzu9yOhVYgISi07np3590KzU5WW1U9oTYdQVYyvkMD3EL1LbLzXiZ10P9fl+HnFHX0V52zIFkboWpvSkdK4HVlAR/7LrWEM6PaexEzR6dCESt+Az+opps+z9JZNT6Y9W2aalGBPdzj275+3AznMTCf3hNispVbb4Im9fKO4HjsyY/rQKVgXwkJ6+IVYPDvYRFJaS1jY42zuoQknNidJ0ajbhHEwI5WkMGbsQ6ZfxAYYFUHPOj4ZWUDO1N9gnSx; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1702963573470; xptwj=qq:60c655ed12ee68048e72:54KBHIYpe8ioxt3yuo3GuRpC1WCSieQoP/HSZ4zw05vUNBqTEGjZylRV7Ee/gQKdw+4AR39Pu3s9w8UN6fC+xQaysEu/16eRafxBSlOhJPWWTVuI1BF3b300k0De/jcPtI82jkh9uGX2O4ufX9jSWMwn87b+BBU=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1702963574000@firstcreate:1702955305178"; xptwg=4227679326:558030DE400990:D7A337:D1486F6E:AEABFA5B:A48C033E:; TS01a90220=0178545c90c924ff084263c8cb94d8a93feb10ba5dcdaafa44d2d4bbee53181a33b55ab3c28c48628f7c56388b13afbe855a015aeb; bm_sv=60E1D169192E935CC6C00B0CDE127AEB~YAAQniLHF0lo0UKMAQAAEPaJgBa2H14Xw8z9j5FuLe+c+mOr9vtjWy+B1JIWMPOzFV3jf2sOwlAcIEVT7Zb0mGwx9GetlyN6e/PNbuNPGRK0tdoA/hf8gjCduTwLS8CC6z4no3eEnvY1ag+q3Dq0y4dikxDbYXvcfVujjYrVNac/v5cOfWcW40fMWhkJQX7hGxnZqmIyc+52KM8NeP9eIqEdybm44398ucnnT/mCybgp/Bn9xYLZwQBJSE7edA60x3A=~1; _px3=6e98f47b3f022f412f75d2fff334dd78b42e1c2da9dbb1ac36bcae228b1359cb:Vj9ElCDReT1PHAkl34AbO/7bhYH1iIhL+fexBxQwF/yJALJP177TUZi0QsuawJtQWCRpPtcCQy7FPqF2SO+Gpw==:1000:x43ZNK+0w/plDdsG3NfS1bezpRm0D1++lXrQuSJabg6/xCZEc7Ghp02iHShQZQWtATaAUPAyVYeZC2R8lPF2X7xPgH9OSstdEZ2g17OOrJ4Fv7E1MK6KZq0DiVn7FxyrcRAXcBHekyDMGiK6TTAugoF3QVB4LAQ7WM6kpnEbXlQLvndZW/IDW1JNGgVGCJvdRpV+VpqD6Ab7P2yv3UX4n+cRwoKWIa99iFjoZbmOuYM=; _pxde=3810374111437fdd342c82c0dbe956b1fb4871c6ade129e930a96584572ba38e:eyJ0aW1lc3RhbXAiOjE3MDI5NjM1NzUwNDJ9'
]


# In[ ]:


def get_soup(url):
    
    header = {
            'User-Agent': random.choice(User_Agent),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Downlink': '10',
            'Dpr': '1',
            'Cookie': random.choice(cookie),
            'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }

    response = requests.get(url, headers=header)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


# In[ ]:


content = get_soup('https://www.staples.com/ptd/review/24496996')
response = requests.get('https://www.staples.com/ptd/review/24496996')
soup = BeautifulSoup(response.text, 'html.parser')  
html_content = response.text   
[one for one in re.findall(r'{"rating":1,"reviewCount":(\d+)}', html_content)]  
re.findall(r'<title data-react-helmet="true">(.*?)(?: \| Staples)?</title>', html_content)
[count for count in re.findall(r'role="link" tabindex="0">(\d+\,?\d*)', html_content)]  


# In[ ]:


def get_review_staple(url):  
    # s = HTMLSession()  
    # r = s.get(url)  
    # content = r.html.html  
    response = requests.get(url)
    # soup = BeautifulSoup(response.text, 'html.parser')  
    content = response.text 
    titles = re.findall(r'"og:title" content="(.*?)(?: \| Staples)', content)

    if not titles:
        titles = re.findall(r'<title data-react-helmet="true">(.*?)(?: \| Staples)?</title>', content)


    review_counts = [count for count in re.findall(r'role="link" tabindex="0">(\d+\,?\d*)', content)]  
    if not review_counts:
        review_counts = [count for count in re.findall(r'"ratingCount":(\d+)', content)]  


    ratings = [rating[0] for rating in re.findall(r'"ratingValue":"(\d+(\.?\d+?)?)","reviewCount":', content)]

    if not ratings:
        ratings = [rating[0] for rating in re.findall(r'<span aria-label="(\d+\.?\d*) out of 5 stars"', content)] 


    list_price = [price for price in re.findall(r'"basePrice":(\d+\.?\d+),', content)] 

    discount_price = [float(price) for price in re.findall(r'Total price \$([\d+\.?\d+]+)', content)] 
    if not discount_price:
        discount_price = [0]

    ones = [one for one in re.findall(r'{"rating":1,"reviewCount":(\d+)}', content)]  
    twos = [two for two in re.findall(r'{"rating":2,"reviewCount":(\d+)}', content)]  
    threes = [three for three in re.findall(r'{"rating":3,"reviewCount":(\d+)}', content)]  
    fours = [four for four in re.findall(r'{"rating":4,"reviewCount":(\d+)}', content)]  
    fives = [five for five in re.findall(r'{"rating":5,"reviewCount":(\d+)}', content)]    


    staple_list = []  

    for title, review_count, rating, one, two, three, four, five, list_price, discount_price in zip(titles, review_counts, 
                                                                                                ratings, ones, twos, threes, fours, fives,
                                                                                                list_price, discount_price):  
        staple = {}  
        staple['HP Model'] = title  
        staple['HP Rating Count'] = review_count  
        staple['HP Rating'] = rating  
        staple['HP list price'] = list_price 
        staple['HP discount price'] = discount_price 
        staple['1star'] = one  
        staple['2star'] = two  
        staple['3star'] = three  
        staple['4star'] = four  
        staple['5star'] = five 
        staple['HP url 1'] = url 
        staple_list.append(staple) 
        print(staple_list)
    
    

    return staple_list  


# In[ ]:


sheets = "Staples"
url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_list = url['HP URL'].to_list()
url_list = []
for value in all_list:
    if value is not None and value not in url_list:
        url_list.append(value)
print(len(url_list))


# In[ ]:


staple_review = []  
for url in url_list:  
    print(url)
    staple_review.extend(get_review_staple(url))  
#     print(staple_review)

# %%
from datetime import datetime, date
staple_marcom = pd.DataFrame(staple_review)  
staple_marcom['scraping_date'] = date.today().strftime('%Y-%m-%d')  
staple_marcom['Retailer'] = "Staples"  
staple_marcom['HPModel'] = staple_marcom['HP Model'].str.extract(r'(^HP [A-Za-z]*\s*[A-Za-z]*\s*\d+[a-zA-Z]*)', expand=False)  
staple_marcom = staple_marcom.melt(id_vars=['HPModel', 'Retailer', 'HP Rating', 'HP Rating Count', 'HP list price', 'HP discount price', 'HP url 1', 'scraping_date'],  
                                   value_vars=['1star', '2star', '3star', '4star', '5star'],  
                                   var_name='Ratings',  
                                   value_name='HP Rating breakdown')  
  
staple_marcom = staple_marcom.sort_values(['HPModel', 'Ratings'])  
staple_marcom['HP Model Number'] = staple_marcom['HPModel'].str.extract(r'(\d+[e]*[d]*)')  
  
staple_marcom


# In[ ]:


# ## Staple competitor

# %%
# comp
def get_review_staple_competitor(url):  
    # s = HTMLSession()  
    # r = s.get(url)  
    # content = r.html.html 
    response = requests.get(url)
    content = response.text 
    titles = re.findall(r'"og:title" content="(.*?)(?: \| Staples)', content)
    if not titles:
        titles = re.findall(r'<title data-react-helmet="true">(.*?)(?: \| Staples)?</title>', content)
    
    review_counts = [count for count in re.findall(r'role="link" tabindex="0">(\d+\,?\d*)', content)]  
    if not review_counts:
        review_counts = [count for count in re.findall(r'"ratingCount":(\d+)', content)]  
    
      
    ratings = [rating[0] for rating in re.findall(r'"ratingValue":"(\d+(\.?\d+?)?)","reviewCount":', content)]
        
    if not ratings:
        ratings = [rating[0] for rating in re.findall(r'<span aria-label="(\d+\.?\d*) out of 5 stars"', content)] 
    

    list_price = [price for price in re.findall(r'"basePrice":(\d+\.?\d+),', content)] 
    
    discount_price = [float(price) for price in re.findall(r'Total price \$([\d+\.?\d+]+)', content)] 
    if not discount_price:
        discount_price = [0]
        
    ones = [one for one in re.findall(r'{"rating":1,"reviewCount":(\d+)}', content)]  
    twos = [two for two in re.findall(r'{"rating":2,"reviewCount":(\d+)}', content)]  
    threes = [three for three in re.findall(r'{"rating":3,"reviewCount":(\d+)}', content)]  
    fours = [four for four in re.findall(r'{"rating":4,"reviewCount":(\d+)}', content)]  
    fives = [five for five in re.findall(r'{"rating":5,"reviewCount":(\d+)}', content)]    
     
  
    staple_list = []  
  
    for title, review_count, rating, one, two, three, four, five, list_price, discount_price in zip(titles, review_counts, 
                                                                                                    ratings, ones, twos, threes, fours, fives,
                                                                                                    list_price, discount_price):  
        staple_comp = {}  
        staple_comp['CompModel'] = title  
        staple_comp['Comp Rating Count'] = review_count  
        staple_comp['Comp Rating'] = rating  
        staple_comp['Comp list price'] = list_price 
        staple_comp['Comp discount price'] = discount_price 
        staple_comp['1star'] = one  
        staple_comp['2star'] = two  
        staple_comp['3star'] = three  
        staple_comp['4star'] = four  
        staple_comp['5star'] = five 
        staple_comp['Comp url 1'] = url 
        staple_list.append(staple_comp)  
        print(staple_list)
    return staple_list  


# In[ ]:


sheets = "Staples"
url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_list = url['Competitor URL'].to_list()

url_list = []
for value in all_list:
    if value is not None and value not in url_list:
        url_list.append(value)
        
print(len(url_list))

# %%
# comp
staple_review_comp = []  
for url in url_list:  
    print(url)
    staple_review_comp.extend(get_review_staple_competitor(url))  

# %%
staple_marcom2 = pd.DataFrame(staple_review_comp)  
  
staple_marcom2['Competitor Brand'] = staple_marcom2['CompModel'].str.extract(r'^(.*?) ')  
staple_marcom2['Competitor Model'] = staple_marcom2['CompModel'].str.extract(r' (.*?\d+a*)')  
  
staple_marcom2 = staple_marcom2.melt(id_vars=['Competitor Brand', 'Competitor Model', 'Comp Rating', 'Comp Rating Count', 'Comp list price', 'Comp discount price', 'Comp url 1'],  
                                     value_vars=['1star', '2star', '3star', '4star', '5star'],  
                                     var_name='Ratings',  
                                     value_name='Comp Rating breakdown')  
  
staple_marcom2['Comp Model number'] = staple_marcom2['Competitor Model'].str.extract(r'(\d+)')  
  
staple_marcom2


# In[ ]:


staple1 = pd.merge(staple_marcom, df_amazon, on='HP Model Number', how='left')  
staple = pd.merge(staple1, staple_marcom2, on=['Comp Model number', 'Ratings'], how='left') 

staple = staple.sort_values(['HP Class','HP Model','Ratings'])

selected_column_final = [
    'HP Class',
    'HP Model',  
    'Retailer',
    'Comp Model',
    'HP Rating',  
    'HP Rating Count',  
    'Comp Rating', 'Comp Rating Count',
    'HP list price','HP discount price', 'Comp list price', 'Comp discount price',
    'Ratings', 
    'HP Rating breakdown',
    'Comp Rating breakdown',
    'HP url 1',
    'Comp url 1',
    'scraping_date'
    
]  

staple = staple[selected_column_final] 
staple.drop_duplicates(inplace = True)
staple['Country'] = 'US'
staple 


# In[ ]:


final_review= pd.concat([final_review, staple], ignore_index = True)

# %%
final_review 

# file_path = r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\staples_star_ratings.csv"
# final_review.to_csv(file_path, index = False)


#walmart New version anam start

def get_review_walmart(soup):  
    def safe_get(data, keys, default=None):
        for key in keys:
            try:
                data = data[key]
            except (KeyError, TypeError):
                return default
        return data

    extracted_reviews = []
    product = {}
    walmar_rating = {}  

    script_tag = soup.find('script', id="__NEXT_DATA__", type="application/json")
    if not script_tag:
        print("API Error: <script> tag not found.")
        return extracted_reviews

    try:
        data = json.loads(script_tag.string)
    except json.JSONDecodeError:
        print("API Error: JSON data could not be decoded.")
        return extracted_reviews

    # Extract product details using the helper function
    product["title"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'product', 'name'], "N/A")
    product["rating"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'averageOverallRating'], "N/A")
    product["review_count"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'totalReviewCount'], 0)
    product["discount_price"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'product', 'priceInfo', 'currentPrice', 'price'], "N/A")
    product["list_price"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'product', 'secondaryOfferPrice', 'currentPrice', 'price'], "N/A")
    walmar_rating["5star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueFiveCount'], 0)
    walmar_rating["4star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueFourCount'], 0)
    walmar_rating["3star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueThreeCount'], 0)
    walmar_rating["2star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueTwoCount'], 0)
    walmar_rating["1star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueOneCount'], 0)
    product['rating_breakdown'] = walmar_rating 

    extracted_reviews.append(product)
    print(extracted_reviews)
    return extracted_reviews

  
  


# %%
sheets = "Walmart"
url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_list = url['HP URL'].to_list()
link_list = []
for value in all_list:
    if value is not None and value not in link_list:
        link_list.append(value)
        
print(len(link_list))


# %%
sheets = "api"
api= pd.read_excel(excel_file_path, sheet_name = sheets)
api_key = api['API'][0]



#%%
HP_walmart_review_list = []    
    
for link in link_list:    
    walmart_review_HP = {}  
    while True:
        try:
            response = requests.get("https://api.scrapingdog.com/scrape", params={
            'api_key': api_key,
            'url': link,
            'dynamic': 'false',
            })
            print(link)
            soup = BeautifulSoup(response.text, 'html.parser')  
            review_data = get_review_walmart(soup)    
            walmart_review_HP.update(review_data[0])    
            walmart_review_HP['HP url 1'] = link    
            print(walmart_review_HP)  
            HP_walmart_review_list.append(walmart_review_HP)  
            time.sleep(5)
            break 
        except Exception:  
            print(f"Error encountered. Retrying in 3 seconds...")  
            time.sleep(3)

# %%
from datetime import date
walmart= pd.DataFrame(HP_walmart_review_list)
walmart.dropna(inplace = True)
walmart['Retailer']="Walmart"
walmart['HP Model Number'] = walmart['title'].str.extract(r'(\d+[e]*)', expand=False)
walmart['scraping_date'] = date.today().strftime('%Y-%m-%d')
walmart['HP Rating Count'] = walmart['review_count']
walmart['HP Rating'] = walmart['rating']

walmart['HP list price']  = walmart['list_price'] 
walmart['HP discount price'] = walmart['discount_price'] 

walmart['each_rating'] = walmart['rating_breakdown'].apply(lambda x: [(k, v) for k, v in x.items()])  
walmart = walmart.explode('each_rating')  
walmart[['Ratings', 'HP Rating breakdown']] = pd.DataFrame(walmart['each_rating'].tolist(), index=walmart.index)
walmart

# %%
sheets = "Walmart"
url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_link_list = url['Competitor URL'].to_list()
link_list = []

for value in all_link_list:
    if value is not None and value not in link_list:
        link_list.append(value)
print(len(link_list))

# %%
walmart_review_list = []    
    
for link in link_list:    
    walmart_review = {}  
    while True:
        try:
            response = requests.get("https://api.scrapingdog.com/scrape", params={
            'api_key': api_key,
            'url': link,
            'dynamic': 'false',
            })
            print(link)
            soup = BeautifulSoup(response.text, 'html.parser')  
            review_data = get_review_walmart(soup)      
            walmart_review.update(review_data[0])    
            walmart_review['Comp url 1'] = link    
            print(walmart_review)  
            walmart_review_list.append(walmart_review)  
            time.sleep(5)
            break
        
        except Exception:  
                print(f"Error encountered. Retrying in 3 seconds...")  
                time.sleep(3) 

# %%
walmart_comp= pd.DataFrame(walmart_review_list)
walmart_comp.dropna(inplace = True)
walmart_comp['Comp Model number'] = walmart_comp['title'].str.extract(r'(\d+)', expand=False)

walmart_comp['Comp Rating Count'] = walmart_comp['review_count']

walmart_comp['Comp Rating'] = walmart_comp['rating']

walmart_comp['Key Competitor Brand'] = walmart_comp['title'].str.extract(r'^(.*?) ')
walmart_comp['Comp list price']  = walmart_comp['list_price'] 
walmart_comp['Comp discount price'] = walmart_comp['discount_price'] 

walmart_comp['each_rating'] = walmart_comp['rating_breakdown'].apply(lambda x: [(k, v) for k, v in x.items()])  
walmart_comp = walmart_comp.explode('each_rating')  
walmart_comp[['Ratings', 'Comp Rating breakdown']] = pd.DataFrame(walmart_comp['each_rating'].tolist(), index=walmart_comp.index)

walmart_comp.head()

# %% [markdown]
# ## Merge walmart comp HP

# %%
walmart_final1 = pd.merge(walmart, df_amazon, on = 'HP Model Number', how = 'left')
walmart_final2 =  pd.merge(walmart_final1, walmart_comp, on = ['Comp Model number','Ratings'], how = 'left')
selected_column_final = [
    'HP Class',
    'HP Model',  
    'Retailer',
    'Comp Model',
    'HP Rating',  
    'HP Rating Count',  
    'Comp Rating', 'Comp Rating Count',
    'HP list price', 'HP discount price',
     'Comp list price', 'Comp discount price',
    'Ratings', 
    'HP Rating breakdown',
    'Comp Rating breakdown',
    'HP url 1',
    'Comp url 1' ,
    'scraping_date'  ,
    'Country'    
    
    
]  

walmart_final2['Country'] = 'US'
walmart_final = walmart_final2[selected_column_final] 
walmart_final = walmart_final.drop_duplicates()


walmart_final

# %%
final_review= pd.concat([final_review, walmart_final], ignore_index = True)
final_review

#walmart New version anam start


# In[ ]:


def get_review_bestbuy(url, max_attempts=5):    
    attempts = 0

    while attempts < max_attempts:
        try:

            s = HTMLSession()
            r = s.get(url)
            r.raise_for_status()  # Raise an HTTPError for bad responses

            content = r.html.html

            title_match = re.search(r'"@type":"Product","name":"(HP[^"]*)"', content)
            titles = [title_match.group(1)] if title_match else []
            if titles:
                review_count = [count for count in re.findall(r'"reviewCount":(\d+)', content)] 
                review_counts = review_count if review_count else [0]
                rating = [rating[0] for rating in re.findall(r'"ratingValue":(\d+(\.\d+)?),"reviewCount":(\d+)}', content)] 
                ratings = rating if rating else [0]
                discount_prices = [float(price) for price in re.findall(r'\\"currentPrice\\":([\d+\.?\d+]+)', content)]
                discount_price = discount_prices if discount_prices else [0]
                # if not discount_price:
                #     discount_price = [float(price) for price in re.findall(r'"currentPrice":([\d+\.?\d+]+)', content)]

                list_price_match = re.search(r'\\"regularPrice\\":([\d+\.?\d+]+)', content)
                list_price = [list_price_match.group(1)] if list_price_match else [0]

                if not list_price_match:
                    list_price_match = re.search(r'"regularPrice":([\d+\.?\d+]+)', content)
                    list_price = [list_price_match.group(1)] if list_price_match else [0]

#                 ones = [one for one in re.findall(r'{"value":1,"count":(\d+),', content)]
#                 twos = [two for two in re.findall(r'{"value":2,"count":(\d+),', content)]
#                 threes = [three for three in re.findall(r'{"value":3,"count":(\d+),', content)]
#                 fours = [four for four in re.findall(r'{"value":4,"count":(\d+),', content)]
#                 fives = [five for five in re.findall(r'{"value":5,"count":(\d+),', content)]
                
                ones = [count for count in re.findall(r'{"value":1,"count":(\d+),', content)] or [count for count in re.findall(r'{\\"value\\":1,\\"count\\":(\d+),', content)] or ['0']
                twos = [count for count in re.findall(r'{"value":2,"count":(\d+),', content)]  or [count for count in re.findall(r'{\\"value\\":2,\\"count\\":(\d+),', content)] or ['0']
                threes =[count for count in re.findall(r'{"value":3,"count":(\d+),', content)] or [count for count in re.findall(r'{\\"value\\":3,\\"count\\":(\d+),', content)] or ['0']
                fours = [count for count in re.findall(r'{"value":4,"count":(\d+),', content)] or [count for count in re.findall(r'{\\"value\\":4,\\"count\\":(\d+),', content)] or ['0']
                fives = [count for count in re.findall(r'{"value":5,"count":(\d+),', content)] or [count for count in re.findall(r'{\\"value\\":5,\\"count\\":(\d+),', content)] or ['0']
                
                bestbuy_list = [] 

                for title, list_price, discount_price, review_count, rating, one, two, three, four, five in zip(titles, list_price, discount_price, review_counts, ratings, ones, twos, threes, fours, fives):  
                    bestbuy = {}
                    bestbuy['HPModel'] = title if title is not None else ""
                    bestbuy['HP Rating Count'] = review_count if review_count is not None else 0
                    bestbuy['HP Rating'] = rating if rating is not None else 0
                    bestbuy['HP list price'] = list_price if list_price is not None else 0
                    bestbuy['HP discount price'] = discount_price if discount_price is not None else 0
                    bestbuy['1star'] = one if one is not None else 0
                    bestbuy['2star'] = two if two is not None else 0
                    bestbuy['3star'] = three if three is not None else 0
                    bestbuy['4star'] = four if four is not None else 0
                    bestbuy['5star'] = five if five is not None else 0
                    bestbuy['HP url 1'] = url

                    
                    
                    print(bestbuy)
                    bestbuy_list.append(bestbuy)

                return bestbuy_list

            else:
                attempts += 1
                print(f"Attempt {attempts} failed. Retrying after 2 seconds...")
                time.sleep(2)

        except Exception as e:
            print(f"An error occurred: {e}")
            attempts += 1
            print(f"Attempt {attempts} failed. Retrying after 2 seconds...")
            time.sleep(2)
    return {}


# In[ ]:


sheets = "Bestbuy"

url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_url_list = url['HP URL'].to_list()
url_list = []
for value in all_url_list:
    if value is not None and value not in url_list:
        url_list.append(value)
        
print(len(url_list))

# %%
bestbuy_review = []

# Assuming url_list is defined somewhere
for url in url_list:
    print(url)
    print()
    bestbuy_review.extend(get_review_bestbuy(url))
    print()

# %%
bestbuy_marcom= pd.DataFrame(bestbuy_review)
bestbuy_marcom['scraping_date'] = date.today().strftime('%Y-%m-%d')
bestbuy_marcom['Retailer']="Best Buy"
bestbuy_marcom['HPModel'] = bestbuy_marcom['HPModel'].str.split('Wireless', n=1, expand=True)[0].str.strip() 
bestbuy_marcom['HPModel'] = bestbuy_marcom['HPModel'].str.replace('-', '')
bestbuy_marcom = bestbuy_marcom.melt(id_vars=['HPModel', 'Retailer', 'HP Rating', 'HP Rating Count', 'HP list price', 'HP discount price', 'HP url 1','scraping_date'],  
                                   value_vars=['1star', '2star', '3star', '4star', '5star'],  
                                   var_name='Ratings',  
                                   value_name='HP Rating breakdown')    
bestbuy_marcom.sort_values(['HPModel','Ratings'])
bestbuy_marcom['HP Model Number'] = bestbuy_marcom['HPModel'].str.extract(r'(\d+e?)')
bestbuy_marcom


# In[ ]:


# %% [markdown]
# ## Bestbuy compe

# %%
def get_review_bestbuy_comp(url, max_attempts=20):
    attempts = 0

    while attempts < max_attempts:
        try:

            s = HTMLSession()  
            r = s.get(url)  
            content = r.html.html  
            bestbuy = {}
            title_match = re.search(r'"@type":"Product","name":"([^"]*)"', content)  
            titles = [title_match.group(1)] if title_match else []  

            review_counts = [count for count in re.findall(r'"reviewCount":(\d+)', content)]  
            ratings = [rating[0] for rating in re.findall(r'"ratingValue":(\d+(\.\d+)?),"reviewCount":(\d+)}', content)]  


            discount_price = [float(price) for price in re.findall(r'\\"currentPrice\\":([\d+\.?\d+]+)', content)]
            if not discount_price:
                discount_price = [float(price) for price in re.findall(r'"currentPrice":([\d+\.?\d+]+)', content)]

            list_price_match = re.search(r'\\"regularPrice\\":([\d+\.?\d+]+)', content)
            list_price = [list_price_match.group(1)] if list_price_match else [0]

            if not list_price_match:
                list_price_match = re.search(r'"regularPrice":([\d+\.?\d+]+)', content)
                list_price = [list_price_match.group(1)] if list_price_match else [0]


            ones = [one for one in re.findall(r'{"value":1,"count":(\d+),', content)]  
            twos = [two for two in re.findall(r'{"value":2,"count":(\d+),', content)]  
            threes = [three for three in re.findall(r'{"value":3,"count":(\d+),', content)]  
            fours = [four for four in re.findall(r'{"value":4,"count":(\d+),', content)]  
            fives = [five for five in re.findall(r'{"value":5,"count":(\d+),', content)]  
            
            bestbuy_list = [] 
            
            for title, list_price, discount_price, review_count, rating, one, two, three, four, five in zip(titles, list_price, discount_price, review_counts, ratings, ones, twos, threes, fours, fives):      
                bestbuy['CompModel'] = title  
                bestbuy['Comp Rating Count'] = review_count  
                bestbuy['Comp Rating'] = rating  
                bestbuy['Comp list price'] = list_price  
                bestbuy['Comp discount price'] = discount_price  
                bestbuy['1star'] = one  
                bestbuy['2star'] = two  
                bestbuy['3star'] = three  
                bestbuy['4star'] = four  
                bestbuy['5star'] = five  
                bestbuy['Comp url 1']= url 

                
                print(bestbuy)
                bestbuy_list.append(bestbuy)

            return bestbuy_list

        except Exception as e:
            print(f"An error occurred: {e}")
            attempts += 1
            print(f"Attempt {attempts} failed. Retrying after 2 seconds...")
            time.sleep(2)


  


# %%
sheets = "Bestbuy"

url = pd.read_excel(excel_file_path, sheet_name = sheets)
all_list = url['Competitor URL'].to_list()

url_list = []
for value in all_list:
    if value is not None and value not in url_list:
        url_list.append(value)
print(len(url_list))

# %%
bestbuy_review_comp = []  
for url in url_list: 
    print(url)
    bestbuy_review_comp.extend(get_review_bestbuy_comp(url))  
    print()

# %%
marcom2= pd.DataFrame(bestbuy_review_comp)
marcom2['Competitor Brand'] = marcom2['CompModel'].str.extract(r'^(.*?) ')
marcom2['Competitor'] = marcom2['CompModel'].str.extract(r' -(.*?\d+a*)')
marcom2['Competitor Model'] = marcom2['Competitor'].str.strip()

marcom2 = marcom2.melt(id_vars=['Competitor Brand','Competitor Model',  'Comp Rating', 'Comp Rating Count', 'Comp list price', 'Comp discount price','Comp url 1'],  
                                   value_vars=['1star', '2star', '3star', '4star', '5star'],  
                                   var_name='Ratings',  
                                   value_name='Comp Rating breakdown')  

marcom2['Comp Model number'] = marcom2['Competitor Model'].str.extract(r'(\d+)')


marcom2


# In[ ]:


# ## Combine HP and comp

# %%
combine = pd.merge(bestbuy_marcom, df_amazon, on='HP Model Number', how='left')  
bestbuy = pd.merge(combine, marcom2, on=['Comp Model number', 'Ratings'], how='left')  
bestbuy['Competitor Model'] = bestbuy['Competitor Brand'] + bestbuy['Competitor Model']
column_order = ['HP Class','HP Model', 'Retailer','Comp Model', 'HP Rating', 'HP Rating Count', 'Comp Rating', 'Comp Rating Count',  
                'HP list price', 'HP discount price', 'Comp list price', 'Comp discount price', 'Ratings',  
                'HP Rating breakdown', 'Comp Rating breakdown', 'HP url 1', 'Comp url 1','scraping_date']  
bestbuy = bestbuy.reindex(columns=column_order)  
  
bestbuy = bestbuy.reset_index(drop=True)  
bestbuy = bestbuy.sort_values(['HP Class','HP Model','Ratings'])
bestbuy['Country'] = 'US'

bestbuy


# In[ ]:


final_review= pd.concat([final_review, bestbuy], ignore_index = True)
final_review

# file_path = r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\bestbuy_star_ratings.csv"
# final_review.to_csv(file_path, index = False)


# In[ ]:


# # HHO

# %%
sheets = "HHO"

sku = pd.read_excel(excel_file_path, sheet_name = sheets)
all_sku_list = sku['SKU'].to_list()
sku_list = []
for value in all_sku_list:
    if value is not None and value not in sku_list:
        sku_list.append(value)
        
print(len(sku_list))


# In[ ]:


def hho_rating(sku, df = None):
    api_url = 'https://api.bazaarvoice.com/data/products.json'
    params = {
        'resource': 'products',
        'filter': f'id:eq:{sku}',
        'filter_reviews': 'contentlocale:eq:en_US,en_CA',
        'filter_reviewcomments': 'contentlocale:eq:en_US,en_CA',
        'filteredstats': 'Reviews',
        'stats': 'Reviews,questions,answers',
        'passkey': 'caBZoE5X0dmsywGCMoQmo6OPymWLQFnY37VernuC3SGkY',
        'apiversion': '5.5',
        'displaycode': '8843-en_us'
    }

    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        data = json.loads(response.text)
        
    product_info = data['Results'][0]
    rating_info = product_info['ReviewStatistics']

    if df is None:
        df = pd.DataFrame()
    for rating in rating_info['RatingDistribution']:
        # Create a temporary DataFrame for each rating to append to the main DataFrame
        temp_df = pd.DataFrame({
            'Ratings': [str(rating["RatingValue"]) + 'star'],
            'HP Rating breakdown': [rating['Count']],
            'HPModel': [product_info['Name']],
            'HP Rating': [rating_info['AverageOverallRating']],
            'HP Rating Count': [product_info['FilteredReviewStatistics']['TotalReviewCount']],
            'HP url 1': [product_info['ProductPageUrl']]
        })

        # Append the temporary DataFrame to the main DataFrame using pd.concat()
        df = pd.concat([df, temp_df], ignore_index=True)

    return df

# In[ ]:


import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")

# %%
result_df = pd.DataFrame()
for sku in sku_list:
    print(sku)
    result_df = hho_rating(sku, result_df)

# %%
result_df['HP Model Number'] = result_df['HPModel'].str.extract(r'(\d+[e]*)')  

hho1 = pd.merge(result_df, df_amazon, on='HP Model Number', how='left')  

hho1 = hho1.sort_values(['HP Class','HP Model','Ratings'])
hho1.columns 
drop_column = [
   'HPModel',  'HP Model Number', 'Comp Model number', 'Comp Model'    
]  

hho = hho1.drop(columns=drop_column)

hho.drop_duplicates(inplace = True)
hho['Retailer'] = 'HHO' 
hho['Country'] = 'US' 
hho['scraping_date'] = date.today().strftime('%Y-%m-%d')


# In[ ]:


# %%
final_review= pd.concat([final_review, hho], ignore_index = True)
final_review

# file_path = r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\hho_star_ratings.csv"
# final_review.to_csv(file_path, index = False)


# In[ ]:


# # Append to previous csv

# %%
final_review.drop_duplicates(subset = ['HP Class', 'HP Model', 'Retailer',  'HP Rating',
       'HP Rating Count','Ratings'],inplace = True) 
final_review.dropna(subset = [ 'HP Model'],inplace = True) 
Marcom_star_trend = final_review

Marcom_star_trend['scraping_date'] =  pd.to_datetime(Marcom_star_trend['scraping_date']).dt.date

Marcom_star_trend['Country'] =Marcom_star_trend['Country'].apply(lambda x: x if x is not None else 'US')
Marcom_star_trend = Marcom_star_trend.dropna(subset=['HP Model'])

# %%
# Define a mapping dictionary for column names
column_mapping = {
    'HP Class': 'HP_Class',
    'HP Model': 'HP_Model',
    'Retailer': 'Retailer',
    'Comp Model': 'Comp_Model',
    'HP Rating': 'HP_Rating',
    'HP Rating Count': 'HP_Rating_Count',
    'Comp Rating': 'Comp_Rating',
    'Comp Rating Count': 'Comp_Rating_Count',
    'Ratings': 'Ratings',
    'HP Rating breakdown': 'HP_Rating_Breakdown',
    'Comp Rating breakdown': 'Comp_Rating_Breakdown',
    'HP url 1': 'HP_url1',
    'Comp url 1': 'Comp_url1',
    'scraping_date': 'Scraping_Date',
    'Amazon HP Ratings breakdown': 'Amazon_HP_Ratings_Breakdown',
    'Amazon Comp Ratings breakdown': 'Amazon_Comp_Ratings_Breakdown',
    'HP Reviews breakdown': 'HP_Reviews_Breakdown',
    'Comp Reviews breakdown': 'Comp_Reviews_Breakdown',
    'HP url 2': 'HP_url2',
    'Comp url 2': 'Comp_url2',
    'HP Product Sold': 'HP_Product_Sold',
    'Comp Product Sold': 'Comp_Product_Sold',
    'Country': 'Country',
    'HP list price':'HP_List_Price', 
    'HP discount price':'HP_Discount_Price',
       'Comp list price':'Comp_List_Price', 
    'Comp discount price':'Comp_Discount_Price'
}

# Rename columns using the mapping dictionary
Marcom_star_trend.rename(columns=column_mapping, inplace=True)


# %%
Marcom_star_trend['HP_List_Price'] = pd.to_numeric(Marcom_star_trend['HP_List_Price'].fillna(0), errors='coerce')
Marcom_star_trend['HP_Discount_Price'] = pd.to_numeric(Marcom_star_trend['HP_Discount_Price'].fillna(0), errors='coerce')
Marcom_star_trend['Comp_List_Price'] = pd.to_numeric(Marcom_star_trend['Comp_List_Price'].fillna(0), errors='coerce')
Marcom_star_trend['Comp_Discount_Price'] = pd.to_numeric(Marcom_star_trend['Comp_Discount_Price'].fillna(0), errors='coerce')




# %%
Marcom_star_trend['HP_List_Price'] = Marcom_star_trend['HP_List_Price'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)
Marcom_star_trend['HP_Discount_Price'] = Marcom_star_trend['HP_Discount_Price'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)
Marcom_star_trend['Comp_List_Price'] = Marcom_star_trend['Comp_List_Price'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)
Marcom_star_trend['Comp_Discount_Price'] = Marcom_star_trend['Comp_Discount_Price'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)





# %%
Marcom_star_trend['HP_Rating'] = Marcom_star_trend['HP_Rating'].astype(float)
Marcom_star_trend['Comp_Rating'] = Marcom_star_trend['Comp_Rating'].replace('',0).fillna(0).astype(float)
Marcom_star_trend['Scraping_Date'] = pd.to_datetime(Marcom_star_trend['Scraping_Date']).dt.date


# %%
Marcom_star_trend['Comp_Rating_Count'] = Marcom_star_trend ['Comp_Rating_Count'].replace('',0).fillna(0).replace(',','',regex = True).astype('int64')
Marcom_star_trend['HP_Rating_Breakdown'] = Marcom_star_trend ['HP_Rating_Breakdown'].fillna(0).replace(',','',regex = True).astype('int64')
Marcom_star_trend['Comp_Rating_Breakdown'] = Marcom_star_trend ['Comp_Rating_Breakdown'].fillna(0).replace('',0).replace(',','',regex = True).astype('int64')
Marcom_star_trend['HP_Reviews_Breakdown'] = Marcom_star_trend ['HP_Reviews_Breakdown'].replace('',0).fillna(0).replace(',','',regex = True).astype('int64')
Marcom_star_trend['Comp_Reviews_Breakdown'] = Marcom_star_trend ['Comp_Reviews_Breakdown'].replace('',0).replace('',0).fillna(0).replace(',','',regex = True).astype('int64')

# %%
# Marcom_star_trend['HP_Rating_Count'] = Marcom_star_trend['HP_Rating_Count'].fillna(0)\
#                                     .replace(',', '', regex=True)\
#                                     .replace('\.', '', regex=True)\
#                                     .replace('review', '', regex=True)\
#                                     .astype('int64')

import numpy as np
import pandas as pd

def clean_hp_rating_count(value):
    if pd.isna(value):
        return 0
    value_str = str(value)  # Convert the value to a string
    if "1 global rating" in value_str:
        return 1
    else:
        # Clean and convert the value
        cleaned_value = value_str.replace(',', '').replace('.', '').replace('review', '').strip()
        try:
            return int(cleaned_value) if cleaned_value else 0
        except ValueError:
            return 0  # Return 0 if conversion fails

Marcom_star_trend['HP_Rating_Count'] = Marcom_star_trend['HP_Rating_Count'].apply(clean_hp_rating_count)


# %%
# Fill NaN values for string columns
Marcom_star_trend['Comp_Model'] = Marcom_star_trend['Comp_Model'].fillna("").replace('0', "").replace(0, "")
Marcom_star_trend['HP_url1'] = Marcom_star_trend['HP_url1'].fillna("").replace('0', "").replace(0, "")
Marcom_star_trend['Comp_url1'] = Marcom_star_trend['Comp_url1'].fillna("").replace('0', "").replace(0, "")

Marcom_star_trend['Amazon_HP_Ratings_Breakdown'].fillna(0, inplace=True)  
Marcom_star_trend['Amazon_HP_Ratings_Breakdown'] = pd.to_numeric(Marcom_star_trend ['Amazon_HP_Ratings_Breakdown'].astype(str).str.rstrip('%'), errors = 'coerce')/100
Marcom_star_trend['Amazon_Comp_Ratings_Breakdown'].fillna(0, inplace=True)  
Marcom_star_trend['Amazon_Comp_Ratings_Breakdown'] = pd.to_numeric(Marcom_star_trend ['Amazon_Comp_Ratings_Breakdown'].astype(str).str.rstrip('%'), errors = 'coerce')/100

Marcom_star_trend['HP_Rating'] = Marcom_star_trend['HP_Rating'].round(1)
Marcom_star_trend['Comp_Rating'] = Marcom_star_trend['Comp_Rating'].round(1)  

# %%
string_columns = Marcom_star_trend.select_dtypes(include=['object']).columns
for i in string_columns:
    Marcom_star_trend[i].fillna("",inplace = True)

int_columns = Marcom_star_trend.select_dtypes(include=[int,float]).columns
for i in int_columns:
    Marcom_star_trend[i].fillna(0,inplace = True)




# %%
Marcom_star_trend.drop(columns = 'Segment',inplace = True)


# In[ ]:


Marcom_star_trend


# In[ ]:


import datetime 
date = datetime.date.today().strftime('%Y%m%d')
path = r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\archive\Daily_rating_trend"
Marcom_star_trend.to_csv(f'{path}\Marcom_star_trend{date}.csv') 


# In[ ]:


# %%
previous = pd.read_csv(r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\Star_rating_trend.csv")
previous.drop_duplicates(subset = ['HP_Class', 'HP_Model', 'Retailer', 'Comp_Model', 'HP_Rating',
       'HP_Rating_Count','Ratings','Scraping_Date'],inplace = True) 
previous


# In[ ]:


# %%
update = pd.concat([previous, Marcom_star_trend])
update.drop_duplicates(subset = ['HP_Class', 'HP_Model', 'Retailer', 'Comp_Model', 'HP_Rating',
       'HP_Rating_Count','Ratings','Scraping_Date'],inplace = True) 

update['Scraping_Date'] = pd.to_datetime(update['Scraping_Date']).dt.date


# %%
# total 27 columns
len(update.columns)

# %%
import datetime 
date = datetime.date.today().strftime('%Y%m%d')
path = r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\archive\Star rating trend historical file"
update.to_csv(f'{path}\Star_rating_trend_{date}.csv',index = False)


# In[ ]:


update.drop_duplicates(inplace = True)
update.to_csv(r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\Star_rating_trend.csv",index = False)

