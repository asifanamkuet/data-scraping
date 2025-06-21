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



print('Running Core_ink_rating_scraping_wout_SMSS.py')

excel_file_path = r"Star rating scrape URL and info - NPI.xlsx"
sheet_name = "data_new"

# Read the Excel sheet into a DataFrame
df_amazon = pd.read_excel(excel_file_path, sheet_name=sheet_name)
df_amazon['HP Model Number'] = df_amazon['HP Model Number'].astype(str)
df_amazon['Comp Model number'] = df_amazon['Comp Model number'].fillna(0).round(0).astype(int).astype(str)

#walmart New version anam start


import json

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

    # Locate and parse the JSON data in the <script> tag
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
    product["5star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueFiveCount'], 0)
    product["4star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueFourCount'], 0)
    product["3star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueThreeCount'], 0)
    product["2star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueTwoCount'], 0)
    product["1star"] = safe_get(data, ['props', 'pageProps', 'initialData', 'data', 'reviews', 'ratingValueOneCount'], 0)

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


url = 'https://www.walmart.com/ip/Canon-PIXMA-TS3522-All-in-One-Wireless-InkJet-Printer-with-Print-Copy-and-Scan-Features/722961036?athbdg=L1600&from=/search'

response = requests.get("https://api.scrapingdog.com/scrape", params={
  'api_key': api_key,
  'url': url,
  'dynamic': 'false',
  })

soup = BeautifulSoup(response.text, 'html.parser')  
#print(soup)
get_review_walmart(soup)


# %%
# HP_walmart_review_list = []    
    
# for link in link_list:    
#     walmart_review_HP = {}  
#     while True:
#         try:
#             url = f"https://api.scrapingdog.com/scrape?api_key={api_key}&url={link}"
#             print(url)
#             review_data = get_review_walmart(url, max_retries=3)    
#             walmart_review_HP.update(review_data[0])    
#             walmart_review_HP['HP url 1'] = link    
#             print(walmart_review_HP)  
#             HP_walmart_review_list.append(walmart_review_HP)  
#             time.sleep(5)
#             break 
#         except Exception:  
#             print(f"Error encountered. Retrying in 3 seconds...")  
#             time.sleep(3)

# # %%
# from datetime import date
# walmart= pd.DataFrame(HP_walmart_review_list)
# walmart.dropna(inplace = True)
# walmart['Retailer']="Walmart"
# walmart['HP Model Number'] = walmart['title'].str.extract(r'(\d+[e]*)', expand=False)
# walmart['scraping_date'] = date.today().strftime('%Y-%m-%d')
# walmart['HP Rating Count'] = walmart['review_count'].str.replace('reviews', '')
# walmart['HP Rating'] = walmart['rating']

# walmart['HP list price']  = walmart['list_price'] 
# walmart['HP discount price'] = walmart['discount_price'] 

# walmart['each_rating'] = walmart['rating_breakdown'].apply(lambda x: [(k, v) for k, v in x.items()])  
# walmart = walmart.explode('each_rating')  
# walmart[['Ratings', 'HP Rating breakdown']] = pd.DataFrame(walmart['each_rating'].tolist(), index=walmart.index)
# walmart

# # %%
# sheets = "Walmart"
# url = pd.read_excel(excel_file_path, sheet_name = sheets)
# all_link_list = url['Competitor URL'].to_list()
# link_list = []

# for value in all_link_list:
#     if value is not None and value not in link_list:
#         link_list.append(value)
# print(len(link_list))

# # %%
# walmart_review_list = []    
    
# for link in link_list:    
#     walmart_review = {}  
#     while True:
#         try:
#             url = f"https://api.scrapingdog.com/scrape?api_key={api_key}&url={link}"    
#             review_data = get_review_walmart(url, max_retries=3)    
#             walmart_review.update(review_data[0])    
#             walmart_review['Comp url 1'] = link    
#             print(walmart_review)  
#             walmart_review_list.append(walmart_review)  
#             time.sleep(5)
#             break
        
#         except Exception:  
#                 print(f"Error encountered. Retrying in 3 seconds...")  
#                 time.sleep(3) 

# # %%
# walmart_comp= pd.DataFrame(walmart_review_list)
# walmart_comp.dropna(inplace = True)
# walmart_comp['Comp Model number'] = walmart_comp['title'].str.extract(r'(\d+)', expand=False)

# walmart_comp['Comp Rating Count'] = walmart_comp['review_count'].str.replace('reviews', '')

# walmart_comp['Comp Rating'] = walmart_comp['rating']

# walmart_comp['Key Competitor Brand'] = walmart_comp['title'].str.extract(r'^(.*?) ')
# walmart_comp['Comp list price']  = walmart_comp['list_price'] 
# walmart_comp['Comp discount price'] = walmart_comp['discount_price'] 

# walmart_comp['each_rating'] = walmart_comp['rating_breakdown'].apply(lambda x: [(k, v) for k, v in x.items()])  
# walmart_comp = walmart_comp.explode('each_rating')  
# walmart_comp[['Ratings', 'Comp Rating breakdown']] = pd.DataFrame(walmart_comp['each_rating'].tolist(), index=walmart_comp.index)

# walmart_comp.head()

# # %% [markdown]
# # ## Merge walmart comp HP

# # %%
# walmart_final1 = pd.merge(walmart, df_amazon, on = 'HP Model Number', how = 'left')
# walmart_final2 =  pd.merge(walmart_final1, walmart_comp, on = ['Comp Model number','Ratings'], how = 'left')
# selected_column_final = [
#     'HP Class',
#     'HP Model',  
#     'Retailer',
#     'Comp Model',
#     'HP Rating',  
#     'HP Rating Count',  
#     'Comp Rating', 'Comp Rating Count',
#     'HP list price', 'HP discount price',
#      'Comp list price', 'Comp discount price',
#     'Ratings', 
#     'HP Rating breakdown',
#     'Comp Rating breakdown',
#     'HP url 1',
#     'Comp url 1' ,
#     'scraping_date'  ,
#     'Country'    
    
    
# ]  

# walmart_final2['Country'] = 'US'
# walmart_final = walmart_final2[selected_column_final] 
# walmart_final = walmart_final.drop_duplicates()


# walmart_final

# # %%
# final_review= pd.concat([final_review, walmart_final], ignore_index = True)
# final_review


#walmart New version anam start