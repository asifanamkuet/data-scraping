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
import requests
import random
import urllib.parse
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Disable urllib3 warnings
urllib3.disable_warnings(InsecureRequestWarning)

# Get the current date and time
now = datetime.now()

# Format the timestamp as a string
timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

# Print the timestamp
print("Current Timestamp:", timestamp)

print('Running MMK_insiders_scraping_wout_walmart.py')

# %%
excel_file_path = r"Star rating scrape URL and info - NPI.xlsx"
# excel_file_path = r"C:\Users\TaYu430\anaconda3\envs\webscrap\My Scripts\Star rating scrape URL and info - NPI.xlsx"
# excel_file_path = r"Star rating scrape URL and info - NPI.xlsx"
sheet_name = "data_new"

# Read the Excel sheet into a DataFrame
df_amazon = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')
df_amazon['HP Model Number'] = df_amazon['HP Model Number'].astype(str)
df_amazon['Comp Model number'] = df_amazon['Comp Model number'].fillna(0).round(0).astype(int).astype(str)
df_amazon

# %%
path = r"Star rating scrape URL and info - NPI.xlsx"
# path = r"C:\Users\TaYu430\anaconda3\envs\webscrap\My Scripts\Star rating scrape URL and info - NPI.xlsx"
# path = r"Star rating scrape URL and info - NPI.xlsx"
sheets = 'review_template'
review_template = pd.read_excel(path, sheet_name=sheets, engine='openpyxl')
review_template


# %%
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


urls = ['https://www.staples.com/ptd/review/24583383',
        'https://www.staples.com/ptd/review/24583386',
        'https://www.staples.com/ptd/review/24583387',
        'https://www.staples.com/ptd/review/24583384',
        'https://www.staples.com/ptd/review/24590899',
        'https://www.staples.com/ptd/review/24590900']

# %%
staples_df_hp = pd.DataFrame()

for url in urls:
    sku = extract_sku_from_url(url)
    print('Get reviews from', url)
    page = max_pages(sku, url)
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
    staple_final = staple_final[
        ['id', 'dateCreated', 'title', 'text', 'rating', 'user', 'syndication', 'incentivized', 'Model']]
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
staple_final['Retailer'] = "Staples"
staple_final['scraping_date'] = pd.to_datetime(date.today())
staple_final['HP Model Number'] = staple_final['Model'].str.extract(r'(\d+e?)')

# staple_final['Review date'] = pd.to_datetime(staple['Review date'])

staple_hp_combine = pd.merge(staple_final, df_amazon, on="HP Model Number", how="left")
staple_hp_combine['Review Model'] = staple_hp_combine['HP Model']
columns_to_drop = [
    'Model', 'HP Model Number', 'Comp Model number', 'HP Model', 'id']

staple_hp_combine.drop(columns_to_drop, axis=1, inplace=True)

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
# staple_final.drop_duplicates(inplace = True)
staple_final['Review_Date'] = pd.to_datetime(staple_final['Review_Date']).dt.date

# %%

df_concat_final = pd.read_csv(r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\MMK\MMK_web_review_raw data.csv")
# df_concat_final = pd.read_csv(r"MMK_web_review_raw data.csv")
pattern = r'(\w+ \d{1,2}, \d{4})'
df_concat_final['Review_Date'] = pd.to_datetime(df_concat_final['Review_Date']).dt.date
df_concat_final['Scraping_Date'] = pd.to_datetime(df_concat_final['Scraping_Date']).dt.date


# %% [markdown]
# # Office Depot

# %%
def ojp_review(sku):
    api_url = f"https://api.bazaarvoice.com/data/batch.json?passkey=cawBtQneBQUKVGwTxna9AAvIPFJuUzw9xB3UlOt5DIrC8&apiversion=5.5&displaycode=2563-en_us&resource.q0=products&filter.q0=id%3Aeq%{sku}&stats.q0=reviews&filteredstats.q0=reviews&filter_reviews.q0=contentlocale%3Aeq%3Aen_US&filter_reviewcomments.q0=contentlocale%3Aeq%3Aen_US&resource.q1=reviews&filter.q1=productid%3Aeq%{sku}&filter.q1=contentlocale%3Aeq%3Aen_US&sort.q1=submissiontime%3Adesc&stats.q1=reviews&filteredstats.q1=reviews&include.q1=authors%2Cproducts%2Ccomments&filter_reviews.q1=contentlocale%3Aeq%3Aen_US&filter_reviewcomments.q1=contentlocale%3Aeq%3Aen_US&filter_comments.q1=contentlocale%3Aeq%3Aen_US&resource.q2=reviews&filter.q2=productid%3Aeq%{sku}&filter.q2=contentlocale%3Aeq%3Aen_US&resource.q3=reviews&filter.q3=productid%3Aeq%{sku}&filter.q3=issyndicated%3Aeq%3Afalse&filter.q3=rating%3Agt%3A3&filter.q3=totalpositivefeedbackcount%3Agte%3A3&filter.q3=contentlocale%3Aeq%3Aen_US&sort.q3=totalpositivefeedbackcount%3Adesc&include.q3=authors%2Creviews%2Cproducts&filter_reviews.q3=contentlocale%3Aeq%3Aen_US&resource.q4=reviews&filter.q4=productid%3Aeq%{sku}&filter.q4=issyndicated%3Aeq%3Afalse&filter.q4=rating%3Alte%3A3&filter.q4=totalpositivefeedbackcount%3Agte%3A3&filter.q4=contentlocale%3Aeq%3Aen_US&sort.q4=totalpositivefeedbackcount%3Adesc&include.q4=authors%2Creviews%2Cproducts&filter_reviews.q4=contentlocale%3Aeq%3Aen_US"

    params = {
        'limit': 100,
        'offset': 0
    }
    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        data = response.json()
        data = json.loads(response.text)

    limit = 100
    no_batch = math.ceil(data['BatchedResults']['q1']['TotalResults'] / limit)
    print('Total review', data['BatchedResults']['q1']['TotalResults'])

    df = pd.DataFrame()
    for x in range(0, no_batch):
        offset = x * limit
        params = {
            'limit': 100,
            'offset': offset
        }

        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            data = json.loads(response.text)
        else:
            print('status code != 200')

        results_data = data['BatchedResults']['q1']['Results']
        df_temp = pd.DataFrame(results_data)
        df = pd.concat([df, df_temp], axis=0)
        time.sleep(3)

    df['SyndicationSource_Name'] = df.apply(
        lambda row: row['SyndicationSource'].get('Name') if row['IsSyndicated'] else None, axis=1)
    return df


import warnings

warnings.filterwarnings("ignore")

# Tassel Plus - 
skus = ['3A9811772', '3A8383523', '3A9783620','3A6914506','3A3984252']

import math

result_df = pd.DataFrame()
for sku in skus:
    print('Get review', sku)
    data = ojp_review(sku)
    print('Review count', len(data))
    if data is not None:
        result_df = pd.concat([result_df, data], axis=0)

# %%
result = result_df.copy()
result = result[result['ContentLocale'].isin(['en_US', 'en_CA'])]

result['Verified_Purchase_Flag'] = result['BadgesOrder'].apply(lambda x: 'Verified Purchaser' if
isinstance(x, list) and len(x) > 0 and 'verifiedPurchaser' in x else " ")

result['Seeding_Flag'] = result['BadgesOrder'].apply(lambda x: 'Seeding' if
isinstance(x, list) and len(x) > 0 and 'incentivizedReview' in x else " ")

result['HP Model Number'] = result['OriginalProductName'].str.extract(r'(\d+e?)')
result_combine = pd.merge(result, df_amazon, left_on='HP Model Number', right_on="HP Model Number", how="left")

from datetime import date

result_combine['Retailer'] = "Office Depot"
result_combine['Scraping_Date'] = pd.to_datetime(date.today())
result_combine['Comp_Model'] = ''
result_combine.rename(columns={'HP Model': 'Review_Model'}, inplace=True)

# result_combine['Competitor_Flag'] = result_combine['Review_Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
result_combine['Competitor_Flag'] = result_combine['Review_Model'].apply(
    lambda x: (x, 'No') if isinstance(x, str) and 'HP' in x else (x, 'Yes'))
result_combine.rename(columns={'LastModeratedTime': 'Review_Date'}, inplace=True)
result_combine['Review_Date'] = pd.to_datetime(result_combine['Review_Date']).dt.date

column_mapping = {
    'UserNickname': 'Review_Name',
    'Rating': 'Review_Rating',
    'Title': 'Review_Title',
    'ReviewText': 'Review_Content',
    'SyndicationSource_Name': 'Syndicated_Source',
    'TotalPositiveFeedbackCount': 'People_Find_Helpful',
    'Syndicated source': 'Syndicated_Source',
    'HP Class': 'HP_Class'
}

# Rename columns
result_combine = result_combine.rename(columns=column_mapping)
result_combine_odp = result_combine[['Review_Date', 'Review_Rating',
                                     'People_Find_Helpful', 'Review_Content', 'Review_Title', 'Review_Name',
                                     'Syndicated_Source', 'Verified_Purchase_Flag', 'Seeding_Flag',
                                     'Review_Model', 'Comp_Model', 'HP_Class',
                                     'Segment', 'Retailer', 'Scraping_Date', 'Competitor_Flag']]

print('Total Office depot review,', len(result_combine_odp))

Final_review = pd.concat([df_concat_final, result_combine_odp], ignore_index=True)


# %% [markdown]
# # HHO

# %%
# def hho_review(sku):
#     api_url = 'https://api.bazaarvoice.com/data/reviews.json'

#     params = {
#         'resource': 'reviews',
#         'action': 'REVIEWS_N_STATS',
#         'filter': f'productid:eq:{sku}',
#         'include': 'authors,products,comments',
#         'limit': 100,
#         'offset': 0,
#         'sort': 'submissiontime:desc',
#         'passkey': 'caBZoE5X0dmsywGCMoQmo6OPymWLQFnY37VernuC3SGkY',
#         'apiversion': '5.5',
#         'displaycode': '8843-en_us'
#     }

#     response = requests.get(api_url, params=params)

#     if response.status_code == 200:
#         data = json.loads(response.text)

#     limit = 100
#     no_batch = math.ceil(data['TotalResults'] / limit)
#     print('Total review', data['TotalResults'])

#     df = pd.DataFrame()
#     for x in range(0, no_batch):
#         offset = x * limit
#         #         print('Offset',offset)
#         params = {
#             'resource': 'reviews',
#             'action': 'REVIEWS_N_STATS',
#             'filter': f'productid:eq:{sku}',
#             'include': 'authors,products,comments',
#             'limit': 100,
#             'offset': offset,
#             'sort': 'submissiontime:desc',
#             'passkey': 'caBZoE5X0dmsywGCMoQmo6OPymWLQFnY37VernuC3SGkY',
#             'apiversion': '5.5',
#             'displaycode': '8843-en_us'
#         }

#         response = requests.get(api_url, params=params)
#         if response.status_code == 200:
#             data = json.loads(response.text)
#         else:
#             print('status code != 200')

#         results_data = data['Results']
#         df_temp = pd.DataFrame(results_data)
#         df = pd.concat([df, df_temp], axis=0)
#         time.sleep(3)

#     df['SyndicationSource_Name'] = df.apply(
#         lambda row: row['SyndicationSource'].get('Name') if row['IsSyndicated'] else None, axis=1)
#     return df


# import warnings

# warnings.filterwarnings("ignore")

# # Add Tassel Base when it has reviews from the Inspect Review Json
# skus = ['40Q35A',
#         '404M0A',
#         '403X0A',
#         '40Q51A',
#         '588S5A',
#         '588S6A']

# import math

# result_df = pd.DataFrame()
# for sku in skus:
#     print('Get review', sku)
#     data = hho_review(sku)
#     print('Review count', len(data))
#     if data is not None:
#         result_df = pd.concat([result_df, data], axis=0)
#         # print('All Review count',len(result_df))

# # %%
# result = result_df.copy()
# result = result[result['ContentLocale'].isin(['en_US', 'en_CA'])]

# result['Verified_Purchase_Flag'] = result['BadgesOrder'].apply(lambda x: 'Verified Purchaser' if
# isinstance(x, list) and len(x) > 0 and 'verifiedPurchaser' in x else " ")

# result['Seeding_Flag'] = result['BadgesOrder'].apply(lambda x: 'Seeding' if
# isinstance(x, list) and len(x) > 0 and 'incentivizedReview' in x else " ")

# # %%
# result['HP Model Number'] = result['OriginalProductName'].str.extract(r'(\d+e?)')
# result_combine = pd.merge(result, df_amazon, left_on='HP Model Number', right_on="HP Model Number", how="left")

# from datetime import date

# result_combine['Retailer'] = "HP.com"
# result_combine['Scraping_Date'] = pd.to_datetime(date.today())
# result_combine['Comp_Model'] = ''
# result_combine.rename(columns={'HP Model': 'Review_Model'}, inplace=True)
# result_combine['Competitor_Flag'] = result_combine['Review_Model'].apply(lambda x: (x, 'No') if isinstance(x, str) and 'HP' in x else (x, 'Yes'))
# result_combine.rename(columns={'LastModeratedTime': 'Review_Date'}, inplace=True)
# result_combine['Review_Date'] = pd.to_datetime(result_combine['Review_Date']).dt.date
# result_combine

# column_mapping = {
#     'UserNickname': 'Review_Name',
#     'Rating': 'Review_Rating',
#     'Title': 'Review_Title',
#     'ReviewText': 'Review_Content',
#     'SyndicationSource_Name': 'Syndicated_Source',
#     'TotalPositiveFeedbackCount': 'People_Find_Helpful',
#     'Syndicated source': 'Syndicated_Source',
#     'HP Class': 'HP_Class'
# }

# # Rename columns
# result_combine = result_combine.rename(columns=column_mapping)
# result_combine_version = result_combine[['Review_Date', 'Review_Rating',
#                                          'People_Find_Helpful', 'Review_Content', 'Review_Title', 'Review_Name',
#                                          'Syndicated_Source', 'Verified_Purchase_Flag', 'Seeding_Flag',
#                                          'Review_Model', 'Comp_Model', 'HP_Class',
#                                          'Segment', 'Retailer', 'Scraping_Date', 'Competitor_Flag']]

# # result_combine_version.drop_duplicates(inplace = True)

# Final_review = pd.concat([Final_review, result_combine_version], ignore_index=True)

# [markdown]
# HHO(new)
sheet_name_amazon = "data_new"
sheet_name_hho = "HHO"

# Read the Excel sheets into DataFrames
df_amazon = pd.read_excel(excel_file_path, sheet_name=sheet_name_amazon, engine='openpyxl')
df_amazon['HP Model Number'] = df_amazon['HP Model Number'].astype(str).str.strip()
df_amazon['Comp Model number'] = df_amazon['Comp Model number'].fillna(0).round(0).astype(int).astype(str)

df_hho = pd.read_excel(excel_file_path, sheet_name=sheet_name_hho, engine='openpyxl')

def extract_hp_model_number(product_name):
    """Extract HP model number using regex, handle None values."""
    if not product_name:
        return None
    match = re.search(r'(\d+e?)', product_name)
    return match.group(0) if match else None

def hho_review(SKU):
    api_url = 'https://api.bazaarvoice.com/data/reviews.json'
    params = {
        'resource': 'reviews',
        'action': 'REVIEWS_N_STATS',
        'filter': f'productid:eq:{SKU}',
        'include': 'authors,products,comments',
        'limit': 100,
        'offset': 0,
        'Stats': "Reviews",
        'filter_reviews': 'contentlocale:eq:nl*,en*,fr*,de*,it*,pt*,es*,sv*,en_US,en_US',
        'filteredstats': 'reviews',
        'sort': 'submissiontime:desc',
        'passkey': 'caBZoE5X0dmsywGCMoQmo6OPymWLQFnY37VernuC3SGkY',
        'apiversion': '5.5',
        'displaycode': '8843-en_us'
    }

    response = requests.get(api_url, params=params)

    if response.status_code != 200:
        print('Failed to retrieve data')
        return pd.DataFrame()

    data = response.json()
    limit = 100
    total_results = data['TotalResults']
    no_batch = math.ceil(total_results / limit)
    print('Total reviews:', total_results)

    columns = [
        'Review_Model', 'HP_Class', 'Retailer', 'Review_Date', 'Review_Name',
        'Review_Rating', 'Review_Rating_Label', 'Review_Title', 'Review_Content',
        'Seeding_Flag', 'Verified_Purchase_Flag', 'People_Find_Helpful', 'Scraping_Date',
        'Aggregation_Flag', 'URL', 'Segment', 'Competitor_Flag', 'Comp_Model', 'Promotion_Flag',
        'Syndicated_Source', 'Response_Date', 'Response_Text', 'Response_Name', 'Country', 'Review_Source'
    ]

    df = pd.DataFrame(columns=columns)

    for x in range(no_batch):
        params['offset'] = x * limit
        print('Offset:', params['offset'])
        
        response = requests.get(api_url, params=params)
        if response.status_code != 200:
            print(f'Status code {response.status_code} at batch {x}')
            continue

        results_data = response.json()['Results']
        rows = []
        for review in results_data:
            rating = review.get('Rating', None)
            rating_label = '1-2-3-star' if rating and rating < 4 else '4-5-star'

            review_date_str = review.get('SubmissionTime', None)
            if review_date_str:
                try:
                    review_date = datetime.fromisoformat(review_date_str.replace('Z', '+00:00'))
                    review_date_str = review_date.strftime('%Y-%m-%d')
                except ValueError:
                    review_date_str = None

            # Check if SKU is in HHO sheet
            hho_row = df_hho[df_hho['SKU'] == SKU]
            if len(hho_row) > 0:
                hp_model_number = hho_row['Product'].values[0]
            else:
                hp_model_number = extract_hp_model_number(review.get('OriginalProductName', ''))

            if hp_model_number:
                hp_model_number_str = str(hp_model_number).strip()
                hp_model_row = df_amazon[df_amazon['HP Model Number'].astype(str).str.strip() == hp_model_number_str]

                print("Using direct comparison:")
                print(hp_model_row)

                print(hp_model_number)

                review_model = hp_model_row['HP Model'].values[0] if len(hp_model_row) > 0 else None
                segment = hp_model_row['Segment'].values[0] if len(hp_model_row) > 0 else None
                hp_class = hp_model_row['HP Class'].values[0] if len(hp_model_row) > 0 else None
            else:
                review_model = "hp"
                segment = "hp_segment"
                hp_class = "hp_class"
            
            badges = review.get('BadgesOrder', [])
            seeding_flag = 'Seeding' if 'incentivizedReview' in badges else None
            verified_purchase_flag = 'Verified' if 'verifiedPurchaser' in badges else None
            
            if review_model:
                model = review_model.replace(" ", "-")
                URL = f'https://www.hp.com/us-en/shop/pdp/{model}#reviews'
            else:
                URL = "URL"
                
            row = {
                'Review_Model': review_model,
                'HP_Class': hp_class,
                'Retailer': 'HP.com',
                'Review_Date': review_date_str,
                'Review_Name': review.get('UserNickname', None),
                'Review_Rating': rating,
                'Review_Rating_Label': rating_label,
                'Review_Title': review.get('Title', None),
                'Review_Content': review.get('ReviewText', None),
                'Seeding_Flag': seeding_flag,
                'Verified_Purchase_Flag': verified_purchase_flag,
                'People_Find_Helpful': review.get('TotalPositiveFeedbackCount', None),
                'Scraping_Date': datetime.now().strftime('%Y-%m-%d'),
                'Aggregation_Flag': None,
                'URL': URL,
                'Segment': segment,
                'Competitor_Flag': None,
                'Comp_Model': None,
                'Promotion_Flag': None,
                'Syndicated_Source': review['SyndicationSource'].get('Name') if review.get('IsSyndicated', False) else None,
                'Response_Date': None,
                'Response_Text': None,
                'Response_Name': None,
                'Country': 'US',
                'Review_Source': review.get('OriginalProductName', None),
            }
            rows.append(row)

        df_temp = pd.DataFrame(rows)
        df = pd.concat([df, df_temp], ignore_index=True)
        time.sleep(3)  # Sleep to avoid hitting API rate limits

    df['Competitor_Flag'] = df['Review_Model'].apply(lambda x: 'No' if isinstance(x, str) and 'HP' in x else 'Yes')

    return df

# Sample usage
# skus = ['28B49A']
skus = ['40Q35A', '404M0A', '403X0A', '40Q51A', '588S5A', '588S6A',
    '537P6A', '28B49A', '28B70A', '28B98A']

result_df = pd.DataFrame()
for SKU in skus:
    print('Get review', SKU)
    data = hho_review(SKU)
    print('Review count', len(data))
    if data is not None:
        result_df = pd.concat([result_df, data], axis=0)

# Ensure consistent formatting
df_amazon['HP Model Number'] = df_amazon['HP Model Number'].str.strip()
result_df['Review_Model'] = result_df['Review_Model'].str.strip()

# Merge with df_amazon to get Segment and other details
result_combine = pd.merge(result_df, df_amazon[['HP Model', 'Segment']], left_on='Review_Model', right_on='HP Model', how='left')

# Define the desired columns
desired_columns = [
    'Review_Model', 'HP_Class', 'Retailer', 'Review_Date', 'Review_Name',
    'Review_Rating', 'Review_Rating_Label', 'Review_Title', 'Review_Content',
    'Seeding_Flag', 'Verified_Purchase_Flag', 'People_Find_Helpful', 'Scraping_Date',
    'Aggregation_Flag', 'URL', 'Segment', 'Competitor_Flag', 'Comp_Model', 'Promotion_Flag',
    'Syndicated_Source', 'Response_Date', 'Response_Text', 'Response_Name', 'Country', 'Review_Source'
]

# Filter columns to include only desired ones, handle missing columns
result_combine_filtered = result_combine[[col for col in desired_columns if col in result_combine.columns]]
Final_review = pd.concat([Final_review, result_combine_filtered], ignore_index=True)

# # %% [markdown]
# # # Walmart

# import random

# User_Agent = [
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0',
#     'Mozilla/5.0 (Linux; Android 11; SAMSUNG SM-G973U) AppleWebKit/537.36 (KHTML, like Gecko)',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (X11; U; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.5399.183 Safari/537.36',
#     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0'
# ]

# cookie = [
#     'ACID=e743918f-9c01-4185-9889-01b383f39a46; hasACID=true; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwMDI5Mjg0MjMwNCwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzAwMjkyODQyMzA0LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MDAyOTI4NDIzMDMsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJzZWxlY3Rpb25Tb3VyY2UiOm51bGx9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNzAwMjkyODQyMzA0LCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6ZTc0MzkxOGYtOWMwMS00MTg1LTk4ODktMDFiMzgzZjM5YTQ2In0%3D; vtc=TPvkWCN79GksgVjpd8lQp4; btc=TPvkWCN79GksgVjpd8lQp4; bsc=XjMXQ-W4dVqYGkP1JSNWEs; _pxvid=d8404a5f-85e4-11ee-a839-f5e2c99825fc; pxcts=dd47a175-85e4-11ee-b2aa-79f32486f419; _tap_path=/rum.gif; _tap-criteo=1700292852331:1700292852725:1; _tap-Ted=1700292852724:1700292852725:1; _tap-lrV=1700292862974:1700292862975:1; _tap-lrB=1700292878531:1700292878532:1; _tap-appnexus=1700292879419:1700292879718:1; _gcl_au=1.1.258469864.1700294686; bstc=XgTuQhQdHEhnMqLxEbRYgw; mobileweb=0; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=; ak_bmsc=928D3BE7B227834B8EF4992840D6AFA1~000000000000000000000000000000~YAAQT0Dfb2Hq79qLAQAAWF0E7xW+cNY2y9+fVR2/0QrvmtyiqK/i/uEwVaXReQYp0Z0y6FV2m0EQCZomlg6PikSMdPE5Yx+Al074WilmM5mGxgz2kYLAXQy0aPKfmDgo9ohHooJNwq7VcMShtmCmJ1ARCqwfhpgMsbNFrR7LgHL3FRZJhrg9alhKuKLZRfToimKXZ0sKxSm5rLcO6L57KGoWlZX3NmA0ddatFrENafTH0mQVlKWpVPKHtgOZZh4EsGHOO90Z1fy0P2hIJNmpHpoDLk1w+0tBu9Upo+8L5KPS0K1LcuVDuaXEx4+n6o2fMg+HrkC1AF5T7cKLiLDZlbL/C5kSesKN3Rp2RaJjt8YzY2QTkaxrmX9TYd6omcLPWSqHLvjJK0/gyfE=; xptc=assortmentStoreId%2B3081; xpm=3%2B1700522123%2BTPvkWCN79GksgVjpd8lQp4~%2B0; b30msc=XgTuQhQdHEhnMqLxEbRYgw; _tap-li=1700522132702:0:2; _uetsid=0cce940087ff11eea88661d0630688d7; _uetvid=237553a085e911ee9eaec99bfa867851; auth=MTAyOTYyMDE4TzUL0tBWqaWlPLiIWSvGIIqP44I9XeKX8N5bcOOhCkNGFqMJTLhMlajVoxZh6%2F64P%2Bx3aAqHkDo7tLTswFgLqhE9gq7sWtCKMSvMMtAlsDnj17kGZ7Mu4K7gp6blyLzB767wuZloTfhm7Wk2KcjygsAEeU%2BeKCMhfP9XV060SY%2Fspww18DSfg4loIXetO33HWWxCKdp%2B8UHdguRD9DC%2FlTyW2FeTzNUdxbN2aHvb8W0UMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHWs0k33oHkBmchRaU9fj5kF7pZ6JaDMzmiWlGlRQ4nUMw5XFK3QDKKcB%2BPe5gKPHMTJomOWP4NHaOZmjOE06S78R4yUE7XpP0usJVgwSa5Hg5dejwrW41QOfpHzdmIzkekjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9DVVJCU0lERSIsIlBJQ0tVUF9JTlNUT1JFIl0sInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQifV0sInNoaXBwaW5nQWRkcmVzcyI6eyJsYXRpdHVkZSI6MzguNDc0NSwibG9uZ2l0dWRlIjotMTIxLjM0MzgsInBvc3RhbENvZGUiOiI5NTgyOSIsImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSIsImNvdW50cnlDb2RlIjoiVVNBIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJ0aW1lWm9uZSI6IkFtZXJpY2EvTG9zX0FuZ2VsZXMifSwiYXNzb3J0bWVudCI6eyJub2RlSWQiOiIzMDgxIiwiZGlzcGxheU5hbWUiOiJTYWNyYW1lbnRvIFN1cGVyY2VudGVyIiwiaW50ZW50IjoiUElDS1VQIn0sImluc3RvcmUiOmZhbHNlLCJkZWxpdmVyeSI6eyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJhY2Nlc3NQb2ludHMiOlt7ImFjY2Vzc1R5cGUiOiJERUxJVkVSWV9BRERSRVNTIn1dLCJodWJOb2RlSWQiOiIzMDgxIiwiaXNFeHByZXNzRGVsaXZlcnlPbmx5IjpmYWxzZSwic3VwcG9ydGVkQWNjZXNzVHlwZXMiOlsiREVMSVZFUllfQUREUkVTUyJdLCJzZWxlY3Rpb25UeXBlIjoiREVGQVVMVEVEIn0sInJlZnJlc2hBdCI6MTcwMDU0NTYxMjY4MiwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOmU3NDM5MThmLTljMDEtNDE4NS05ODg5LTAxYjM4M2YzOWE0NiJ9; _tap-googdsp=1700524013260:1700524013261:1; bm_mi=6D08B7F70C2629CB7335921BD0A476C4~YAAQVkDfbybwhNCLAQAAiz8h7xVv+k/UWG2WIDe/gPQ8AOZTnjAhiIOZ0bLj8CiGeNePuQWuPNwhDXS1oNp15tCC1PJSEhUXHiX+NSzkvUL7leDWiBNTKpbglVNmWH1NuG/LvNHQA2LBVlOMNtN+pa4w85WJ5WffbcN5bm4oXQMtRoZmDiaP9LejTQc7RtRDzb/UBiKxSj7B7tWFZWvoaMl7Byh7Zw98qmlF62SYLz85rGSvABr10DN471JChbH4Q/G82+KTkYcWus+IMdzcowTy7Z+Q8iX5zSmhRA2ELuD5Ie109BCzItjTb03IbEwxaZaoK0uGP1lkf74XjjcvevsgHZqUduo=~1; _tap-wmt-dw=1700522135009:0:2; _px3=5ea3ebae35f07f1831578749c875dce471cd3c238e9b1d48cc4d8ac7ed6ef5be:/ONT+vAVQYHGOAb5jG51c6bmUcQjbaW1zryLfFf3ZEFrqWRTeciC0Y4hk/ovL1XeinngRvREckraA7RQaqwaeA==:1000:tUHSmj1FtO2NPUlmIukbTE9SnWpD2T21PLjZBMihCAM89elMZOLre7HSRlFqK71v0f8yL9k4n8Sg8C5OWCQ9mrtXRYNZ7J2cHZIgW/Xriz9FwISvvFB2qUm7ermjWRrawUo7rSOqC6UHNjyG4cNmWupEtwDC+dn8hy+VsaMiY64jQxqICji2ZH+yxxSM8rRGtusit4qn5KaOrVpNR2530EK/u3KBOrrKvimdzwbEfh4=; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1700524027817; xptwj=qq:856f56eac45d86299598:ZRnl9M8BUfAYvTYtX+QCNLhEzqw6K+xHmpEjeoxuN7UABA+EM4HcK1WV764ZOO5/6syK6HJNzQ+7oCA1dX0quNAOWtTaQIDcRQupz54KdhbyjvwwBjDPRRo9S45zB814KqqVnZB4xW5GOyMmpEBETfwct40JrJ9yQKgEdWfokI/A+gc=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1700524028000@firstcreate:1700292842259"; xptwg=3735093285:1780D552F836C50:3B5C8D6:477BA478:6AAF8424:7C4EFE87:; TS01a90220=01419f1d62bb3082af49e4e290c9e4f7b7d4d09f7a4d1f20d21c60c4e1a7da9ee45eaa7b433f2f9eae578eda14efecc83c7473e1aa; bm_sv=FBAB233769AD51E9AA3317FB35DBDC6D~YAAQVkDfb73yhNCLAQAA5XMh7xUG5PTSMRUK4v2TvEXPwemnWtVYlvkgiCKbkuxNXqf+vVZB8rZcQemv+ydTIl18pO1qMpban0Uf+/ci+ZkYfinA4Fd+fcZ1uHAyQxRGF/plo2v0Gq9TxZgp4pp/YFFuxSnGTqGRKl4RkFJbdxQSVM0CBeOhtUQWXZ0y9L4TE3TF7jw/A9Nu++2UrVU5zLqdE22Fo4Pdw6Yn6RnKbC9OZ7gJsF2DhCgy6+IkYxmg/SE=~1; _pxde=8e9d44f04024aa1db6ed77fcc7bbf79de1d603492bbaba0843e90b5cd14038cf:eyJ0aW1lc3RhbXAiOjE3MDA1MjQwMjg5MTR9',
#     'ACID=7cff0725-9085-4be0-bf3c-6839f8621f69; hasACID=true; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwMjYzMDM3NDkxNiwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzAyNjMwMzc0OTE2LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MDI2MzAzNzQ5MTUsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJzZWxlY3Rpb25Tb3VyY2UiOm51bGx9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNzAyNjMwMzc0OTE2LCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6N2NmZjA3MjUtOTA4NS00YmUwLWJmM2MtNjgzOWY4NjIxZjY5In0%3D; vtc=c_lPqL_NYDC6UXPeh9MF7U; _pxvid=5644c6ff-9b27-11ee-ac94-b6ae3dc3013d; pxcts=578a7ded-9b27-11ee-b1db-83fbf11618da; thx_guid=f069aa2ed76a293b146f2f45273ffc5e; QuantumMetricUserID=86dbb1aaa4c0846b6472227c8127c115; bm_mi=A9B761149346E34C2336993088CBA838~YAAQpCLHF0tB41iMAQAAT6lrexYSWryWW33ix0ebW0gEwR9R9Q3T0mju/pC7V7LqjuwHJrGseRkgFSZZiZVHV/zjTu4nCYLngf6698jYoiODJd20Ah32C8vCA7fNXyHUhtfSgpdKNkkOpAYXO8oYiiVKF1l5IHZeILy9fj41t1W9tF9DODYuxTNU8QPPnZr4w70z23lcK+3EBKd8IfqBgPT9kjLT3pKPY5P1EjKBk7Wvn9d44E3YtdQOdIlnmZMFqcniDHXEd0TgXXrvQ7NVo1BkEMaGynnbyeApl64EsIYaBB5nCsrN2NFOY0C1ZumHLPsxg5pg~1; ak_bmsc=28DD2AB37291D5EE54D348A14FFB7027~000000000000000000000000000000~YAAQpCLHF1FB41iMAQAA3atrexY3RQO5uSrgAn4o71OTeCY3Kb4oklPOhzO4ob+ZppkPcKbMuYwUEtBs6vEVlp9CoeIcig2wNR+YFPAMLag0FgMkBgPTNS7DAEU7mXMzqG7Zxes7pw+1iCYIJKVm0KAso3OE50SIiVGnpFKhhL7E2/cDwpWbUdrNVY30TGqqMk/uCOYcvkqNQE0wzVpTYkK2hWIeztHPT5fNqkdtRUylfN9FBTzfaUxix+emgkOw4S/zZxgkw4lNSo/YbiRZDpT409NuUxV4ht5j47adbQ1Q8F08pAvMkK3vGzOypF53bdu9Iw3g3D4PUZDOdQ2FFArxdUalCDwieOVrUbY5Xu55+30+fnRIFgzDM+aa72JMrHteO5326b1oz/QcUX1+EgWPmeSXJTk6/0EJvYS6yU9fnzsad5XVkCe4cRcS0AP/ZVfbCDorHjsVEachEwRjpKmMWx8q6DOuGFw00I1vRusHKdW1nrCknd+TjPY2u042+mVSAQCo0JoYGB8VZlNhvUof6KzcVkfB4ZG5; auth=MTAyOTYyMDE45uQqkShnPmrFyDklTStmr7wc0IXA0uhqKhIjidzMeOlor0FoZrStjvQEbiRm5ZqSmQEJ%2FPunCsfySE6LPKFrLFOCiFQAqcgu3n0iVe3t%2BsYE2L20v13oIDsvWtwF1M8S767wuZloTfhm7Wk2Kcjygi5k0VvBM%2FJjwcKWWhCnBS%2FsNVBmy9J1bR2VHO%2FdV8LIpQmp8XOq309QoW%2BZviaSOv8sqUVU54sFd4Bd6dus2ZEUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHVeQqO76rPmdncwFjpe%2BDY%2BOi7voHCqN3McdaIPwoybp22ykAEJaZ9rj2LqLTXsr9v3%2FEocn3Z%2BtLU09JmG1TrtCvNQVjLp%2Ffv%2BF%2BvOqp4HL4oel5ASvElr1Cex8QK9UZEjyrOXbKKhH072NS%2FW0j%2FU%3D; bstc=VDdH8jJ4m0c1jwDTa0PzoE; mobileweb=0; xptc=assortmentStoreId%2B3081; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=1mX_Y|7C2Eg|ERspM|HhvdQ|IS-p_|LPh6f|XbHrX|csP8O|gUDh7|oY0CV|qKfBf|yamTG; exp-ck=IS-p_1LPh6f1XbHrX1gUDh71qKfBf1; xpm=1%2B1702877712%2Bc_lPqL_NYDC6UXPeh9MF7U~%2B0; QuantumMetricSessionID=fb9f81cd396976e7c5f3318d82cf9aca; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1702878308583; xptwj=qq:1076adbafd040b695fce:7QGLRUcehov3qVy4qSYDvW4xgk6Uac2yX7HX1tE4u646HYe83hDIeMje3Thc/1gvh8z8MrbJNvYdlJL4OmUTikr/fII5bRfC3yxM6l2D8uZIkuA2LrEs6TpjeeKaJ8Uu78B/EOBkj1BzXZ7N6125enlMLSzG; xptwg=3146300320:19444130883AE60:3FBA1BE:1196AC61:D2F58FD2:22F02742:; _px3=6c9349297a823e7545b893fcfa8e6bea6eecb57dc1b11b7cf64522744342fc05:q5wbXoTzZgQ7QP9YgqzSywMmiYqoqw/ZrncImDY3CrTPeEkOqVzPo2zJ3AHIc+s/Ltf7IlS8awVgjpNXM98pxQ==:1000:cn/ohrnROd+TGJA88Xeo4/KYIo/UsLvu6vSoOqMnmEug9QezGcx/WE0rfSuHGKZjjjaYrqXq8pBrJ5nlgEeADLsTsoKQiFm1rc/m/6Q+wMMNleiOZAl9VZy3wpYUymfUCrFwGU+qqgFlXPCfQNN9o6vjljQksDltmcFQIof95UjvNs7UMihymHVA7zlP5BOXCURkIHiiJbM73dOLjmbFMFnxGHKXULDB8u2cb1oECR0=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1702878315000@firstcreate:1702630374882"; TS01a90220=014e9abc5b76ea37289d24aa7bf6872327f3a801259b614de94e2e0e90c1b0f94c88be0edeb3c58cf3cba5b3568b2b79f66cc4dc9c; bm_sv=ADF605AFD1AA1E340A3C57CE2E012D0E~YAAQJPN0aPSze2mMAQAAbQJ1exaqQAmPQEugdiG3MQAUrup+aS98eOHJhIKWYEY4cQ16kvxr5PjD68+TG/q08uWKgkntuhl4BsnOGqcEXY7GjWBoB0neW+58ysLmfhee1rCKsVjS3iaBCoFZwcqIgkO3eDhiRFyKZXfNTkKDX7JBTyy63S1DAg2SrohZrD0KlpE5E2rFPlHxbpWohjTWhvXGlDIHA0lk8uQB+NvqN5R8yDL6bMxSm0YrJ6/p6h5kxvQ=~1; _pxde=3cb689d88ebd75dfd5e5c64a8389623aecf6bac16e69f216cf01a4e825c18d52:eyJ0aW1lc3RhbXAiOjE3MDI4NzgzMTUzNDl9',
#     'ACID=6ff15283-acba-4fe2-89c5-ac59a9b887d1; hasACID=true; thx_guid=0c376c1283b8b14be6de36c7c9897b80; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1696941267921; _m=9; vtc=WYxqCClwKT085jYR_DbfQI; _pxvid=5a44b304-6769-11ee-a5a3-89fdf7f6e6ba; auth=MTAyOTYyMDE4IIJ07VmsLMduZmu3wWCW8eIqM67RZKVCKQnP%2BJhyprb8mcLFoQ86xyheUk7V1wBI53SlNalpIeiZEAnwibnwBxAuKnHyan446S83cruRp5IqJEEhjLzfZ9dTkdF%2F5mvs767wuZloTfhm7Wk2Kcjygt6CFmh5hT8BoAhiLFQG8TM4tK7YyL%2Bjr93Ekvm3gtoWXd1A1TJkrpfzbS%2B%2BXZ2ssMBCHcdxxw3SP0Sy3y18bhsUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHfClvOkjwxW8L0euuWDrN9AOTwJZ5k6XdH2IzYdedb%2BeKcBnww%2BqCeKjSX3bV3tRPjMVnRfkQSZ38Y3kHRhf5YWMEw3bsV%2BTWUdfiZ61nY1rm2KT4Gr0iVCCeIJhV8GhuUjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9JTlNUT1JFIiwiUElDS1VQX0NVUkJTSURFIl0sInNlbGVjdGlvblR5cGUiOiJMU19TRUxFQ1RFRCJ9XSwic2hpcHBpbmdBZGRyZXNzIjp7ImxhdGl0dWRlIjozOC40NzQ1LCJsb25naXR1ZGUiOi0xMjEuMzQzOCwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeUNvZGUiOiJVU0EiLCJnaWZ0QWRkcmVzcyI6ZmFsc2UsInRpbWVab25lIjoiQW1lcmljYS9Mb3NfQW5nZWxlcyJ9LCJhc3NvcnRtZW50Ijp7Im5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJpbnRlbnQiOiJQSUNLVVAifSwiaW5zdG9yZSI6ZmFsc2UsImRlbGl2ZXJ5Ijp7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsIm5vZGVUeXBlIjoiU1RPUkUiLCJhZGRyZXNzIjp7InBvc3RhbENvZGUiOiI5NTgyOSIsImFkZHJlc3NMaW5lMSI6Ijg5MTUgR2VyYmVyIFJvYWQiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5IjoiVVMiLCJwb3N0YWxDb2RlOSI6Ijk1ODI5LTAwMDAifSwiZ2VvUG9pbnQiOnsibGF0aXR1ZGUiOjM4LjQ4MjY3NywibG9uZ2l0dWRlIjotMTIxLjM2OTAyNn0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImFjY2Vzc1BvaW50cyI6W3siYWNjZXNzVHlwZSI6IkRFTElWRVJZX0FERFJFU1MifV0sImh1Yk5vZGVJZCI6IjMwODEiLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJzdXBwb3J0ZWRBY2Nlc3NUeXBlcyI6WyJERUxJVkVSWV9BRERSRVNTIl0sInNlbGVjdGlvblR5cGUiOiJMU19TRUxFQ1RFRCJ9LCJyZWZyZXNoQXQiOjE2OTg0MDA0ODkwNzgsInZhbGlkYXRlS2V5IjoicHJvZDp2Mjo2ZmYxNTI4My1hY2JhLTRmZTItODljNS1hYzU5YTliODg3ZDEifQ%3D%3D; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIn0sInNoaXBwaW5nQWRkcmVzcyI6eyJ0aW1lc3RhbXAiOjE2ODU3ODc2OTkwNzksInR5cGUiOiJwYXJ0aWFsLWxvY2F0aW9uIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJkZWxpdmVyeVN0b3JlTGlzdCI6W3sibm9kZUlkIjoiMzA4MSIsInR5cGUiOiJERUxJVkVSWSIsInRpbWVzdGFtcCI6MTY5ODM5Njg4OTA3Miwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIiwic2VsZWN0aW9uU291cmNlIjpudWxsfV19LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwiYmFzZSI6Ijk1ODI5In0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOjZmZjE1MjgzLWFjYmEtNGZlMi04OWM1LWFjNTlhOWI4ODdkMSJ9; bstc=RDjQg_AX5lxPJuFgq3Nu74; mobileweb=0; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=92YHy|YnYws|yUqGy; exp-ck=YnYws4; pxcts=7ca9260b-74a6-11ee-b9ba-cadc77ff3193; xptc=assortmentStoreId%2B3081; xpm=1%2B1698396890%2BWYxqCClwKT085jYR_DbfQI~%2B0; TS01a90220=016ea84bd28377d28f6c5f8c825a73acff43907723c0c62c3b0f4412526365096a8d6e578fadcc7b7c5313bbb50b7e1d6a09790715; xptwj=qq:8c955083c3f2d971e73b:/AUbzj+G4qHiFzoBmTqw7sTxNCjaAsdlFcEBElDEMhfux6fBocZ2XuU9J43MZqv9xFdQR6jCZn1NHMlGARop+dGCrSwqG/FMfkjiqrzMhk6qDRXnGwdXkmVMkY17cQBnEuy2yF31oDbLB2VcFQm/6GzBoQa2xx4u4jyJ; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1698397083000@firstcreate:1696941267921"; xptwg=2654488666:13D200536871A40:321F50E:2AE8C8EB:145FE951:E3F58838:; _px3=2259e2ff82c009ea11f3276f67eeec8c7933f2015f7c4f4f537a810e135667b2:WTyoBm43yjHnl6DaKRhXrBuw1dVxFeMmNao4s1nyzyzay9AT/JNRr2njauNA3Q05CZCkyHcGpiOlACvkOPaeLA==:1000:eqWykI7N/3Nxz6qb3xQH+stpzInFVztcX104+VKDHUoglkWCA2mLjEu+Zknx3FqHY6MZskTdWOhU7b/cxkcRsZzn2v7xd2d0SIFOVaBuJFFm4ddlo4ejUXkO/Ta7SH2GcvS0zq5pIVgCSlg9SjcmMala24rEipeabfBpgSjY1DFP4u3vQ5vVD9nlh6dHTGDAN6J86YkunMWOWKq/mltB5LLWDt5U+hsHxwlUQFOl53E=; _pxde=67376920231ea198f43cb2a2fd4e3570e6f0c4527d7e9406b6848b481178e72e:eyJ0aW1lc3RhbXAiOjE2OTgzOTcwODU3NDh9',
#     'ACID=6ff15283-acba-4fe2-89c5-ac59a9b887d1; hasACID=true; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1696941267921; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIn0sInNoaXBwaW5nQWRkcmVzcyI6eyJ0aW1lc3RhbXAiOjE2ODU3ODc2OTkwNzksInR5cGUiOiJwYXJ0aWFsLWxvY2F0aW9uIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJkZWxpdmVyeVN0b3JlTGlzdCI6W3sibm9kZUlkIjoiMzA4MSIsInR5cGUiOiJERUxJVkVSWSIsInRpbWVzdGFtcCI6MTY5Njk0MTI2ODAwMSwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIiwic2VsZWN0aW9uU291cmNlIjpudWxsfV19LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTY4NTc4NzY5OTA3OSwiYmFzZSI6Ijk1ODI5In0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOjZmZjE1MjgzLWFjYmEtNGZlMi04OWM1LWFjNTlhOWI4ODdkMSJ9; userAppVersion=us-web-1.102.0-0f3d752097f13fd03499487f7cfc0f9ff879d809-1005; abqme=true; vtc=WYxqCClwKT085jYR_DbfQI; _pxhd=5a7ffd639284c9b62b5b6953d2b6554b5e4fb23e72bdea13cb0d60c5e9cb2592:5a44b304-6769-11ee-a5a3-89fdf7f6e6ba; TBV=7; _pxvid=5a44b304-6769-11ee-a5a3-89fdf7f6e6ba; pxcts=5b1312c1-6769-11ee-9d4b-928400606778; xptwj=qq:19ae55e85ed74ecb934a:FzwixoJNbjsJTKIVOxs2Y3BCAjYnbpEJ9QAEPF+vcgu7rou9eHViyjDPVj+jQqEQsDVe8eLUcM9yr4bzIXF5/EpE+3GBy+nQfjIux03VKMmH4uP0zvUVBAnki5gXoud346PderEXI4ZdwzI5dEw9RZpxrSE=; _astc=dd455cd93be2a8805fa78a0c5637c0bc; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1696943665000@firstcreate:1696941267921"; xptwg=3827596973:8C0849C3A8C7E8:1626C14:C8D91831:9FD2BC9F:ED33D50A:; TS012768cf=0178545c900bf5c440f69b21bbdea7b97f1bb93829c83cdf12d8a829eaa1f335330ed161b9a95404539366655b7d1af2acfeaa823d; TS01a90220=0178545c900bf5c440f69b21bbdea7b97f1bb93829c83cdf12d8a829eaa1f335330ed161b9a95404539366655b7d1af2acfeaa823d; TS2a5e0c5c027=0881c5dd0aab20006278b8f1c282bae24adaf3370d4de18765bc5b96e8045bf269a5218310a2b5f308691a3aad113000c4a62f799fcd6ee383055c96bd53eb55adecf63f1102cc5dd537a8a8e81ee1756147c51df92a6c584fb9a07c447c0309',
#     'ACID=13f858d3-9165-43cb-bab4-a63c55e6a6a8; hasACID=true; _m=9; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwMjk1NTMwNTIxNSwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzAyOTU1MzA1MjE1LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MDI5NTUzMDUyMTQsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJzZWxlY3Rpb25Tb3VyY2UiOm51bGx9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNzAyOTU1MzA1MjE1LCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6MTNmODU4ZDMtOTE2NS00M2NiLWJhYjQtYTYzYzU1ZTZhNmE4In0%3D; vtc=bqXm-Yjh0fiyfC30O9pl40; pxcts=e0a91fc6-9e1b-11ee-b6a5-16e7f3c0c35f; _pxvid=dfcedab4-9e1b-11ee-852d-cc6ba9195b8b; thx_guid=d45cea97b66ab1e12333b05cf300756a; auth=MTAyOTYyMDE4eh0UBw4CZsMoITCUpLy%2FJSKZAhO1G27GRMvlySSJu45p1%2FLh4CYkzkJMDMayVQPkr4cn%2Flzfu3Cnm2UosjL24mxfJqc48PMpEXdaud0aTiSYybEajj235v6w6v39wTDg767wuZloTfhm7Wk2KcjygobRHThsmZk%2BGcqTfIab85Qi91RLvjJ4oWxX7pdsgCM7kaNdhw7fWS2J7XYV98BtLp94fDX6wtiILXdT4QaPibQUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHYAmxWm2QCSM81oB%2BzgtGh7GRphnRVqhmKz4T4aeRpfPdIrk6V7SOwO2Q2sHD6RhS27h8BprVsmSYkJBi2ZANdkS%2BmkqvUibwJ%2ByNBdR4lDAXTGRW5wSkfxBkI28si3Kp0jyrOXbKKhH072NS%2FW0j%2FU%3D; bstc=eiIdhbpg62SIN7bXqMg3z8; mobileweb=0; xptc=assortmentStoreId%2B3081; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=1mX_Y|M65NP|WuHSe|XmyU7|j21L4|oY0CV|qKfBf|qMQpD|r97uO; exp-ck=WuHSe1XmyU71j21L41qKfBf1r97uO1; xpm=1%2B1702962355%2BbqXm-Yjh0fiyfC30O9pl40~%2B0; bm_mi=549FCFD96C94678551FEF51A3881B212~YAAQX/N0aH0UHFmMAQAAD7F9gBYxuYoIp+e5g1nqvEyexI6hSo7fx80xYZvYxOzX42gMoKUiODVyjrfCTZxzUPZoY9b4p074yFBo9qtr3dvqcrWK+Ax3QnilXHYN/O3i9uGgMgaYzDbvFsofcB+UXepCydTAGXEwlgTjcuhCK/WHSSkwgTYZBCgoH4ZvNuWxbDqSZJVHpneNw6xdnWnxgDwCiwgTl9vrLyjdg09e4T2smzNssFE245sbVMdbiBsLplNv/n7syfFu/VVXuZyCtMvNaORC85FiXoDfVzz0LARwtlqQLr1WRjGOEklBkKaf65Mxcoy8~1; ak_bmsc=2EB05928C59E3D09719A985EA17D031E~000000000000000000000000000000~YAAQX/N0aIcUHFmMAQAAg7N9gBbfI4TKoREfgSIqsmk7VQxNHd1hZRptwqQOz86rLpb4Dm8hwXmGCtpOkNohVRQHY/oG6ctA+y0cssVoASKpTSd1fEYyHDqbC+DrYiN6LJ0q930S+F4z3j6ktqalEJm6Bh4BtsGU6d5h43XeXgGRDzTLgZf+D5taWBpWzCUAb/xXmehSq77svJniJ4HmGoQYRn4fH8UeuB958xg08dR7TF3qVNpEUpqlv1WV4x9fcXDK35s9YBcHcZJUpGXMIx0+3rA/JVyuVqSLJnTzu9yOhVYgISi07np3590KzU5WW1U9oTYdQVYyvkMD3EL1LbLzXiZ10P9fl+HnFHX0V52zIFkboWpvSkdK4HVlAR/7LrWEM6PaexEzR6dCESt+Az+opps+z9JZNT6Y9W2aalGBPdzj275+3AznMTCf3hNispVbb4Im9fKO4HjsyY/rQKVgXwkJ6+IVYPDvYRFJaS1jY42zuoQknNidJ0ajbhHEwI5WkMGbsQ6ZfxAYYFUHPOj4ZWUDO1N9gnSx; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1702963573470; xptwj=qq:60c655ed12ee68048e72:54KBHIYpe8ioxt3yuo3GuRpC1WCSieQoP/HSZ4zw05vUNBqTEGjZylRV7Ee/gQKdw+4AR39Pu3s9w8UN6fC+xQaysEu/16eRafxBSlOhJPWWTVuI1BF3b300k0De/jcPtI82jkh9uGX2O4ufX9jSWMwn87b+BBU=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1702963574000@firstcreate:1702955305178"; xptwg=4227679326:558030DE400990:D7A337:D1486F6E:AEABFA5B:A48C033E:; TS01a90220=0178545c90c924ff084263c8cb94d8a93feb10ba5dcdaafa44d2d4bbee53181a33b55ab3c28c48628f7c56388b13afbe855a015aeb; bm_sv=60E1D169192E935CC6C00B0CDE127AEB~YAAQniLHF0lo0UKMAQAAEPaJgBa2H14Xw8z9j5FuLe+c+mOr9vtjWy+B1JIWMPOzFV3jf2sOwlAcIEVT7Zb0mGwx9GetlyN6e/PNbuNPGRK0tdoA/hf8gjCduTwLS8CC6z4no3eEnvY1ag+q3Dq0y4dikxDbYXvcfVujjYrVNac/v5cOfWcW40fMWhkJQX7hGxnZqmIyc+52KM8NeP9eIqEdybm44398ucnnT/mCybgp/Bn9xYLZwQBJSE7edA60x3A=~1; _px3=6e98f47b3f022f412f75d2fff334dd78b42e1c2da9dbb1ac36bcae228b1359cb:Vj9ElCDReT1PHAkl34AbO/7bhYH1iIhL+fexBxQwF/yJALJP177TUZi0QsuawJtQWCRpPtcCQy7FPqF2SO+Gpw==:1000:x43ZNK+0w/plDdsG3NfS1bezpRm0D1++lXrQuSJabg6/xCZEc7Ghp02iHShQZQWtATaAUPAyVYeZC2R8lPF2X7xPgH9OSstdEZ2g17OOrJ4Fv7E1MK6KZq0DiVn7FxyrcRAXcBHekyDMGiK6TTAugoF3QVB4LAQ7WM6kpnEbXlQLvndZW/IDW1JNGgVGCJvdRpV+VpqD6Ab7P2yv3UX4n+cRwoKWIa99iFjoZbmOuYM=; _pxde=3810374111437fdd342c82c0dbe956b1fb4871c6ade129e930a96584572ba38e:eyJ0aW1lc3RhbXAiOjE3MDI5NjM1NzUwNDJ9'
# ]


# def get_page_number(url, cookie):
#     User = random.choice(User_Agent)
#     header = {
#         'User-Agent': User,
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#         'Accept-Encoding': 'gzip, deflate, br',
#         'Accept-Language': 'en-US,en;q=0.9',
#         'Cache-Control': 'max-age=0',
#         'Cookie': random.choice(cookie),  # Replace with the actual Cookie
#         'Downlink': '10',
#         'Dpr': '1',
#         'Referer': url,
#         'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
#         'Sec-Ch-Ua-Mobile': '?0',
#         'Sec-Ch-Ua-Platform': '"Windows"',
#         'Sec-Fetch-Dest': 'document',
#         'Sec-Fetch-Mode': 'navigate',
#         'Sec-Fetch-Site': 'same-origin',
#         'Sec-Fetch-User': '?1',
#         'Upgrade-Insecure-Requests': '1'
#     }
#     try:
#         user_agents = [
#             # Chrome on Windows
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Firefox on Windows
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
#             # Safari on Mac
#             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Edge on Windows
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.48',
#             # Opera on Windows
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.277',
#             # Chrome on Linux
#             'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Firefox on Linux
#             'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
#             # Chrome on Android
#             'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
#             # Safari on iOS
#             'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
#             # Firefox on Android
#             'Mozilla/5.0 (Android 10; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0',
#             # Opera on Android
#             'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36 OPR/64.2.3282.60115',
#             # Edge on Android
#             'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36 EdgA/46.6.4.5151',
#             # Samsung Browser on Android
#             'Mozilla/5.0 (Linux; Android 10; SAMSUNG SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/15.0 Chrome/91.0.4472.120 Mobile Safari/537.36',
#             # UC Browser on Android
#             'Mozilla/5.0 (Linux; U; Android 10; en-US; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.120 UCBrowser/13.3.8.1305 Mobile Safari/537.36',
#             # Opera Mini on Android
#             'Opera/9.80 (Android; Opera Mini/58.0.2254/172.56; U; en) Presto/2.12.423 Version/12.16',
#             # BlackBerry Browser
#             'Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, Like Gecko) Version/6.0.0.337 Mobile Safari/534.1+',
#             # Internet Explorer 11 on Windows
#             'Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
#             # Edge Legacy on Windows
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/44.18362.449.0',
#             # Safari on macOS
#             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
#             # Internet Explorer 10 on Windows
#             'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)',
#             # Safari on iPad
#             'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
#             # Microsoft Edge (Chromium-based) on Windows
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.48',
#             # Silk Browser on Fire OS
#             'Mozilla/5.0 (Linux; U; Android 4.1.2; en-us; SCH-I535 4G Build/JZO54K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 Silk/2.2',
#             # PlayStation 4 Browser
#             'Mozilla/5.0 (PlayStation 4 7.02) AppleWebKit/605.1.15 (KHTML, like Gecko)',
#             # Opera on macOS
#             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.277',
#             # Brave Browser on macOS
#             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Brave/91.1.26.67',
#             # Chrome on iOS
#             'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/91.0.4472.80 Mobile/15E148 Safari/604.1',
#             # Firefox on iOS
#             'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/34.0 Mobile/15E148 Safari/605.1.15',
#             # Chrome on macOS
#             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Chrome on Chrome OS
#             'Mozilla/5.0 (X11; CrOS x86_64 14150.64.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
#             # Chrome on BlackBerry
#             'Mozilla/5.0 (BB10; Touch) AppleWebKit/537.35+ (KHTML, like Gecko) Version/10.3.3.2205 Mobile Safari/537.35+',
#             # Chrome on Windows Phone
#             'Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36 Edge/40.15254.603',
#             # Firefox on Windows Phone
#             'Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950 XL) Gecko/20100101 Firefox/88.0',
#             # Samsung Internet on Android
#             'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/14.2 Chrome/91.0.4472.120 Mobile Safari/537.36',
#             # Chrome on KaiOS
#             'Mozilla/5.0 (Mobile; LYF/F30C/000JGJ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36',
#             # Safari on tvOS
#             'Mozilla/5.0 (Apple TV; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/604.1',
#             # Silk Browser on Kindle Fire
#             'Mozilla/5.0 (Linux; U; Android 4.0.4; en-us; KFTT Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30 Silk/3.17',
#             # Chrome on Oculus Browser
#             'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 OculusBrowser/14.7.0 Mobile VR Safari/537.36',
#             # Firefox on Oculus Browser
#             'Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0 OculusBrowser/14.7.0',
#             # Chrome on HoloLens
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 HoloLens/2.0.210325.1003 Safari/537.36',
#             # Safari on WatchOS
#             'Mozilla/5.0 (Apple Watch; CPU iPhone OS 8_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12D508 Safari/600.1.4',
#             # Firefox on Linux x86_64
#             'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
#             # Chrome on Linux x86_64
#             'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Chrome on macOS x86_64
#             'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Chrome on Windows x86_64
#             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             # Safari on iOS x86_64
#             'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
#             # Firefox on iOS x86_64
#             'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/34.0 Mobile/15E148 Safari/605.1.15',
#         ]

#         headers = {
#             'Accept-Language': 'en-US,en;q=0.5',
#             'Accept-Encoding': 'gzip, deflate, br',
#             'Referer': 'https://www.walmart.com/',  # Referer header might be required for some websites
#             'Connection': 'keep-alive',
#             'Cache-Control': 'max-age=0',
#         }

#         # Choose a random user agent
#         headers['User-Agent'] = random.choice(user_agents)

#         response = requests.get(url, headers=headers)
#         soup = BeautifulSoup(response.text, 'html.parser')
#         page_number_elements = soup.find_all(
#             lambda tag: tag.name == 'a' and 'page-number' in tag.get('data-automation-id', ''))
#         print("response recorded")
#         page_numbers = [int(element.text) for element in page_number_elements]

#         if page_numbers:
#             last_page_number = max(page_numbers)
#             return last_page_number
#         else:
#             print("No page numbers found. Assuming only one page.")
#             # return 1
#             return max(page_numbers)

#     except requests.exceptions.RequestException as e:
#         print(f"Request error encountered: {e}. Retrying in 20 seconds...")
#         time.sleep(20)
#     except Exception as e:
#         print(f"Error encountered: {e}. Retrying in 20 seconds...")
#         time.sleep(20)

#     return None


# def get_review_walmart(url, cookie):
#     extracted_reviews = []
#     retry_count = 0
#     header = {
#         'User-Agent': random.choice(User_Agent),
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#         'Accept-Encoding': 'gzip, deflate, br',
#         'Accept-Language': 'en-US,en;q=0.9',
#         'Cache-Control': 'max-age=0',
#         'Cookie': random.choice(cookie),
#         'Downlink': '10',
#         'Dpr': '1',
#         'Referer': url,
#         'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
#         'Sec-Ch-Ua-Mobile': '?0',
#         'Sec-Ch-Ua-Platform': '"Windows"',
#         'Sec-Fetch-Dest': 'document',
#         'Sec-Fetch-Mode': 'navigate',
#         'Sec-Fetch-Site': 'same-origin',
#         'Sec-Fetch-User': '?1',
#         'Upgrade-Insecure-Requests': '1'
#     }

#     sheets = "api"
#     api = pd.read_excel(excel_file_path, sheet_name=sheets)
#     api_key = api['API'][0]
#     api_key

#     try:
#         url = f"https://api.scrapingdog.com/scrape?dynamic=true&api_key={api_key}&url={url}"
#         # url = f"https://api.scrapingdog.com/scrape?api_key={api_key}&url={link}"
#         print(url)
#         response = requests.get(url, headers=header)
#         response.raise_for_status()
#         soup = BeautifulSoup(response.text, 'html.parser')
#         li_elements = soup.find_all('li', class_='dib w-100 mb3')
#         title_all = soup.find('a', class_='w_x7ug f6 dark-gray')
#         if title_all:
#             title = title_all.get('href')
#             pattern = r'(\d{4}[a-zA-Z]?)-'
#             model = re.findall(pattern, title)
#         #         model = re.search(r'\b(\d{4}e)\b', title_all).group(1)
#         li_elements = soup.find_all('li', class_='dib w-100 mb3')

#         if li_elements:
#             for li_tag in li_elements:
#                 product = {}
#                 product['Model'] = title
#                 product['Review rating'] = li_tag.select_one('.w_iUH7').text
#                 product['Verified Purchase or not'] = li_tag.select_one(
#                     '.pl2.green.b.f7.self-center').text if li_tag.select_one('.pl2.green.b.f7.self-center') else None
#                 product['Review date'] = li_tag.select_one('.f7.gray').text if li_tag.select_one('.f7.gray') else None

#                 review_title_element = li_tag.select_one('h3.b')
#                 product['Review title'] = review_title_element.text if review_title_element else None

#                 product['Review Content'] = li_tag.find('span', class_='tl-m mb3 db-m').text if li_tag.find('span',
#                                                                                                             class_='tl-m mb3 db-m') else None
#                 product['Review name'] = li_tag.select_one('.f6.gray').text if li_tag.select_one('.f6.gray') else None

#                 syndication_element = li_tag.select_one('.b.ph1.dark.gray')
#                 product['Syndicated source'] = syndication_element.text if syndication_element else None
#                 product['URL'] = url

#                 extracted_reviews.append(product)
#         elif "Robot or human" in response.text:
#             print()


#     except requests.exceptions.RequestException as e:
#         print(f"Request error encountered: {e}. Retrying in 5 seconds...")
#         time.sleep(5)
#     except Exception as e:
#         print(f"Error encountered: {e}. Retrying in 5 seconds...")
#         time.sleep(5)

#     return extracted_reviews


# # %%
# urls = [
#     'https://www.walmart.com/reviews/product/5195244518',
#     'https://www.walmart.com/reviews/product/5226743165',
#     'https://www.walmart.com/reviews/product/5193278547',
#     'https://www.walmart.com/reviews/product/5129928602',
#     'https://www.walmart.com/reviews/product/5193278546',
#     'https://www.walmart.com/reviews/product/5129928603'
# ]

# # %%
# import time

# walmart_reviews = []

# for link in urls:
#     # initial value don't modify
#     retry_count = 0

#     # you can modify with your need
#     max_try = 5
#     retry_limit = max_try
#     print(link)
#     while retry_count < max_try:
#         try:
#             last_page_number = get_page_number(link, cookie)
#             if last_page_number is None:
#                 retry_count += 1
#                 if retry_count <= retry_limit:
#                     print("Failed to retrieve last page number. Retrying... Also Extract the data")
#                     if retry_count == 1:
#                         for page_number in range(1, last_page_number + 1):
#                             retry_count = 0  # Reset retry count for each page
#                             while retry_count < max_try:
#                                 try:
#                                     target_url = f'{link}?page={page_number}'
#                                     extracted_reviews = get_review_walmart(target_url, cookie)

#                                     if len(extracted_reviews) == 0:
#                                         print('No reviews found. Retrying in 5 seconds...')
#                                         retry_count += 1
#                                         time.sleep(5)
#                                     else:
#                                         walmart_reviews.extend(extracted_reviews)
#                                         print(f'Review count in page {page_number}:', len(extracted_reviews))
#                                         time.sleep(2)
#                                         break

#                                 except Exception as e:
#                                     print(f"Error encountered: {e}. Retrying in 3 seconds...")
#                                     retry_count += 1
#                                     time.sleep(3)
#                             else:
#                                 print(f"Max retries exceeded for page {page_number}. Skipping to the next page.")

#                     time.sleep(3)
#                     continue
#                 else:
#                     print("Failed to retrieve last page number after multiple retries. Changing the link.")
#                     # Change the link here
#                     link = new_link
#                     retry_count = 0  # Reset retry count
#                     continue
#             print('Total pages:', last_page_number)
#             break
#         except Exception as e:
#             print(f"Error encountered: {e}. Retrying in 3 seconds...")
#             time.sleep(3)
#         retry_count += 1
#     else:
#         print("Max retries exceeded for this link. Moving to the next link.")
#         continue  # Move to the next link if max retries exceeded

#     if last_page_number is None:
#         print("Skipping processing for this link due to inability to retrieve last page number.")
#         continue  # Move to the next link if last_page_number is None

#     for page_number in range(1, last_page_number + 1):
#         retry_count = 0  # Reset retry count for each page
#         while retry_count < max_try:
#             try:
#                 target_url = f'{link}?page={page_number}'
#                 extracted_reviews = get_review_walmart(target_url, cookie)

#                 if len(extracted_reviews) == 0:
#                     print('No reviews found. Retrying in 5 seconds...')
#                     retry_count += 1
#                     time.sleep(5)
#                 else:
#                     walmart_reviews.extend(extracted_reviews)
#                     print(f'Review count in page {page_number}:', len(extracted_reviews))
#                     time.sleep(2)
#                     break

#             except Exception as e:
#                 print(f"Error encountered: {e}. Retrying in 3 seconds...")
#                 retry_count += 1
#                 time.sleep(3)
#         else:
#             print(f"Max retries exceeded for page {page_number}. Skipping to the next page.")

# # %%
# walmart = pd.DataFrame(walmart_reviews)
# walmart['Retailer'] = "Walmart"

# from datetime import date

# walmart['scraping_date'] = date.today().strftime('%Y/%m/%d')
# walmart['scraping_date'] = pd.to_datetime(walmart['scraping_date']).dt.date
# walmart['Review date'] = pd.to_datetime(walmart['Review date']).dt.date
# walmart['Review rating'] = walmart['Review rating'].astype(str).str.replace(' out of 5 stars review', '').astype(int)
# walmart.drop_duplicates(inplace=True)

# walmart['HP Model Number'] = walmart['Model'].str.extract(r'(\d+e?)')

# walmart['Review date'] = pd.to_datetime(walmart['Review date'])

# walmart_hp_combine = pd.merge(walmart, df_amazon, on="HP Model Number", how="left")
# walmart_hp_combine['Review Model'] = walmart_hp_combine['HP Model']
# columns_to_drop = [
#     'Model', 'HP Model Number', 'Comp Model number', 'HP Model'
# ]

# walmart_hp_combine = walmart_hp_combine.drop(columns_to_drop, axis=1)

# walmart_hp_combine = walmart_hp_combine.drop_duplicates()
# walmart_hp_combine['Competitor_Flag'] = walmart_hp_combine['Review Model'].apply(lambda x: 'No' if 'HP' in x else 'Yes')
# walmart_hp_combine['Country'] = 'US'

# column_mapping = {
#     'Review date': 'Review_Date',
#     'review_text': 'Review_Content',
#     'Review rating': 'Review_Rating',
#     'url': 'URL',
#     'review_title': 'Review_Title',
#     'Verified Purchase or not': 'Verified_Purchase_Flag',
#     'reviewer_name': 'Review_Name',
#     'syndication': 'Syndicated_Source',
#     'stars': 'Review_Rating',
#     'Retailer': 'Retailer',
#     'scraping_date': 'Scraping_Date',
#     'Comp Model': 'Comp_Model',
#     'HP Class': 'HP_Class',
#     'Review Model': 'Review_Model',
#     'Review title': 'Review_Title',
#     'Review Content': 'Review_Content',
#     'Review date': 'Review_Date',
#     'URL': 'URL',
#     'Seeding or not': 'Seeding_Flag',
#     'Review name': 'Review_Name',
#     'People_find_helpful': 'People_Find_Helpful',
#     'Syndicated source': 'Syndicated_Source',
#     'Comp Model': 'Comp_Model',
#     'HP Class': 'HP_Class',
#     'Review Model': 'Review_Model',
#     'Competitor_Flag': 'Competitor_Flag'
# }

# # Rename columns
# walmart_hp_combine = walmart_hp_combine.rename(columns=column_mapping)
# print('Total walmart review,', len(walmart_hp_combine))

# Final_review = pd.concat([Final_review, walmart_hp_combine], ignore_index=True)

Final_review['Country'] = 'US'
Final_review['Review_Date'] = pd.to_datetime(Final_review['Review_Date']).dt.date
Final_review['Review_Rating'] = Final_review['Review_Rating'].astype('int64')
Final_review['People_Find_Helpful'] = Final_review['People_Find_Helpful'].fillna(0).astype('int64')
Final_review['Scraping_Date'] = pd.to_datetime(Final_review['Scraping_Date']).dt.date

string_columns = Final_review.select_dtypes(include='object').columns
Final_review[string_columns] = Final_review[string_columns].fillna('')
Final_review.head()


# %% [markdown]
# # Map Insider review in staples

# %%
def clean_text(text):
    text = str(text)

    # Remove non-English characters and punctuations
    cleaned_text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    # Remove extra whitespaces and convert to lowercase
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip().lower()
    english_words = re.findall(r'\b[a-z]+\b', cleaned_text)
    first_ten_words = ''.join(english_words[:10])
    return first_ten_words


result_combine_filtered['FirstTenWords'] = result_combine_filtered['Review_Content'].fillna("").apply(clean_text)
hho = result_combine_filtered[result_combine_filtered['Syndicated_Source'] == 'The Insiders'][['FirstTenWords']]
hho.drop_duplicates(subset='FirstTenWords', keep='first', inplace=True)
staple_final['FirstTenWords'] = staple_final['Review_Content'].fillna("").apply(clean_text)

insider = pd.merge(hho, staple_final, how='inner', on='FirstTenWords', suffixes=('', '_staples'))
insider.drop(columns=['FirstTenWords'], inplace=True)
insider['Review_Source'] = 'hp.com - The Insiders'
insider_concat = pd.concat([Final_review, insider])
insider_concat.sort_values(
    by=['Review_Source', 'Review_Model', 'Retailer', 'Review_Date', 'Review_Title', 'Review_Content'], ascending=False,
    inplace=True)
# insider_concat.drop_duplicates(subset=['Review_Content','Review_Model','Retailer','Review_Date','Review_Title'], keep='first', inplace=True)

### Consolidate review source

insider_concat['Review_Source'] = insider_concat.apply(
    lambda row: 'hp.com - The Insiders' if row['Review_Source'] == 'hp.com - The Insiders' else
    row['Syndicated_Source'] if row['Syndicated_Source'] and row['Syndicated_Source'] != '' and row[
        'Syndicated_Source'] != ' ' else
    row['Seeding_Flag'] if row['Seeding_Flag'] and row['Seeding_Flag'] != ' ' and row['Seeding_Flag'] != '' and row[
        'Seeding_Flag'] != False and row['Seeding_Flag'] != 'FALSE'
                           and row['Seeding_Flag'] != 'False' else 'Organic', axis=1)
insider_concat['Review_Source'] = insider_concat['Review_Source'].apply(
    lambda x: 'Seeding' if x == True or x == 'TRUE' or x == 'True' else x)

insider_concat['Review_Source'].unique()

# %%
insider_concat['Review_Rating_Label'] = insider_concat['Review_Rating'].astype(int).apply(
    lambda x: '1-2-3-star' if x < 4 else '4-5-star')
insider_concat['Country'] = 'US'
insider_concat['Review_Date'] = pd.to_datetime(insider_concat['Review_Date']).dt.date
insider_concat['Review_Rating'] = insider_concat['Review_Rating'].astype('int64')
insider_concat['People_Find_Helpful'] = insider_concat['People_Find_Helpful'].fillna(0).astype('int64')
insider_concat['Scraping_Date'] = pd.to_datetime(insider_concat['Scraping_Date']).dt.date

string_columns = insider_concat.select_dtypes(include='object').columns
insider_concat[string_columns] = insider_concat[string_columns].fillna('')
# insider_concat.drop(columns = ['Sentiment','Topic'],inplace = True)
insider_concat.drop_duplicates(inplace=True)
insider_concat.to_csv(
    r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\MMK_Review.csv",
    index=False)

# %% [markdown]
# ## Cumulative count

# %%
insider_concat['Review_Date'] = pd.to_datetime(insider_concat['Review_Date'])
insider_concat = insider_concat[
    ['Retailer', 'Review_Model', 'Review_Date', 'Review_Rating', 'Review_Source']].sort_values(
    by=['Retailer', 'Review_Model', 'Review_Date', 'Review_Rating'])

insider_concat = insider_concat.sort_values(
    by=['Retailer', 'Review_Model', 'Review_Rating', 'Review_Date', 'Review_Source'])
insider_concat['Daily_Count'] = \
    insider_concat.groupby(['Retailer', 'Review_Model', 'Review_Rating', 'Review_Date', 'Review_Source'])[
        'Review_Model'].transform('count')

# Create a DataFrame with all possible combinations of 'Retailer', 'Review_Model', 'Review_Rating', and dates up to today
dates = pd.date_range(start=insider_concat['Review_Date'].min(), end=pd.Timestamp.today(), freq='D')
idx = pd.MultiIndex.from_product([insider_concat['Retailer'].unique(), insider_concat['Review_Model'].unique(),
                                  insider_concat['Review_Rating'].unique(), insider_concat['Review_Source'].unique(),
                                  dates],
                                 names=['Retailer', 'Review_Model', 'Review_Rating', 'Review_Source', 'Review_Date'])
full_df = pd.DataFrame(index=idx).reset_index()
full_df['Count'] = 0

# Merge with existing DataFrame
merged_df = pd.merge(full_df, insider_concat,
                     on=['Retailer', 'Review_Model', 'Review_Rating', 'Review_Source', 'Review_Date'], how='left')
merged_df['Daily_Count'].fillna(0, inplace=True)
merged_df['Review_Count'] = merged_df['Daily_Count'] + merged_df['Count']
merged_df.sort_values(by=['Retailer', 'Review_Model', 'Review_Date', 'Review_Rating'], inplace=True)
merged_df['Cumulative_Review_Count'] = \
    merged_df.groupby(['Retailer', 'Review_Model', 'Review_Rating', 'Review_Source'])['Review_Count'].cumsum()
merged_df.drop(columns='Count', inplace=True)
merged_df.drop(columns='Daily_Count', inplace=True)

# %%
merged_df.to_csv(
    r"C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\04_source file for PowerBI\MMK_Review_count.csv",
    index=False)
print('MMK_insider script completed. MMK_review and MMK_review_count files saved')
