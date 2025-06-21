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

def log_error_to_csv(error_type, error_message, error_traceback):
    with open('errors.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([error_type, error_message, error_traceback])

def program():
    # Get the current date and time
    now = datetime.now()
    
    # Format the timestamp as a string
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    
    # Print the timestamp
    print("Current Timestamp:", timestamp)
    
    print('Running Tassel_raw_date_scraping.py')
    
    # %%
    excel_file_path = r"C:\Users\tayu430\anaconda3_remote\envs\webscrapper\My Scripts\Star rating scrape URL and info - NPI.xlsx"
    sheet_name = "data_new"
    
    # Read the Excel sheet into a DataFrame
    df_amazon = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')
    df_amazon['HP Model Number'] = df_amazon['HP Model Number'].astype(str)
    df_amazon['Comp Model number'] = df_amazon['Comp Model number'].fillna(0).round(0).astype(int).astype(str)
    df_amazon
    
    # %%
    path = r"C:\Users\tayu430\anaconda3_remote\envs\webscrapper\My Scripts\Star rating scrape URL and info - NPI.xlsx"
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
    
    # %% [markdown]
    # ## UK
    
    # %%
    from datetime import datetime
    global_cookies = {}
    def accept_cookies(url):
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
    
        try:
            # Visit the URL
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error for non-2xx status codes
    
            # Accept cookies policy
            payload = {'accept': 'all'}
            response = session.post(url, data=payload, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error for non-2xx status codes
    
            # Extract all cookies from the response headers and update global cookies
            for key, value in response.cookies.items():
                global_cookies[key] = value
    
        except requests.HTTPError as e:
            print(f"Error occurred during accepting cookies: {e}")
            return None
        return global_cookies
    
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
    
        
    def get_soup_us(url):
        global global_cookies
    
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
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
    
        # Define the initial JSON cookies data
        initial_cookies_json = '''
        [
            {
                "Host raw": "https://.amazon.co.uk/",
                "Name raw": "ubid-acbuk",
                "Path raw": "/",
                "Content raw": "259-6909043-1437223",
                "Expires": "22-05-2025 22:35:05",
                "Expires raw": "1747931705",
                "Send for": "Encrypted connections only",
                "Send for raw": "true",
                "HTTP only raw": "false",
                "SameSite raw": "no_restriction",
                "This domain only": "Valid for subdomains",
                "This domain only raw": "false",
                "Store raw": "firefox-default",
                "First Party Domain": ""
            },
            {
                "Host raw": "http://www.amazon.co.uk/",
                "Name raw": "csm-hit",
                "Path raw": "/",
                "Content raw": "tb:XN6TY644ZA4CNHWK94S0+s-XN6TY644ZA4CNHWK94S0|1717379581783&t:1717379581783&adb:adblk_no",
                "Expires": "19-05-2025 07:53:01",
                "Expires raw": "1747619581",
                "Send for": "Any type of connection",
                "Send for raw": "false",
                "HTTP only raw": "false",
                "SameSite raw": "no_restriction",
                "This domain only": "Valid for host only",
                "This domain only raw": "true",
                "Store raw": "firefox-default",
                "First Party Domain": ""
            }
        ]
        '''
    
        # Parse the initial JSON data
        initial_cookies_data = json.loads(initial_cookies_json)
        initial_cookies = {cookie["Name raw"]: cookie["Content raw"] for cookie in initial_cookies_data}
    
        # Use the global cookies if available, otherwise use initial cookies
        cookies_to_use = global_cookies if global_cookies else initial_cookies
    
        # Introduce a random delay to mimic human behavior
        time.sleep(random.uniform(1, 5))
    
        # Make the request with headers and cookies
        session = requests.Session()
        response = session.get(url, headers=headers, cookies=cookies_to_use, timeout=30)
        
        # Update the global cookies with the current response cookies
        global_cookies.update(session.cookies.get_dict())
    
        # Parse the response content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        # file_name = f"{random.randint(5, 150)}.html"
        # with open(file_name, 'w', encoding='utf-8') as file:
        #     file.write(str(soup))
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
    urls = ['https://www.amazon.co.uk/HP-DeskJet-Wireless-included-Reliable/product-reviews/B0CFFBXYSH/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&formatType=current_format',
           'https://www.amazon.co.uk/HP-DeskJet-Wireless-Included-Reliable/product-reviews/B0CFFC6LRR/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&formatType=current_format',
           'https://www.amazon.co.uk/HP-DeskJet-Wireless-Included-Reliable/product-reviews/B0CB722L39/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&formatType=current_format']
    
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
                        soup = get_soup(url)  # Get the soup object from the URL
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
    urls =  ['https://www.amazon.es/HP-DeskJet-2820e-Impresora-Multifunci%C3%B3n/product-reviews/B0CFFWJHMF/ref=cm_cr_arp_d_viewopt_fmt?formatType=current_format',
            'https://www.amazon.es/Impresora-Multifunci%C3%B3n-HP-impresi%C3%B3n-Fotocopia/product-reviews/B0CFG1PB4P/ref=cm_cr_arp_d_viewopt_fmt?formatType=current_format']
    
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
                        soup = get_soup(url)  # Get the soup object from the URL
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
    
    urls = ['https://www.amazon.com/HP-DeskJet-Wireless-included-588S5A/product-reviews/B0CT2R7199/ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=all_reviews&formatType=current_format',
           'https://www.amazon.com/HP-DeskJet-Wireless-Included-588S6A/product-reviews/B0CT2QHQVF/ref=cm_cr_arp_d_viewopt_fmt?ie=UTF8&reviewerType=all_reviews&formatType=current_format']
    
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
                        soup = get_soup_us(url)  # Get the soup object from the URL
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
    
    urls = ['https://www.bestbuy.com/site/reviews/hp-deskjet-2855e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6574145?variant=A',
           'https://www.bestbuy.com/site/reviews/hp-deskjet-4255e-wireless-all-in-one-inkjet-printer-with-3-months-of-instant-ink-included-with-hp-white/6575024?variant=A']
    
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
    
    import random
    User_Agent = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
    
    def get_page_number(url, cookie):
        User = random.choice(User_Agent)
        header = {
            'User-Agent': User,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Cookie': random.choice(cookie),  # Replace with the actual Cookie
            'Downlink': '10',
            'Dpr': '1',
            'Referer': url,
            'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }
        try:
            user_agents = [
                # Chrome on Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Firefox on Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
                # Safari on Mac
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Edge on Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.48',
                # Opera on Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.277',
                # Chrome on Linux
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Firefox on Linux
                'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
                # Chrome on Android
                'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
                # Safari on iOS
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
                # Firefox on Android
                'Mozilla/5.0 (Android 10; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0',
                # Opera on Android
                'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36 OPR/64.2.3282.60115',
                # Edge on Android
                'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36 EdgA/46.6.4.5151',
                # Samsung Browser on Android
                'Mozilla/5.0 (Linux; Android 10; SAMSUNG SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/15.0 Chrome/91.0.4472.120 Mobile Safari/537.36',
                # UC Browser on Android
                'Mozilla/5.0 (Linux; U; Android 10; en-US; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.120 UCBrowser/13.3.8.1305 Mobile Safari/537.36',
                # Opera Mini on Android
                'Opera/9.80 (Android; Opera Mini/58.0.2254/172.56; U; en) Presto/2.12.423 Version/12.16',
                # BlackBerry Browser
                'Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, Like Gecko) Version/6.0.0.337 Mobile Safari/534.1+',
                # Internet Explorer 11 on Windows
                'Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
                # Edge Legacy on Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/44.18362.449.0',
                # Safari on macOS
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
                # Internet Explorer 10 on Windows
                'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)',
                # Safari on iPad
                'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
                # Microsoft Edge (Chromium-based) on Windows
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.48',
                # Silk Browser on Fire OS
                'Mozilla/5.0 (Linux; U; Android 4.1.2; en-us; SCH-I535 4G Build/JZO54K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 Silk/2.2',
                # PlayStation 4 Browser
                'Mozilla/5.0 (PlayStation 4 7.02) AppleWebKit/605.1.15 (KHTML, like Gecko)',
                # Opera on macOS
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.277',
                # Brave Browser on macOS
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Brave/91.1.26.67',
                # Chrome on iOS
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/91.0.4472.80 Mobile/15E148 Safari/604.1',
                # Firefox on iOS
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/34.0 Mobile/15E148 Safari/605.1.15',
                # Chrome on macOS
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Chrome on Chrome OS
                'Mozilla/5.0 (X11; CrOS x86_64 14150.64.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                # Chrome on BlackBerry
                'Mozilla/5.0 (BB10; Touch) AppleWebKit/537.35+ (KHTML, like Gecko) Version/10.3.3.2205 Mobile Safari/537.35+',
                # Chrome on Windows Phone
                'Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36 Edge/40.15254.603',
                # Firefox on Windows Phone
                'Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950 XL) Gecko/20100101 Firefox/88.0',
                # Samsung Internet on Android
                'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/14.2 Chrome/91.0.4472.120 Mobile Safari/537.36',
                # Chrome on KaiOS
                'Mozilla/5.0 (Mobile; LYF/F30C/000JGJ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36',
                # Safari on tvOS
                'Mozilla/5.0 (Apple TV; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/604.1',
                # Silk Browser on Kindle Fire
                'Mozilla/5.0 (Linux; U; Android 4.0.4; en-us; KFTT Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30 Silk/3.17',
                # Chrome on Oculus Browser
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 OculusBrowser/14.7.0 Mobile VR Safari/537.36',
                # Firefox on Oculus Browser
                'Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0 OculusBrowser/14.7.0',
                # Chrome on HoloLens
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 HoloLens/2.0.210325.1003 Safari/537.36',
                # Safari on WatchOS
                'Mozilla/5.0 (Apple Watch; CPU iPhone OS 8_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12D508 Safari/600.1.4',
                # Firefox on Linux x86_64
                'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
                # Chrome on Linux x86_64
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Chrome on macOS x86_64
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Chrome on Windows x86_64
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                # Safari on iOS x86_64
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
                # Firefox on iOS x86_64
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/34.0 Mobile/15E148 Safari/605.1.15',
            ]
    
            headers = {
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.walmart.com/',  # Referer header might be required for some websites
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
            }
    
            # Choose a random user agent
            headers['User-Agent'] = random.choice(user_agents)
    
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            page_number_elements = soup.find_all(
                lambda tag: tag.name == 'a' and 'page-number' in tag.get('data-automation-id', ''))
            print("response recorded")
            page_numbers = [int(element.text) for element in page_number_elements]
    
            if page_numbers:
                last_page_number = max(page_numbers)
                return last_page_number
            else:
                print("No page numbers found. Assuming only one page.")
                # return 1
                return max(page_numbers)
    
        except requests.exceptions.RequestException as e:
            print(f"Request error encountered: {e}. Retrying in 20 seconds...")
            time.sleep(20)
        except Exception as e:
            print(f"Error encountered: {e}. Retrying in 20 seconds...")
            time.sleep(20)
    
        return None
    
    
    def get_review_walmart(url, cookie):
        extracted_reviews = []
        retry_count = 0
        header = {
            'User-Agent': random.choice(User_Agent),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Cookie': random.choice(cookie),
            'Downlink': '10',
            'Dpr': '1',
            'Referer': url,
            'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }
    
        sheets = "api"
        api = pd.read_excel(excel_file_path, sheet_name=sheets)
        api_key = api['API'][0]
        api_key
    
        try:
            url = f"https://api.scrapingdog.com/scrape?dynamic=true&api_key={api_key}&url={url}"
            # url = f"https://api.scrapingdog.com/scrape?api_key={api_key}&url={link}"
            print(url)
            response = requests.get(url, headers=header)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            li_elements = soup.find_all('li', class_='dib w-100 mb3')
            title_all = soup.find('a', class_='w_x7ug f6 dark-gray')
            if title_all:
                title = title_all.get('href')
                pattern = r'(\d{4}[a-zA-Z]?)-'
                model = re.findall(pattern, title)
            #         model = re.search(r'\b(\d{4}e)\b', title_all).group(1)
            li_elements = soup.find_all('li', class_='dib w-100 mb3')
    
            if li_elements:
                for li_tag in li_elements:
                    product = {}
                    product['Model'] = title
                    product['Review rating'] = li_tag.select_one('.w_iUH7').text
                    product['Verified Purchase or not'] = li_tag.select_one(
                        '.pl2.green.b.f7.self-center').text if li_tag.select_one('.pl2.green.b.f7.self-center') else None
                    product['Review date'] = li_tag.select_one('.f7.gray').text if li_tag.select_one('.f7.gray') else None
    
                    review_title_element = li_tag.select_one('h3.b')
                    product['Review title'] = review_title_element.text if review_title_element else None
    
                    product['Review Content'] = li_tag.find('span', class_='tl-m mb3 db-m').text if li_tag.find('span',
                                                                                                                class_='tl-m mb3 db-m') else None
                    product['Review name'] = li_tag.select_one('.f6.gray').text if li_tag.select_one('.f6.gray') else None
    
                    syndication_element = li_tag.select_one('.b.ph1.dark.gray')
                    product['Syndicated source'] = syndication_element.text if syndication_element else None
                    product['URL'] = url
    
                    extracted_reviews.append(product)
            elif "Robot or human" in response.text:
                print()
    
    
        except requests.exceptions.RequestException as e:
            print(f"Request error encountered: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Error encountered: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    
        return extracted_reviews
    
    
    # %%
    urls = [
        'https://www.walmart.com/reviews/product/5129928602',
        'https://www.walmart.com/reviews/product/5129928603'
    ]
    
    # %%
    import time
    
    walmart_reviews = []
    
    for link in urls:
        # initial value don't modify
        retry_count = 0
    
        # you can modify with your need
        max_try = 5
        retry_limit = max_try
        print(link)
        while retry_count < max_try:
            try:
                last_page_number = get_page_number(link, cookie)
                if last_page_number is None:
                    retry_count += 1
                    if retry_count <= retry_limit:
                        print("Failed to retrieve last page number. Retrying... Also Extract the data")
                        if retry_count == 1:
                            for page_number in range(1, last_page_number + 1):
                                retry_count = 0  # Reset retry count for each page
                                while retry_count < max_try:
                                    try:
                                        target_url = f'{link}?page={page_number}'
                                        extracted_reviews = get_review_walmart(target_url, cookie)
    
                                        if len(extracted_reviews) == 0:
                                            print('No reviews found. Retrying in 5 seconds...')
                                            retry_count += 1
                                            time.sleep(5)
                                        else:
                                            walmart_reviews.extend(extracted_reviews)
                                            print(f'Review count in page {page_number}:', len(extracted_reviews))
                                            time.sleep(2)
                                            break
    
                                    except Exception as e:
                                        print(f"Error encountered: {e}. Retrying in 3 seconds...")
                                        retry_count += 1
                                        time.sleep(3)
                                else:
                                    print(f"Max retries exceeded for page {page_number}. Skipping to the next page.")
    
                        time.sleep(3)
                        continue
                    else:
                        print("Failed to retrieve last page number after multiple retries. Changing the link.")
                        # Change the link here
                        link = new_link
                        retry_count = 0  # Reset retry count
                        continue
                print('Total pages:', last_page_number)
                break
            except Exception as e:
                print(f"Error encountered: {e}. Retrying in 3 seconds...")
                time.sleep(3)
            retry_count += 1
        else:
            print("Max retries exceeded for this link. Moving to the next link.")
            continue  # Move to the next link if max retries exceeded
    
        if last_page_number is None:
            print("Skipping processing for this link due to inability to retrieve last page number.")
            continue  # Move to the next link if last_page_number is None
    
        for page_number in range(1, last_page_number + 1):
            retry_count = 0  # Reset retry count for each page
            while retry_count < max_try:
                try:
                    target_url = f'{link}?page={page_number}'
                    extracted_reviews = get_review_walmart(target_url, cookie)
    
                    if len(extracted_reviews) == 0:
                        print('No reviews found. Retrying in 5 seconds...')
                        retry_count += 1
                        time.sleep(5)
                    else:
                        walmart_reviews.extend(extracted_reviews)
                        print(f'Review count in page {page_number}:', len(extracted_reviews))
                        time.sleep(2)
                        break
    
                except Exception as e:
                    print(f"Error encountered: {e}. Retrying in 3 seconds...")
                    retry_count += 1
                    time.sleep(3)
            else:
                print(f"Max retries exceeded for page {page_number}. Skipping to the next page.")
    
    
    
    # %%
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
    
    previous = pd.read_csv(r'C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\Tassel\Tassel_EMEA_Review_Raw.csv')
    previous['Review_Date'] = pd.to_datetime(previous['Review_Date']).dt.date
    previous['Scraping_Date'] =  pd.to_datetime(previous['Scraping_Date']).dt.date
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
    tassel_emea_review_raw = pd.read_csv(r'C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\Tassel\Tassel_EMEA_Review_Raw.csv')
    
    # Combine the data, keeping the header of Tassel_EMEA_Review_Raw.csv
    combined_data = pd.concat([tassel_emea_review_raw, merged_reviews], ignore_index=True)
    
    # Save the combined data to Tassel_EMEA_Review_Raw.csv
    combined_data.to_csv(r'C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\Tassel\Tassel_EMEA_Review_Raw.csv', index=False)
    
    # # # Remove the original CSV files
    # # for file_path in file_paths:
    # #     os.remove(file_path)
    
    df = pd.read_csv(r'C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\Tassel\Tassel_EMEA_Review_Raw.csv')
    
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
    df_no_duplicates.to_csv(r'C:\Users\TaYu430\OneDrive - HP Inc\General - Core Team Laser & Ink\For Lip Kiat and Choon Chong\Web review\14_Text_mining\Tassel\Tassel_EMEA_Review_Raw.csv', index=False)
    
    print('Tassel_raw_data_scraping completed. Tassel_raw file saved')



try: