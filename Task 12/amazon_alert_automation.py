from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse
import re
from datetime import datetime
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Email details please change the followings
sender_email = "mark147258369963852741@gmail.com" #Anam Temporary disposable mail
receiver_email = "asifanam4@gmail.com"
subject = "Alert: Flag Triggered in MMK Star Ratings"
password = "hqkf qbrk wjgf vtfa" #Google App passwor 


# Get the current date and time
now = datetime.now()

# Format the timestamp as a string
scraping_date = now.strftime("%d-%m-%Y")


# Function to fetch and parse Amazon product page content
def get_soup_amazon(url):
    """
    Fetches the HTML content of the Amazon product page and returns it as a BeautifulSoup object.
    """
    parsed_url = urlparse(url)
    host = parsed_url.netloc  # Extract the hostname for setting headers
    headers = {
        "Host": host,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    # Set cookies based on Amazon domain to handle session requirements
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
    # Send the request with headers and cookies, then parse the HTML

    req = requests.get(url, headers=headers, cookies=cookies)
    soup = BeautifulSoup(req.content, "html.parser")
    return soup

def to_product_url(url):
    pattern = r"https://www\.amazon\.com/product-reviews/([A-Z0-9]+)"
    # Check if the URL matches the pattern
    if re.match(pattern, url):
        # Construct the product URL using the product ID
        html = get_soup_amazon(url)
        a_tag = html.find('a', class_='a-link-normal')
        if a_tag:
            a_tag_link_with_class = a_tag['href']
        return f"https://www.amazon.com/{a_tag_link_with_class}/"

    else:
        return url

def get_product_title(soup):
    """
    Extracts the product title from the Amazon product page.
    """
    title_element = soup.find("span", id="productTitle")  # Find the title element by id
    if title_element:
        return title_element.text.strip()  # Return the text content of the title, stripped of extra whitespace
    return None  # Return None if the title element is not found

def get_model(title):
    """
    Extracts the main product name from a product title using a regular expression.
    
    Parameters:
    - title (str): The product title as a string.
    
    Returns:
    - str: The main product name if found, or None if no match is found.
    """
    # Search for the main product name pattern in the title (up to '8025e')
    name_match = re.search(r'([A-Za-z\s]+?\d+e*)', title)
    
    # Return the matched product name, or None if no match is found
    if name_match:
        return name_match.group(0)
    return None


def get_class_name(model):
    # Load the Excel sheet
    try:
        df = pd.read_excel('Star rating scrape URL and info - NPI.xlsx', sheet_name='data_new')
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return None

    # Check if the necessary columns exist ('HP model' and 'HP class')
    if 'HP Model' not in df.columns or 'HP Class' not in df.columns:
        print("Required columns 'HP model' or 'HP class' not found.")
        return None

    # Search for the title in the 'HP model' column and get the corresponding 'HP class'
    row = df[df['HP Model'] == model]

    if not row.empty:
        # Return the 'HP class' if found
        return row.iloc[0]['HP Class']
    else:
        print(f"Model '{model}' not found in the data.")
        return None



# Function to check if the product rating is below 4.0
def poor_rating_flag(soup):
    """
    Checks if the product has a rating below 4.0.
    """
    elements = soup.find_all("span", class_="a-size-base a-color-base")  # Find rating elements
    for element in elements:
        text = element.text.strip()
        try:
            rating_value_new = float(text)  # Convert text to float
            if rating_value_new < 4.0:
                return True, rating_value_new
            else:
                return False, rating_value_new
        except ValueError:
            continue  # Ignore non-numeric values
    return False

def untwist_flag(single_url):
    """
    Checks if specific UI elements (indicating product versions or variations) are missing.
    Returns False if versions are present, True if missing.
    """
    soup = get_soup_amazon(single_url)
    version_select = soup.find("select", id="format-type-dropdown")
    
    if version_select:
        options = version_select.find_all("option")
        for option in options:
            if "Old Version" in option.text or "New Version" in option.text:
                return False  # Versions found, return False
    return True  # No version-related options found, return True


# Function to check if the product is marked as "Currently unavailable."
def delist_flag(soup):
    """
    Checks if the product is marked as "Currently unavailable."
    """
    availability_element = soup.find("span", class_="a-size-medium a-color-success")
    if availability_element and "Currently unavailable" in availability_element.text.strip():
        return True
    else:
        return False

def create_email_body(data):
    """
    Formats the email body based on dictionary values.
    """
    body = "Alert triggered for the following row:\n\n"
    body += f"HP_Model {data['HP_Model']}\n"
    body += f"HP_Class {data['HP_Class']}\n"
    body += f"Retailer {data['Retailer']}\n"
    body += f"HP_Rating {data['HP_Rating']}\n"
    body += f"Scraping_Date {data['Scraping_Date']}\n"
    body += f"Poor_rating_flag {data['Poor_rating_flag']}\n"
    body += f"Untwist_flag {data['Untwist_flag']}\n"
    body += f"Delist_flag {data['Delist_flag']}\n"
    body += f"URL {data['url']}\n"
    
    return body

def send_email(sender_email, receiver_email, subject, body, password):
    """
    Sends an email with the given subject and body to the receiver.
    """
    # Setup the MIME
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    try:
        # Connect to the Gmail server and send the email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error: {e}")



# Function to check if any of the flags is True, indicating that an email should be sent
def check_and_notify(is_poor_rating, is_untwisted, is_delisted, data=None):
    """
    Determines if an email notification should be sent based on the conditions.
    """
    if is_poor_rating or is_untwisted or is_delisted:
        return True  # Send mail if any flag is True
    else:
        return False

# List of Amazon product URLs to check
urls = [
    'https://www.amazon.com/product-reviews/B08WCDLKFK',
    'https://www.amazon.com/product-reviews/B08WC8VD8G'
]

# Prepare data storage for each URL
data = []

# Define retailer and scrape date
retailer = 'Amazon'

# Process each URL
for single_url in urls:
    soup = get_soup_amazon(to_product_url(single_url))  # Parse the product page
    is_poor_rating, rating = poor_rating_flag(soup)  # Check for poor rating flag
    is_untwisted = untwist_flag(single_url)  # Check for untwist flag
    is_delisted = delist_flag(soup)  # Check if delisted
    
    # Extract title and model
    title = get_product_title(soup)
    model = get_model(title)
    class_name = get_class_name(model)
    
    # Determine if an email needs to be sent based on flags
    should_send_mail = check_and_notify(is_poor_rating, is_untwisted, is_delisted)

    # Append to data list
    data_row = {
        "HP_Class": class_name,
        "HP_Model": model,
        "Retailer": retailer,
        "HP_Rating": rating,
        "Scraping_Date": scraping_date,
        "Poor_rating_flag": is_poor_rating,
        "Untwist_flag": is_untwisted,
        "Delist_flag": is_delisted,
        "url": single_url,
        "Mail send needed or not": should_send_mail
    }
    data.append(data_row)
    print(data_row)
    # Send email if any flag is triggered
    if should_send_mail:
        body = create_email_body(data_row)
        send_email(sender_email, receiver_email, subject, body, password)
# Create a DataFrame from collected data
df = pd.DataFrame(data)

# Save DataFrame to CSV
csv_file_path = 'amazon_alert_automation_table.csv'
df.to_csv(csv_file_path, index=False)
