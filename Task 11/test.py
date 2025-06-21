import json

# Specify the path to your local JSON file
file_path = 'file.json'

# Open and load the JSON file
with open(file_path, 'r') as file:
    data = json.load(file)

product = {}


product["title"] = data['props']['pageProps']['initialData']['data']['product']['name']

product["rating"] = data['props']['pageProps']['initialData']['data']['reviews']['averageOverallRating']

product["review_count"] = data['props']['pageProps']['initialData']['data']['reviews']['totalReviewCount']


product["discount_price"] = data['props']['pageProps']['initialData']['data']['product']['priceInfo']['currentPrice']['price']
product["list_price"] = data['props']['pageProps']['initialData']['data']['product']['secondaryOfferPrice']['currentPrice']['price'] 
product['5star'] = data['props']['pageProps']['initialData']['data']['reviews']['ratingValueFiveCount']
product['4star'] = data['props']['pageProps']['initialData']['data']['reviews']['ratingValueFourCount']
product['3star'] = data['props']['pageProps']['initialData']['data']['reviews']['ratingValueThreeCount']
product['2star'] = data['props']['pageProps']['initialData']['data']['reviews']['ratingValueTwoCount']
product['1star'] = data['props']['pageProps']['initialData']['data']['reviews']['ratingValueOneCount']
print(product)