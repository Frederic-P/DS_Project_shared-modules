"""
    UTILITIES FILE WITH ALL CRAWLING METHODS TO FETCH DATA FROM FLICKR. CAN BE DONE
    BY EITHER ACCESSING GZIPPED SITEMAPS OR DIRECTLY ADDRESSING THE FLICKR API.
"""

import requests

def left_pad(n):
    """ returns a number (n) padded with leading zeros to adhere
        to the Flickr sitemap structure.
        e.g.: 00000025
    """
    #make sure n = integer
    base = '00000000'+str(int(n))
    return base[-8:]

def backoff(url):
    """
    Backoff method: will retry to fetch the given URL (parameters = str)
    when it fails it will keep trying with ever incrementing time in between 
    two following requests until it succeeds. 
    """
    fail = 0
    while True:
        try:
            r = requests.get(url)
            return r
        except:
            fail += 1
            print('Attempt: ', str(fail))
            time.sleep(10*fail)

def fetch_gzip(date, number):
    datestring = date
    numberstring = left_pad(number)
    url = f'https://www.flickr.com/sitemap/{datestring}/photos/sitemap-photos-{numberstring}.xml.gz'
    # replace by call to backoff
    r = requests.get(url, timeout=20)
    print(r.status_code)
    if r.status_code == 200:
        data = r.content
        return (True, data)
    elif r.status_code == 403:
        #you reached the end of the available sitemaps for this day! 
        return (False, None)
        
        
def get_pool(img_id, key):
    pass
    
def get_tags(img_id, key):
    pass
