"""
    UTILITIES FILE WITH ALL CRAWLING METHODS TO FETCH DATA FROM FLICKR. CAN BE DONE
    BY EITHER ACCESSING GZIPPED SITEMAPS OR DIRECTLY ADDRESSING THE FLICKR API.
"""

import requests
import time
from pathlib import Path
import os

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
    when it fails, it will keep trying with ever incrementing time in between 
    two following requests until it succeeds. Additionally, a response with 
    status code 500 or higher will be considered a failure. 
    """
    fail = 0
    while True:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code >= 500:
                raise requests.exceptions.HTTPError("Server error: 5xx")
            return r
        #except (requests.exceptions.RequestException, requests.exceptions.HTTPError):
        except:
            fail += 1
            print('Attempt: ', str(fail))
            time.sleep(10 * fail)


def fetch_gzip(date, number):
    datestring = date
    numberstring = left_pad(number)
    url = f'https://www.flickr.com/sitemap/{datestring}/photos/sitemap-photos-{numberstring}.xml.gz'
    # replace by call to backoff
    #r = requests.get(url, timeout=20)
    r = backoff(url)
    if r.status_code == 200:
        data = r.content
        return (True, data)
    elif r.status_code == 403:
        #you reached the end of the available sitemaps for this day! 
        return (False, None)
        
        
def fetch_image_groups(img_id, key):
    """
        recovery of POOL information through: https://www.flickr.com/services/api/explore/flickr.photos.getAllContexts
        Will return the groups the image is part of. 
        img_id: INT : flickr generated image ID of the hosted photo.
    """
    url = f'https://www.flickr.com/services/rest/?method=flickr.photos.getAllContexts&api_key={key}&photo_id={img_id}&format=json&nojsoncallback=1'
    #r = requests.get(url)
    r = backoff(url)
    try:
        response = r.json()
    except:
        #handle err 500
        print('fail in fetch_image_groups')
        print(r.status_code)
    if 'pool' in response:
        return response['pool']
    else:
        return []
    
def fetch_image_tags(img_id, key):
    """Recovery of TAG information through: https://www.flickr.com/services/api/flickr.photos.getInfo.html 
        ARCHITECTURE WARNING:
    tag ID's are unique for every instance of the tag i.e.: 
    2 images with the same tag do not share the same tagid in the JSON response!!
    Collecting the Flickr tag ID is not interesting; and we need a system that keeps track of 
    tags we have in the backend!

    e.g.: 
        1) https://www.flickr.com/photos/margotraggett/12874140314/
        repl: 
                {
                    "id": "84955214-12874140314-7355",
                    "author": "84976544@N07",
                    "authorname": "Margot Raggett",
                    "raw": "lion",
                    "_content": "lion",
                    "machine_tag": 0
                },
        2) https://www.flickr.com/photos/87413816@N04/52897111556/
        repl:
                {
                    "id": "87381677-52897111556-7355",
                    "author": "87413816@N04",
                    "authorname": "Frederic_P.",
                    "raw": "lion",
                    "_content": "lion",
                    "machine_tag": 0
                },
    """
    url = f'https://www.flickr.com/services/rest/?method=flickr.photos.getInfo&api_key={key}&photo_id={img_id}&format=json&nojsoncallback=1'
    #r = requests.get(url)
    r = backoff(url)
    while r.status_code > 499: 
        time.sleep(60)
        r = backoff(url)
    try:
        response = r.json()
    except:
        print(r.status_code)
        print(r)
        exit('FAILED CASE TO HANDLE')
    if 'photo' not in response and response['stat'] == 'fail' and ('not found' in response['message'] or 'is private' in response['message']):
        return False
    #print(response)
    while 'photo' not in response and 'API service is not currently available.' in response['message']:
        time.sleep(60)
        print('API timeout')
        r = backoff(url)
        response = r.json()
    try:
        tags = response['photo']['tags']['tag']
    except:
        print(response)
        exit('unhandled case found; aborted script')
    #unix timetag
    upload_date = response['photo']['dateuploaded']
    return {
        'tags': tags,
        'upload_date' : upload_date,
        'views': response['photo']['views']
    }

    
def get_image(link, storage_location, padding, levels):
    """
    link: str: URL where the image is available
    storage_lcation: str: base directory where the image will be stored. 
    padding: int: how many symbols should be used per subfolder name
    levels: int: how many levels of subfolders should be created under storage_location. 
    This function will download a given image and returns it full path on the local drive 
    after downloading. It is safe to use the image ID as a filename too; we ensured
    during the deduplication phase that these are unique!
    """
    img_data = requests.get(link).content
    img_name = link.split('/')[-1]
    storage_path = [storage_location]
    for i in range(levels):
        # Create a subfolder name of specified length
        subfolder_name = img_name[i * padding:(i + 1) * padding]
        storage_path.append(subfolder_name)
    basedir = os.path.join(*storage_path)
    Path(basedir).mkdir(parents=True, exist_ok=True)
    file_path = os.path.join(basedir, img_name)
    file = open(file_path, 'wb')
    file.write(img_data)
    file.close()
    return file_path
        
