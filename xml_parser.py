import pandas as pd
import xml.etree.ElementTree as ET


def parse_sitemap(data, sample):
    """parses a sitemap which is passed in the data variable
    returns a subsample of that expressed as a float in sample.
    Will also return the amount of images in the sitemap."""

    namespace = {
        'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
        'image': 'http://www.google.com/schemas/sitemap-image/1.1'
    }


    root = ET.fromstring(data)
    # Initialize a list to hold the extracted data
    sitemap = []

    # Parse the XML string




    # Iterate over all <url> elements in the <urlset> parent
    for url in root.findall('ns:url', namespace):
        loc = url.find('ns:loc', namespace).text.strip()
        image_loc_result = url.find('image:image/image:loc', namespace)
        if image_loc_result is None:
            continue
        image_loc = url.find('image:image/image:loc', namespace).text.strip()

        image_title_elem = url.find('image:image/image:title', namespace)
        image_title = image_title_elem.text.strip() if image_title_elem is not None else None
        sitemap.append((loc, image_loc, image_title))
    # root = tree.getroot()
    df = pd.DataFrame(sitemap, columns=['URL', 'image_loc', 'title'])
    sampled = df.sample(frac=sample)
    sampled['user_imid'] = sampled['URL'].str.split('/photos/').str[1]
    sampled['user'] = sampled['user_imid'].str.split('/').str[0]
    sampled['imid'] = sampled['user_imid'].str.split('/').str[1]
    return (sampled, len(df))
