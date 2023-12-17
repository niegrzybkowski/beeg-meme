

import requests
from PIL import Image
import io

from saveToBucket import upload_to_bucket

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
}
PATH = "/home/nifi/images/"

def get_img_from_link(link, filename):
    try:
        img_page = requests.get(link, headers=headers)
        if img_page.status_code != 200:
            raise Exception("Couldn't get image")
        image_file = io.BytesIO(img_page.content)
        image = Image.open(image_file)
        full_filename = PATH + '.'.join((filename, link.split('.')[-1]))
        image.save(full_filename)
        upload_to_bucket(link, full_filename)

    except Exception as e:
        print(e)     

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        prog='Image loader',
        description='Function to load and save images from link'
    )

    parser.add_argument('-l', '--link',
                        help='URL to the image')
    
    parser.add_argument('-f', '--filename',
                        help='Name of the file to save')

    args = parser.parse_args()
    get_img_from_link(args.link, args.filename)
