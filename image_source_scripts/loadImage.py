import requests
from PIL import Image
import io

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
}


def get_img_from_link(link):
    try:
        img_page = requests.get(link, headers=headers)
        if img_page.status_code != 200:
            return np.empty(0)
        image_file = io.BytesIO(img_page.content)
        image = list(Image.open(image_file).convert("L").getdata())
    except:
        image = []
    return image