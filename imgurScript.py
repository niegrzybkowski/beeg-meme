from imgurpython import ImgurClient
import datetime
import argparse
import json

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
}
size = 200

parser = argparse.ArgumentParser(
    prog="ImgurAPIGetter",
    description="Script to retrieve images using the Imgur API"
)
parser.add_argument("-n", "--number",
                    help="Number of images retrieved",
                    type=int,
                    default=5
                    )

parser.add_argument("-id", "--client_id",
                    required=True)

parser.add_argument("-p", "--password",
                    required=True)
args = parser.parse_args()


def parse_record(record):
    if not record.is_album:
        return [album_image_to_dict(record)]
    else:
        res = []
        for img in record.images:
            res.append(enrich_image(img, record))
        return res


# def get_img_from_link(link):
#     img_page = requests.get(link, headers=headers)
#     if img_page.status_code != 200:
#         return np.empty(0)
#     image_file = io.BytesIO(img_page.content)
#     image = Image.open(image_file).convert("L")
#     return np.array(image.resize((size, size)))


def add_global_id(dct):
    dct["global_id"] = dct["id"] + "-" + str(dct["datetime"])
    dct["datetime"] = datetime.datetime.fromtimestamp(dct["datetime"]).isoformat()
   # if not dct["animated"]:
   #     dct["image"] = get_img_from_link(dct["link"])


def album_image_to_dict(img):
    img_dict = vars(img)
    add_global_id(img_dict)
    return img_dict


def enrich_image(img, album):
    redundants = ['cover',
 'cover_height',
 'cover_width',
 'images',
 'images_count',
 'include_album_ads',
 'layout',
 'privacy']
    override = ['title', 
    'description', 
    'comment_count',
 'favorite_count',
 'ups',
 'downs',
 'points',
 'score','account_url',
 'account_id']
    for att in dir(album):
        if att.startswith('__'):
            continue
        if att in img.keys() and att not in override:
            continue
        if att in override:
            if img[att] is not None:
                continue
        if att in redundants:
            continue
        img[att] = getattr(album, att)
            
    add_global_id(img)
    return img


if __name__ == "__main__":
    client = ImgurClient(args.client_id, args.password)

    items = client.gallery(section='user', sort='time', page=0, show_viral=False)
    n = args.number
    #results = []
    for item in items[:n]:
        #results.extend(parse_record(item))
        for img in parse_record(item):
            if not img["animated"]:
                print(json.dumps(img))
                #print(img)