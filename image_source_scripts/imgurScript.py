from imgurpython import ImgurClient
import datetime
import argparse
import json

from loadImage import get_img_from_link



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


def change_attributes(dct):
    dct["global_id"] = dct["id"] + "-" + "imgur"
    dct["created"] = dct["datetime"]
    dct["created_utc"] = dct["datetime"]-3600
    dct["datetime"] = datetime.datetime.fromtimestamp(dct["created"]).isoformat()
    dct["datetime_utc"] = datetime.datetime.fromtimestamp(dct["created_utc"]).isoformat()
    dct["url"] = dct.pop("link")
    if not dct["animated"]:
        dct["image"] = get_img_from_link(dct["url"])


def album_image_to_dict(img):
    img_dict = vars(img)
    change_attributes(img_dict)
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
            
    change_attributes(img)
    return img


if __name__ == "__main__":
    client = ImgurClient(args.client_id, args.password)

    items = client.gallery(section='user', sort='time', page=0, show_viral=False)
    n = args.number
    i = 0
    for item in items:
        for img in parse_record(item):
            if not img["animated"]:
                i += 1
                print(json.dumps(img))
        if i >= n:
            break