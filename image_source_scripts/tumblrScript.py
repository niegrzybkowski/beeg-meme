from pytumblr import TumblrRestClient
import xml.etree.ElementTree as ET
import datetime
import argparse
import json

from loadImage import get_img_from_link

parser = argparse.ArgumentParser(
    prog="TumblrAPIGetter",
    description="Script to retrieve images using the Tumblr API"
)
parser.add_argument("-n", "--number",
                    help="Number of images retrieved",
                    type=int,
                    default=5
                    )

parser.add_argument("-ck", "--consumer_key",
                    help="The consumer key of your Tumblr Application",
                    required=True)

parser.add_argument("-cs", "--consumer_secret",
                    help="The consumer secret of your Tumblr Application",
                    required=True)

parser.add_argument("-ot", "--oauth_token",
                    help="The user specific token, received from the /access_token endpoint",
                    required=True)

parser.add_argument("-os", "--oauth_secret",
                    help="The user specific secret, received from the /access_token endpoint",
                    required=True)

parser.add_argument("-t", "--tag",
                    help="Reddit tag name",
                    default="memes")
args = parser.parse_args()


img_tag  = "npf_row" 

def retrieve_img_url(post):
    # find start and end of the image tag
    start = post['body'].find('<div')
    end = post['body'].find("</div>") + len("</div>")
    # parse xml string
    body = ET.fromstring(post["body"][start:end])
    # get a string of post's image in different resolutions
    img_formats = body[0][0].attrib['srcset']
    # retrieve the last resolution (the largest) and get the url
    largest_img = img_formats.split(',')[-1].strip().split(' ')[0]
    return largest_img

def create_record(post):
    # post's image url
    img_url = retrieve_img_url(post)
    # post's publication datetime
    dt = datetime.datetime.fromtimestamp(p["timestamp"]).isoformat()
    record = {"id": post["id"],
              "datetime": dt,
              "author": post['blog']['name'],
              "note_count": post['note_count'],
              "description": post['summary'],
              "img": img_url
              }
    return record

if __name__ == "__main__":
    client = TumblrRestClient(
        args.consumer_key,
        args.consumer_secret,
        args.oauth_token,
        args.oauth_secret
    )
    limit = args.number
    
    num_imgs = 0
    min_timestamp = None
    while num_imgs < limit:
        posts = client.tagged(args.tag, limit=min(args.number,20),before=min_timestamp)
        for p in posts:
            min_timestamp = p["timestamp"]
            if "body" in p and img_tag in p["body"]:
                record = create_record(p)
                print(json.dumps(record))
                get_img_from_link(record['img'], str(record['id'])+'-tumblr')
                num_imgs += 1
                if num_imgs == limit:
                    break
    #print(num_imgs)
