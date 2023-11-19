from pytumblr import TumblrRestClient
import datetime
import argparse
import json

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
                    default="dank memes")
args = parser.parse_args()


img_tag  = "img src=\""

def retrieve_url(content_xml, img_tag):
    start = content_xml.find(img_tag) + len(img_tag)
    end = content_xml.find("\"", start)
    return content_xml[start:end]


if __name__ == "__main__":
    client = TumblrRestClient(
        args.consumer_key,
        args.consumer_secret,
        args.oauth_token,
        args.oauth_secret
    )

    posts = client.tagged("dank memes", limit=args.number)
    for p in posts:
        content_xml = p["trail"][0]["content"]
        if img_tag in content_xml:
            # post's image url
            img_url = retrieve_url(content_xml, img_tag)
            # post's publication datetime
            dt = datetime.datetime.fromtimestamp(p["timestamp"]).isoformat()
            record = {"id":p["id"],"datetime":dt,"img":img_url}
            print(json.dumps(record))