from google.cloud import storage
import os

 
def upload_to_bucket(blob_name, path_to_file):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        'beeg-meme-cred.json')

    bucket = storage_client.get_bucket("images_beeg_meme")
    blob = bucket.blob(blob_name)
    print(path_to_file)
    print(blob_name)
    blob.upload_from_filename(path_to_file)
    os.remove(path_to_file)
