import os
import StringIO
import hashlib
import boto3

ACCESS_KEY = "AKIAR3BRNOQR3K5DBQUY"
SECRET_KEY = "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"
BUCKET = ""
FILE_ROOT = ""


def syncS3():
    connection = S3Connection(ACCESS_KEY, SECRET_KEY)
    bucket = connection.get_bucket(BUCKET)
    s3_keys = bucket.list()
    save_keys(s3_keys)

def save_keys(keys):
    for key in keys:
        key_string = str(key.key)
        parent_folder = "\\".join(key_string.split("/")[0:2])
        parent_folder = os.path.join(FILE_ROOT, parent_folder)
        key_path = os.path.join(parent_folder, key_string.split("/")[-1])
        if not os.path.exists(parent_folder):
            os.makedirs(parent_folder)
        if not os.path.exists(key_path):
            save_to = open(key_path, "wb")
            key.get_file(save_to)
            save_to.close()
            print "saved: %s" % key_path
        else:
            # etag holds the md5 for the key, wrapped in quotes
            s3_md5 = key.etag.strip('"')
            local_md5 = hashlib.md5(open(key_path, "rb").read()).hexdigest()
            if s3_md5 == local_md5:
                print "already exists, file the same: %s" % key_path
            else:
                save_to = open(key_path, "wb")
                key.get_file(save_to)
                save_to.close()
                print "file changed, overwrote: %s" % key_path
        

if __name__ == "__main__":
    syncS3()
