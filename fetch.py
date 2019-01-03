import json
import os
import requests

from derpibooru.image import Image
from multiprocessing.pool import ThreadPool


_FILE_PATH = os.path.abspath(os.path.dirname(__file__))


class FetchFailed(Exception):
    pass


class ImageFetchFailed(FetchFailed):
    pass


class MetadataFetchFailed(FetchFailed):
    pass


def get_image_metadata(image_id):
    url = "https://derpibooru.org/{}.json?fav=&comments=".format(image_id)

    response = requests.get(url)

    try:
        response.raise_for_status()
    except Exception as e:
        raise MetadataFetchFailed(
            "image {} metadata fetch failed: {}".format(
                image_id, e
            )
        )
    return response.json()


def download_image(url, path):
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(path, "wb") as f:
            for chunk in r:
                f.write(chunk)
    else:
        raise ImageFetchFailed(
            "Fetching from URL {} to file {} failed: {}".format(
                url, path, r.status
            )
        )


def fetch_image(image_id, scaling="large", overwite=False):
    # fetch image metadata
    image = Image(get_image_metadata(image_id))

    # figure out where to put the data
    directory = build_path(image_id, prefix=_FILE_PATH + "/images/")

    # get image URL for desired scale
    url = image.representations[scaling]

    # build image filename and path
    imageext = url.split(".")[-1]
    imagefilename = "{}.{}".format(scaling, imageext)
    imagepath = "{}/{}".format(
        directory, imagefilename
    )

    # create directory if it doesn't exist yet
    if not os.path.exists(directory) or overwite:
        os.makedirs(directory)

    # write the metadata to the directory
    metadatapath = "{}/metadata.json".format(directory)
    with open(metadatapath, "w") as f:
        json.dump(image.data, f)

    # download the file unless it exists
    # and overwrite isn't set
    if not os.path.exists(imagepath) or overwite:
        download_image(url, imagepath)


def build_path(image_id, prefix=""):
    folder2 = image_id % 1000
    folder1 = image_id - folder2
    return "{}{:04d}/{:03d}".format(prefix, folder1, folder2)


def main():
    fetch_image(0)


if __name__ == "__main__":
    main()