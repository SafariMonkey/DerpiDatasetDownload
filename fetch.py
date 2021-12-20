import glob
import json
import os
import random
import requests
import shutil
import time
import logging

from derpibooru.image import Image
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from utils import (
    setup_logging,
    print_success,
    print_before,
    print_exc,
    rate_limit,
    time_call,
    thread_local_session,
    free_space_left,
    generate_numbers_below,
)

_THIS_FILE_PATH = os.path.abspath(os.path.dirname(__file__))
_DATA_PATH = os.getenv("DERPIDL_DATA_PATH", "{}/data".format(_THIS_FILE_PATH))
with open('user_key.txt', 'r') as f:
    _USER_KEY = f.read()


class FetchFailed(Exception):
    pass


class ImageFetchFailed(FetchFailed):
    pass


class MetadataFetchFailed(FetchFailed):
    pass


class SearchFetchFailed(FetchFailed):
    pass


class RepresentationMissing(KeyError):
    pass


def get_image_metadata(image_id):
    url = "https://derpibooru.org/{}.json?fav=&comments=".format(image_id)

    response = thread_local_session().get(url)

    try:
        response.raise_for_status()
    except Exception as e:
        raise MetadataFetchFailed(
            "image {} metadata fetch failed: {}".format(
                image_id, e
            )
        )
    return remove_per_user_data(response.json())


def remove_per_user_data(data):
    data.pop("interactions", None)
    data.pop("spoilered", None)
    return data


def get_random_images(seed, per_page, page):
    if per_page > 50:
        raise ValueError("per_page cannot exceed 50")
    url = (
        "https://derpibooru.org/search.json"
        f"?key={_USER_KEY}"
        "&q=(safe+||+!safe)"
        f"&sf=random%3A{seed}"
        f"&sd=desc&page={page}"
    )

    response = thread_local_session().get(url)

    try:
        response.raise_for_status()
    except Exception as e:
        raise SearchFetchFailed(
            "random images fetch failed: {}".format(
                e
            )
        )
    return response.json()['search']


def iter_random_images(seed, per_page, page, executor, batch_size=1):
    last_batches = []
    while True:
        futures = []
        for _ in range(batch_size):
            future = executor.submit(get_random_images, seed, per_page, page)
            futures.append(future)
            page += 1
        if len(last_batches) > 0:
            yield (page-1-batch_size, last_batches)
        last_batches = [item
                        for future in futures
                        for item in future.result()]



def download_image(url, path):
    local_session = thread_local_session()
    r = local_session.get(url, stream=True)
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


def fetch_image(image_id, image_metadata=None, scaling="large",
                overwite=False, overwitemeta=False):
    # figure out where to put the data
    directory = build_path(image_id, prefix=_DATA_PATH + "/images")

    # build glob paths with what we know
    imageglob = "{}/{}.*".format(
        directory, scaling
    )
    metadatapath = "{}/metadata.json".format(directory)

    # check if both the scaling and metadata is already downloaded
    imagefiles = glob.glob(imageglob)
    assert len(imagefiles) < 2
    
    if ((len(imagefiles) == 1 and not overwite)
        and (os.path.exists(metadatapath) and not overwitemeta)):
        logging.debug(f"{imagefiles[0]} and {metadatapath} already exist")
        return

    # fetch image metadata and build image
    if image_metadata is None:
        image_metadata = get_image_metadata(image_id)

    if isinstance(image_metadata, Image):
        # might have been passed an Image directly
        image = image_metadata
    else:
        image = Image(image_metadata)

    # get image URL for desired scale
    try:
        url = image.representations[scaling]
    except KeyError:
        raise RepresentationMissing

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
    if not os.path.exists(metadatapath) or overwitemeta:
        with open(metadatapath, "w") as f:
            json.dump(image.data, f)

    # download the file unless it exists
    # and overwrite isn't set
    if not os.path.exists(imagepath) or overwite:
        download_image(url, imagepath)


def build_path(image_id, prefix=""):
    folder1 = image_id % 10000
    folder2 = (image_id - folder1) // 10000
    return "{}/{:04d}/{:04d}".format(prefix, folder1, folder2)


def fetch_image_instrumented(image):
    image_metadata = None
    # accept images passed in as a metadata object
    if isinstance(image, dict):
        image, image_metadata = (image['id'], image)

    print_before(f"starting {image}",
        print_exc,
        rate_limit, 1.0, 8,
        print_success, f"finished {image}",
        fetch_image, image, image_metadata,
    )


# May be slightly slower than fetch_images_parallel, but more debuggable.
def fetch_images_sequential(images):
    for image in images:
        fetch_image_instrumented(image)


# May or may not be faster than fetch_images_sequential (with Session reuse).
# When it is, we'll take what we can get.
def fetch_images_parallel(images, executor=None):
    if executor is None:
        with ProcessPoolExecutor(max_workers=6) as executor:
            executor.map(fetch_image_instrumented, images)
    else:
        executor.map(fetch_image_instrumented, images)
    

def persist_page(page):
    Path(_DATA_PATH).mkdir(parents=True, exist_ok=True)
    with open(_DATA_PATH+'/current_page.txt', 'w') as f:
        f.write('%d' % page)


def get_persisted_page(default=1):

    try:
        with open(_DATA_PATH+'/current_page.txt', 'r') as f:
            return int(f.read())
    except (IOError, FileNotFoundError):
        return default


def main():
    setup_logging()

    start_page = get_persisted_page()
    fetch_counter = 0
    total_fetch_counter = 0

    page = start_page
    with ProcessPoolExecutor(max_workers=8) as executor:
        image_iterator = iter_random_images(
            seed=209384,
            per_page=50,
            page=page,
            executor=executor,
            batch_size=2,
        )
        try:
            for (page, batch) in image_iterator:
                persist_page(page)
                fetch_images_parallel(batch, executor=executor)
                total_fetch_counter += len(batch)
                fetch_counter += len(batch)
                if fetch_counter >= 1000:
                    logging.info(f"fetched {fetch_counter} since last, page {page}")
                    fetch_counter = 0
                    if free_space_left(_DATA_PATH) < 10_000*1024*1024:
                        logging.warn(f"only {free_space_left(_DATA_PATH)//(1024*1024)}MB left, stopping")
                        break
        except KeyboardInterrupt:
            logging.info("Got ctrl-c, stopping")
    logging.info(f"fetched {total_fetch_counter}, on page {page}")


if __name__ == "__main__":
    main()