import os
import subprocess
from pathlib import Path

import requests
from google.cloud import storage

SENTINEL_ROOT = os.environ.get("SENTINEL_ROOT", "/var/satellite-data/")


def download_from_googlecloud(client, bucket, blob_prefix, productname, rootdir=SENTINEL_ROOT):
    """ Replacement for gsutil to recursively download from a bucket using the Google Storage Python API"""
    out_folder = Path(rootdir) / productname
    out_folder.mkdir(exist_ok=True)
    blobs = client.list_blobs(bucket, prefix=blob_prefix, delimiter='/')
    for blob in blobs:
        if blob.name.endswith("_$folder$"):
            prefix_new = blob.name[:-len("_$folder$")] + "/"
            productname_new = productname + "/" + blob.name[:-9].split("/")[-1]
            download_from_googlecloud(client, bucket, prefix_new, productname_new, rootdir)
        else:
            filename = blob.name.split("/")[-1]
            filepath = Path(rootdir) / productname / filename
            blob.download_to_filename(filepath)
            return 0


def download_S2_GCS_py(s2_product, credentials, **kwargs):
    """If Sentinel-2 L2A Data has arleady been archived on the sentinel hub, this function
    downloads the data from the Google Cloud Server. SAFE files will be saved in /var/satellite-data/         Uses the Python API
    Requires Google Cloud Storage credentials
    export GOOGLE_APPLICATION_CREDENTIALS="credentials_gs.json"


    Argument in:
    s2_products_df from previous sentinel query"""

    outdir = kwargs.get('outdir', SENTINEL_ROOT)

    date = s2_product.beginposition.to_pydatetime()
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    productname = s2_product.title
    utm = productname[39:41]
    latb = productname[41:42]
    square = productname[42:44]
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials)

#    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "demo4_preprocessing/credentials_gs.json"

    client = storage.Client()
    bucket = client.bucket("gcp-public-data-sentinel-2")
    blob_prefix = "L2/tiles/{}/{}/{}/{}.SAFE/".format(utm, latb, square, productname)
    download_from_googlecloud(client, bucket, blob_prefix, productname, outdir)
    return 0


def download_S2_GCS(s2_product, credentials=None, **kwargs):
    """If Sentinel-2 L2A Data has arleady been archived on the sentinel hub, this function
    downloads the data from the Google Cloud Server. SAFE files will be saved in /var/satellite-data/

    Argument in:
    s2_products_df from previous sentinel query"""

    date = s2_product.beginposition.to_pydatetime()
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    productname = s2_product.title
    utm = productname[39:41]
    latb = productname[41:42]
    square = productname[42:44]

    if credentials is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials)

    # client = storage.Client()
    # bucket = client.bucket("gcp-public-data-sentinel-2")
    # blob_prefix = f"L2/tiles/{utm}/{latb}/{square}/{productname}.SAFE/"

    # download_from_googlecloud(client, bucket, blob_prefix, productname, SENTINEL_ROOT)

    # # tiles/[UTM code]/latitude band/square/productname.SAFE
    outdir = kwargs.get('outdir', SENTINEL_ROOT)

    filepath = Path(outdir) / (productname + ".SAFE")
    if filepath.exists():
        print("S2 already downloaded")
        return 0

    proc_output = subprocess.run(
        [
            "gsutil",
            "-m",
            "cp",
            "-r",
            "gs://gcp-public-data-sentinel-2/L2/tiles/{}/{}/{}/{}.SAFE".format(
                utm, latb, square, productname
            ),
            str(outdir),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
    )
    return proc_output.returncode


def download_S2_AWS(s2_product, **kwargs):
    """If Sentinel-2 L2A Data has arleady been archived on the sentinel hub, this function
    downloads the data from the AWS. SAFE files will be saved in /var/satellite-data/

    Argument in:
    s2_products_df from previous sentinel query
    """

    date = s2_product.beginposition.to_pydatetime()
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    productname = s2_product.title
    utm = productname[39:41]
    latb = productname[41:42]
    square = productname[42:44]
    outdir = kwargs.get('outdir', SENTINEL_ROOT)

    # tiles/[UTM code]/latitude band/square/[year]/[month]/[day]/[sequence]/DATA
    url = "s3://sentinel-s2-l2a/tiles/{}/{}/{}/{}/{}/{}/".format(utm, latb, square, year, month, day)
    filename = str(Path(outdir) / (productname + ".SAFE"))
    process = subprocess.run(
        [
            "aws",
            url,
            filename,
            "--request-payer",
            "requester",
            "--recursive"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def download_S2_sentinelhub(s2_product, **kwargs):
    """If Sentinel-2 L2A Data has arleady been archived on the sentinel hub, this function
    downloads the data from the AWS. SAFE files will be saved in /var/satellite-data/

    Argument in:
    s2_products_df from previous sentinel query
    """

    outdir = kwargs.get('outdir', SENTINEL_ROOT)
    # tiles/[UTM code]/latitude band/square/[year]/[month]/[day]/[sequence]/DATA
    proc_output = subprocess.run(
        [
            "sentinelhub.aws",
            "--product",
            str(s2_product.title),
            "-f",
            str(outdir),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
    )


def download_S1_AWS(s1_product, **kwargs):
    """If Sentinel-1 GRD Data has arleady been archived on the sentinel hub, this function
    downloads the data from the AWS. SAFE files will be saved in /var/satellite-data/

    Argument in:
    s1_products_df from previous sentinel query
    """

    date = s1_product.beginposition.to_pydatetime()
    year = str(date.year)
    month = str(date.month)
    day = str(date.day)
    productname = s1_product.title

    outdir = kwargs.get('outdir', SENTINEL_ROOT)
    # [product type]/[year]/[month]/[day]/[mode]/[polarization]/[product identifier]
    url = "s3://sentinel-s1-l1c/GRD/{}/{}/{}/IW/DV/{}/".format(year, month, day, productname)
    filename = outdir + productname + ".SAFE"
    process = subprocess.run(
        [
            "aws", "s3", "cp",
            url,
            filename,
            "--request-payer",
            "requester",
            "--recursive"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def download_S1_NOAA_py(s1_product, auth=None, **kwargs):
    if auth:
        username = auth["username"]
        password = auth["password"]
        outdir = SENTINEL_ROOT
    else:
        username = input("Earthdata Login Username: ")
        password = getpass.getpass(prompt="Password: ")
        # outputpath = input(
        #     "Where would you like to save the downlaoded data? (e.g /media/raid/satellite-data/ ) "
        # )
    productname = s1_product.title
    producttype = productname[7:10]
    satellite = productname[2]
    outdir = kwargs.get('outdir', SENTINEL_ROOT)

    if producttype == "SLC":
        url = "https://datapool.asf.alaska.edu/{}/S{}/{}.zip"
    else:
        url = "https://datapool.asf.alaska.edu/{}_HD/S{}/{}.zip"
    url = url.format(producttype, satellite, productname)

    err = 0
    p_out = Path(outdir) / (productname + ".zip")
    if p_out.exists():
        print("S1 already downloaded")
        return
    with requests.Session() as s:
        s.auth = (username, password)
        r1 = s.request('get', url)
        r = s.get(r1.url, auth=(username, password))
        if r.ok:
            with p_out.open('wb') as f:
                f.write(r.content)
            f.close()
        else:
            err = -1
        s.close()
    return err


def download_S1_NOAA(s1_product, auth=None, **kwargs):
    """If Sentinel-1 GRD Data has arleady been archived on the sentinel hub, this function
    downloads the data from ASF NASA API. ZIP files will be saved

    Argument in:
    s1_products_df from previous sentinel query
    """

    if auth:
        username = auth["username"]
        password = auth["password"]
        # outputpath = SENTINEL_ROOT
    elif Path("credentials_noaa.json").exists():
        credentials_noaa = json.load(open("credentials_noaa.json"))
        username = credentials_noaa["username"]
        password = credentials_noaa["password"]
    else:
        username = input("Earthdata Login Username: ")
        password = getpass.getpass(prompt="Password: ")
        # outputpath = input(
        #     "Where would you like to save the downlaoded data? (e.g /media/raid/satellite-data/ ) "
        # )

    productname = s1_product.title
    producttype = productname[7:10]
    satellite = productname[2]
    outdir = kwargs.get('outdir', SENTINEL_ROOT)

    if producttype == "SLC":
        url = "https://datapool.asf.alaska.edu/{}/S{}/{}.zip"
    else:
        url = "https://datapool.asf.alaska.edu/{}_HD/S{}/{}.zip"
    url = url.format(producttype, satellite, productname)
    print(url)

    args = [
        "wget",
        "-c",
        "--http-user={}".format(username),
        "--http-password='{}'".format(password),
        url,
        "-P",
        outdir,
        # "--quiet",
    ]
    print(args)
    process = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return process.returncode


