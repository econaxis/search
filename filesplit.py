import multiprocessing

import requests

import zipfile
import io

from lxml import etree

import os
import hashlib
from multiprocessing import Pool


def download():
    for i in arr:
        print("Extracting ", i)
        url = f"https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/2020/{i}"
        r = requests.get(url, stream=True)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(".")


def process(filename):
    if filename.find("PROCESSED") != -1:
        return

    try:
        f = open(filename).read()
    except Exception as e:
        print(e)
        return
    parser = etree.XMLParser(recover=True)

    startpos = 0

    while True:
        endpos = f.find("<?xml", startpos + 1)
        if endpos == -1:
            break

        elem = etree.fromstring(f[startpos:endpos].encode("ascii"), parser)
        filename = elem.get("file")

        if filename:
            digest = hashlib.md5(filename.encode("ascii")).hexdigest()

            path = digest[0:2] + "/" + digest[2:4] + "/"

            os.makedirs(path, exist_ok=True)

            print(filename)

            file = open(path + filename, "w")
            file.write(f[startpos:endpos])
            a = 5
        startpos = endpos
    os.rename(filename, filename + "PROCESSED")


pool = multiprocessing.Pool(processes=4)


def _process():
    pool.map(process, [x for x in os.listdir('.') if os.path.isfile(x)])


def _download():
    urls = ["ipa201231.zip", "ipa201224.zip", "ipa201217.zip", "ipa201210.zip", "ipa201203.zip", "ipa201126.zip", "ipa201119.zip", "ipa201112.zip", "ipa201105.zip", "ipa201029.zip", "ipa201022.zip", "ipa201015.zip", "ipa201008.zip", "ipa201001.zip", "ipa200924.zip", "ipa200917.zip", "ipa200910.zip", "ipa200903.zip", "ipa200827.zip", "ipa200820.zip", "ipa200813.zip", "ipa200806.zip", "ipa200730.zip", "ipa200723.zip", "ipa200716.zip", "ipa200709.zip", "ipa200702.zip", "ipa200625.zip", "ipa200618.zip", "ipa200611.zip", "ipa200604.zip", "ipa200528.zip", "ipa200521.zip", "ipa200514.zip", "ipa200507.zip", "ipa200430.zip", "ipa200423.zip", "ipa200416.zip", "ipa200409.zip", "ipa200402.zip", "ipa200326.zip", "ipa200319.zip", "ipa200312.zip", "ipa200305.zip", "ipa200227.zip", "ipa200220.zip", "ipa200213.zip", "ipa200206.zip", "ipa200130.zip", "ipa200123.zip", "ipa200116.zip", "ipa200109.zip", "ipa200102.zip",]
    pool.map(download, urls)

_download()