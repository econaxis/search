import multiprocessing

import requests

import zipfile
import io

from lxml import etree

import os
import hashlib
from multiprocessing import Pool


def download(i):
    print("Extracting ", i)
    url = f"https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/2019/{i}"
    r = requests.get(url, stream=True)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(".")


def process(filename):
    if filename.find("PROCESSED") != -1:
        return

    try:
        xfile = open(filename)
        f = xfile.read()
    except Exception as e:
        print(e)
        return
    parser = etree.XMLParser(recover=True)

    startpos = 0

    while True:
        try:
            endpos = f.find("<?xml", startpos + 1)
            if endpos == -1:
                break

            elem = etree.fromstring(f[startpos:endpos].encode("ascii"), parser)

            xmlfile = elem.get("file")

            if xmlfile:
                digest = hashlib.md5(xmlfile.encode("ascii")).hexdigest()

                path = digest[0:2] + "/" + digest[2:4] + "/"

                os.makedirs(path, exist_ok=True)

                print(xmlfile)

                if not os.path.isfile(path+xmlfile):
                    file = open(path + xmlfile, "w")
                    file.write(f[startpos:endpos])

            startpos = endpos
        except Exception as e:
            print(e)
        finally:
            startpos = endpos

    os.rename(filename, filename + "PROCESSED")


pool = multiprocessing.Pool(processes=3)


def _process():
    pool.map(process, [x for x in os.listdir('.') if os.path.isfile(x)])


def _download():
    urls = ["ipa191226.zip", "ipa191219.zip", "ipa191212.zip", "ipa191205.zip", "ipa191128.zip", "ipa191121.zip", "ipa191114.zip", "ipa191107.zip", "ipa191031.zip", "ipa191024.zip", "ipa191017.zip", "ipa191010.zip", "ipa191003.zip", "ipa190926.zip", "ipa190919.zip", "ipa190912.zip", "ipa190905.zip", "ipa190829.zip", "ipa190822_r1.zip", "ipa190822_r2.zip", "ipa190822.zip", "ipa190815.zip", "ipa190815_r2.zip", "ipa190815_r1.zip", "ipa190808_r1.zip", "ipa190808.zip", "ipa190801.zip", "ipa190801_r1.zip", "ipa190725.zip", "ipa190718.zip", "ipa190711.zip", "ipa190704.zip", "ipa190627.zip", "ipa190620.zip", "ipa190613.zip", "ipa190606.zip", "ipa190530.zip", "ipa190523.zip", "ipa190516.zip", "ipa190509.zip", "ipa190502.zip", "ipa190425.zip", "ipa190418.zip", "ipa190411.zip", "ipa190404.zip", "ipa190328.zip", "ipa190321.zip", "ipa190314.zip", "ipa190307.zip", "ipa190228.zip", "ipa190221.zip", "ipa190214.zip", "ipa190207.zip", "ipa190131.zip", "ipa190124.zip", "ipa190117.zip", "ipa190110.zip", "ipa190103.zip"]
    pool.map(download, urls)

_process()

