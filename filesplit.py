import multiprocessing

import requests

import zipfile
import io

from lxml import etree

import os
import time
import hashlib
from multiprocessing import Pool

os.chdir("/mnt/extra/uspto/data")


def download(i):
    print("Extracting ", i)
    url = f"https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/2018/{i}"
    r = requests.get(url, stream=True)
    if not r.ok:
        print(r)
        time.sleep(10)
        return download(i)

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

                if not os.path.isfile(path + xmlfile):
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
    urls = ["ipa181227.zip", "ipa181220.zip", "ipa181213.zip", "ipa181206.zip", "ipa181129.zip", "ipa181122.zip", "ipa181115.zip", "ipa181108.zip", "ipa181101.zip", "ipa181025.zip", "ipa181018.zip", "ipa181011.zip", "ipa181004.zip", "ipa180927.zip", "ipa180920.zip", "ipa180913.zip", "ipa180906.zip", "ipa180830.zip", "ipa180823.zip", "ipa180816.zip", "ipa180809.zip", "ipa180802.zip", "ipa180726.zip", "ipa180719.zip", "ipa180712.zip", "ipa180705.zip", "ipa180628.zip", "ipa180621.zip", "ipa180614.zip", "ipa180607.zip", "ipa180531.zip", "ipa180524.zip", "ipa180517.zip", "ipa180510.zip", "ipa180503.zip", "ipa180426.zip", "ipa180419.zip", "ipa180412.zip", "ipa180405.zip", "ipa180329.zip", "ipa180322.zip", "ipa180315.zip", "ipa180308.zip", "ipa180301.zip", "ipa180222.zip", "ipa180215.zip", "ipa180208.zip", "ipa180201.zip", "ipa180125.zip", "ipa180118.zip", "ipa180111.zip", "ipa180104.zip"]
    pool.map(download, urls)


_process()
