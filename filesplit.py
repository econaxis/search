
import requests

import zipfile
import io

arr = [
"ipa210527.zip",
"ipa210520.zip",
"ipa210513.zip",
"ipa210506.zip",
"ipa210429.zip",
"ipa210422.zip",
"ipa210415.zip",
"ipa210408.zip",
"ipa210401.zip",
"ipa210325.zip",
"ipa210318.zip",
"ipa210311.zip",
"ipa210304.zip",
"ipa210225.zip",
"ipa210218.zip",
"ipa210211.zip",
"ipa210204.zip",
"ipa210128.zip",
"ipa210121.zip",
"ipa210114.zip",
"ipa210107.zip"]

for i in arr:
    print("Extracting ", i)
    url = f"https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/2021/{i}"
    r = requests.get(url, stream = True)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("i")