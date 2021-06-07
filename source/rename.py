import sys
import hashlib
import os

def ren(prefix, old, new):
    try:
        os.rename(prefix + "-" + old, prefix + "-" + new)
    except Exception as e:
        print(e)


f = open("index_files")

for line in f.readlines():
    line = line.rstrip()
    hashs = hashlib.md5(line.encode("ascii")).hexdigest()[0:5]
    ren("positions", line, hashs)
    ren("filemap", line, hashs)
    ren("terms", line, hashs)
    ren("frequencies", line, hashs)