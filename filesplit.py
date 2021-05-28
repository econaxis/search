from os import listdir, chdir, mkdir
import os
from os.path import isfile, join
from sys import argv
import re
from urllib.parse import unquote

chdir(argv[1])

files = (f for f in listdir(argv[1]) if isfile(join(argv[1], f)))
madedir = {}
madedir2 = {}
i = 0

for f in files:
    ff = unquote(f)
    ff = re.sub('[._\-\[\],\.1-9]', '', ff)
    ff = ff.upper()

    i += 1
    if i % 100 == 0:
        print(i * 100 / 3e6)

    dir1 = ff[0:2]

    if len(f) > 4:
        dir2 = dir1 + "/" + ff[2:4]
    else:
        dir2 = dir1 + "/this"

    realdir = f"dir_{dir2}"
    realdir = realdir

    if realdir not in madedir2 and not os.path.exists(realdir):
        os.makedirs(realdir)
        madedir2[realdir] = True

    os.rename(f, f"{realdir}/{f}")
