import subprocess
import os

os.chdir(os.environ["DATA_FILES_DIR"] + "/indices")

def ren(prefix, old, new):
    try:
        os.rename(prefix + "-" + old, prefix + "-" + new)
    except Exception as e:
        print(e)


f = open("index_files")

good = []

for line in f.readlines():
    line = line.rstrip()
    good.append("positions-" + line)
    good.append("filemap-" + line)
    good.append("terms-" + line)
    good.append("frequencies-" + line)

total = subprocess.check_output('fd "positions|filemap|terms|frequencies" --max-depth=1', shell = True).decode("ascii")


print("keeping ", good)
for j in total.splitlines():
    if j not in good:
        os.renames(j, "old/" + j)
        print("removing ", j)