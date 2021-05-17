import tarfile
import sys
import os
import signal

datadir = sys.argv[1]

if not datadir:
    raise RuntimeError("Datadir must be specified")

os.chdir(datadir)
tfl = open('total-files-list', 'r')

linesi = 0
curfile = 1
tar = tarfile.open(f'tar-{curfile}.tar', 'a')
tarlist = tar.getnames()

def signal_handler(sig, frame):
    print("closing soon")
    tar.close()
    tfl.close()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


for line in tfl:
    linesi += 1
    line = line[:-1]

    if f'data/{line}' in tarlist:
        continue

    try:
        tar.add(f'data/{line}')
    except Exception as e:
        print(e)


    if linesi > 100000:
        linesi = 0
        curfile += 1
        tar.close()
        tar = tarfile.open(f'tar-{curfile}.tar', 'w')
        tarlist = tar.getnames()

    if linesi % 5000 == 0:
        print(linesi)
