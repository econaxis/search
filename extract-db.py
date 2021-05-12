import dbm
import json
import os
import sys

for s in sys.argv[1:]:
    d = dbm.open(s)

    for k in d.keys():

        try:
            data = json.loads(d[k])

            if(not data["title"] or not data["plain_text"]): continue

            path = "/mnt/henry-80q7/.cache/data-files/" + data["title"].replace("/", "-")
            if os.path.exists(path): continue
            print(path)

            str = [x["text"] for x in data["plain_text"]]
            open(path, 'w').write("".join(str))
        except Exception as e:
            print(e)
