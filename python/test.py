import ctypes
import json

from query_console import QueryConsole
from search_lib import ParallelIndexer


def basic_test():
    from search_lib import Indexer, Searcher
    ind = Indexer()
    ind.append_file("fdsa fdsa fdsa hello world testing earth", 182)
    ind.append_file("abcdef jdkidxl fdsaf", 183)
    ind.persist("pppp")

    search = Searcher("pppp")

    assert list(search.search_terms("fdsa").iter_ids()) == [182, 183]
    assert list(search.search_terms("fdsaf").iter_ids()) == [183]
    assert list(search.search_terms("abcdef").iter_ids()) == [183]


def load_bookmarks(data: dict = None) -> [str]:
    if data is None:
        data = json.load(open("bookmarks-2021-11-03.json", "r"))

    result = []
    for j in data:
        if j == "uri":
            result.append(data[j])
        elif type(data[j]) is dict:
            result.extend(load_bookmarks(data[j]))
        elif type(data[j]) is list:
            for i in data[j]:
                result.extend(load_bookmarks(i))

    return result


import requests, concurrent.futures


def download(url):
    try:
        print(url[0])
        return url, requests.get(url[1])
    except Exception as e:
        print(e)
        return url, None


def fetch_pages(url: [str]) -> [str]:
    import sqlite3
    con = sqlite3.connect('output.db')
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS data (id integer, url text, contents blob)')

    url = list(zip(range(1, len(url) + 1), url))

    with concurrent.futures.ProcessPoolExecutor(max_workers=22) as executor:
        for (id, url), response in list(executor.map(download, url)):
            if not response:
                continue
            if response.status_code == 200:
                if len(response.content) < 1e8 and response.headers["Content-Type"].startswith("text/html"):
                    print("Good ", url, id)
                    cur.execute("INSERT INTO data VALUES (?, ?, ?)", (id, url, response.content))
            else:
                print(response)
    con.commit()
    con.close()


def load_pages() -> [dict]:
    import sqlite3
    con = sqlite3.connect('output.db')
    cur = con.cursor()

    size = 5
    ind = ParallelIndexer()
    for row in cur.execute('SELECT * FROM data ORDER BY id LIMIT 100000'):
        ind.append_file(row[2], row[0])
        ind.append_file(row[2], row[0] * 10)
        ind.append_file(row[2], row[0] * 100)
        ind.append_file(row[2], row[0] * 10000)
        ind.append_file(row[2], row[0] * 100000)

    print("Persisting")
    ind.end()


def test_live():
    from search_lib import Searcher
    console = QueryConsole()
    with Searcher("output") as searcher:
        while console.valid:
            query = console.run_event_loop()
            if query:
                with searcher.search_terms(*query) as result:
                    console.set_results(result)
        assert (ctypes.c_uint32.in_dll(searcher.dll, "elems_allocated").value == 0)


load_pages()
# test_live()
# try_search()
