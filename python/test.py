import requests, concurrent.futures
import json

from web import test_flask
from query_console import QueryConsole
from search_lib import ParallelIndexer
from table_manager import TableManager


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


def load_wikibooks_pages():
    tbm = TableManager(b"/tmp/test.db")
    with ParallelIndexer() as ind:
        for index in range(1, 86736):
            url, contents = tbm.get(index)
            ind.append_file(contents, index)
        ind.end()


def load_wikibooks_pages_tbm():
    import sqlite3
    con = sqlite3.connect("f.db")
    cur = con.cursor()

    cur.execute("SELECT COUNT(rowid) FROM en")
    count = cur.fetchone()
    print(count, "rows")
    tbm = TableManager()
    for row in cur.execute("SELECT rowid,url, body_text FROM en ORDER BY rowid"):
        tbm.store(row[0], row[1], row[2])
    tbm.flush()


def test_live():
    from search_lib import Searcher
    console = QueryConsole()
    with Searcher("par-index") as searcher:
        while console.valid:
            query = console.run_event_loop()
            if query:
                result = searcher.search_terms(*query).printable()
                console.set_results(result)


# load_wikibooks_pages_tbm()
# load_wikibooks_pages()
# test_live()
test_flask()
print("done")
# try_search()
