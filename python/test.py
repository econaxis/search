from search_lib import *


def basic_test():
    ind = Indexer()
    ind.append_file("fdsa fdsa fdsa hello world testing earth", 182)
    ind.append_file("abcdef jdkidxl fdsaf", 183)
    ind.persist("pppp")

    search = Searcher("pppp")

    assert list(search.search_terms("fdsa").iter_ids()) == [182, 183]
    assert list(search.search_terms("fdsaf").iter_ids()) == [183]
    assert list(search.search_terms("abcdef").iter_ids()) == [183]


basic_test()