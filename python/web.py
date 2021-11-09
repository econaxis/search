from table_manager import tbm


def test_flask():
    from flask import Flask
    from flask_cors import CORS, cross_origin
    from search_lib import Searcher
    from flask_compress import Compress

    with Searcher("par-index") as searcher:
        app = Flask(__name__)
        Compress(app)
        app.config['CORS_HEADERS'] = 'Content-Type'

        @app.route("/<query>")
        @cross_origin()
        def run(query: str):
            if len(query) == 0:
                return None
            query = query.upper().split('+')
            print(query)
            result = searcher.search_terms(*query).printable()
            return result

        @app.route("/id/<int:id>")
        @cross_origin()
        def get_id(id: int):
            return tbm.get(id)[1]

        app.run(threaded=False)