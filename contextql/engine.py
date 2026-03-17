import duckdb

class ContextQLEngine:
    def __init__(self, db_path=":memory:"):
        self.conn = duckdb.connect(db_path)
        self.contexts = {}

    def create_context(self, name, query, key):
        self.contexts[name] = {
            "query": query,
            "key": key
        }

    def resolve_context(self, name):
        ctx = self.contexts[name]
        result = self.conn.execute(ctx["query"]).fetchall()
        return set(row[0] for row in result)

    def execute(self, query):
        # VERY minimal proof-of-concept
        if "CONTEXT IN" in query:
            raise NotImplementedError("Context execution not yet implemented")
        return self.conn.execute(query).fetchdf()