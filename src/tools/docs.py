from utils.rag_search_api import RAGSearchAPIClient

class Docs:
    def __init__(self):
        self.search_client = RAGSearchAPIClient()

    def query(self, query: str) -> list[str]:
        return self.search_client.search(query)