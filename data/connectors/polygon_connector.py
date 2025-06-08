from polygon import RESTClient
import os


class PolygonConnector:
    def __init__(self):
        self.client = RESTClient(os.getenv("POLYGON_API_KEY"))
