import time
import os
import json
from pprint import pprint
from dotenv import load_dotenv
from elasticsearch import Elasticsearch


load_dotenv()


class Search:
    """
    A class to interact with Elasticsearch for managing indices and documents.

    Attributes
    ----------
    es : Elasticsearch
        The Elasticsearch client instance.
    """

    def __init__(self):
        """
        Initializes the Search class with an Elasticsearch client using
        environment variables for the cloud ID and API key. Prints connection
        information on successful connection.
        """
        self.es = Elasticsearch(cloud_id=os.environ['ELASTIC_CLOUD_ID'],
                                api_key=os.environ['ELASTIC_API_KEY'])
        client_info = self.es.info()
        print('Connected to Elasticsearch!')
        pprint(client_info.body)
