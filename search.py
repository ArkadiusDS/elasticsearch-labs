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

    def create_index(self):
        """
        Deletes the 'my_documents' index if it exists and creates a new one.
        """
        self.es.indices.delete(index='my_documents', ignore_unavailable=True)
        self.es.indices.create(index='my_documents')

    def insert_documents(self, documents):
        """
        Inserts a list of documents into the 'my_documents' index in bulk.

        Parameters
        ----------
        documents : list
            A list of documents (dictionaries) to be indexed.

        Returns
        -------
        dict
            The response from the bulk insertion operation.
        """
        operations = []
        for document in documents:
            operations.append({'index': {'_index': 'my_documents'}})
            operations.append(document)
        return self.es.bulk(operations=operations)

    def search(self, **query_args):
        return self.es.search(index='my_documents', **query_args)
