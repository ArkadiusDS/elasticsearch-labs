import time
import os
import json
from pprint import pprint
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

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
        self.model = SentenceTransformer('intfloat/multilingual-e5-small')
        self.es = Elasticsearch(cloud_id=os.environ['ELASTIC_CLOUD_ID'],
                                api_key=os.environ['ELASTIC_API_KEY'])
        client_info = self.es.info()
        print('Connected to Elasticsearch!')
        pprint(client_info.body)

    def create_index(self):
        self.es.indices.delete(index='my_documents', ignore_unavailable=True)
        self.es.indices.create(index='my_documents', mappings={
            'properties': {
                'embedding': {
                    'type': 'dense_vector',
                }
            }
        })

    def get_embedding(self, text):
        return self.model.encode(text)

    def insert_document(self, document):
        return self.es.index(index='my_documents', document={
            **document,
            'embedding': self.get_embedding(document['summary']),
        })

    def insert_documents(self, documents):
        operations = []
        for document in documents:
            operations.append({'index': {'_index': 'my_documents'}})
            operations.append({
                **document,
                'embedding': self.get_embedding(document['summary']),
            })
        return self.es.bulk(operations=operations)

    def reindex(self):
        """
        Recreates the 'my_documents' index and inserts documents from 'data.json'.

        Returns
        -------
        dict
            The response from the bulk insertion operation.
        """
        self.create_index()
        with open('data.json', 'rt') as f:
            documents = json.loads(f.read())
        return self.insert_documents(documents)

    def search(self, **query_args):
        """
        Searches for documents in the 'my_documents' index using the provided query arguments.

        Parameters
        ----------
        **query_args : dict
            Arbitrary keyword arguments to be passed as the search query parameters.
            These can include various Elasticsearch search options like 'query',
            'from', 'size', etc.

        Returns
        -------
        dict
            The response from the search operation, containing search hits and metadata.
        """
        return self.es.search(index='my_documents', **query_args)

    def retrieve_document(self, id):
        """
        Retrieves a document from the 'my_documents' index by its ID.

        Parameters
        ----------
        id : str
            The ID of the document to retrieve.

        Returns
        -------
        dict
            The response from the get operation, containing the document data.
        """
        return self.es.get(index='my_documents', id=id)
