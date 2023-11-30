"""Vertex AI on Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""

import time

import numpy as np
from vertexai.language_models import TextEmbeddingModel


class Embedding:
    def __init__(self, model_version="textembedding-gecko@001"):
        self.model_version = model_version
        self.api_request_limit = 5
        self.model = TextEmbeddingModel.from_pretrained(self.model_version)
        self.size = len(self.string("dummy data"))
        self.api_retry_limit = 5  # counts
        self.api_retry_delay = 5  # seconds
        self.api_backoff_factor = 2  # base of exponential backoff multiplier
        self.verbose = False

    def string(self, input_string) -> list:
        """Text embedding with the textembedding-gecko@001 LLM

        Parameters
        ----------
        input_string : str, text to be embedded

        Returns
        -------
        list of float, representing the embeddings
        """
        embed = self.get_embeddings_with_backoff(input_string, input_kind="str")
        return embed[0].values

    def _sequence_old(self, input_seq) -> list:
        """Create text embeddings for an iterable sequence of strings

        Parameters
        ----------
        input_seq : sequence of str, text to be embedded

        Returns
        -------
        list of float, representing the embeddings. The number
            of embeddings returned is based on what pretrained model
            was specified when the class was instanced.
        """
        embed = self.model.get_embeddings(input_seq)
        embed = [_e.values for _e in embed]
        return embed

    def get_embeddings_with_backoff(self, input_data, input_kind="list", verbose=False):
        """ """
        if input_kind == "str":
            input_data = [input_data]

        for _r in range(self.api_retry_limit):
            try:
                return self.model.get_embeddings(input_data)
            except Exception as e:
                wait = self.api_retry_delay * (self.api_backoff_factor**_r)
                if verbose:
                    print(f"Error: {e}. Waiting {wait} seconds to retry...")
                time.sleep(wait)
        return

    def _sequence(self, input_seq) -> list:
        """Create text embeddings for an iterable sequence of strings

        Parameters
        ----------
        input_seq : sequence of str, text to be embedded

        Returns
        -------
        list of float, representing the embeddings. The number
            of embeddings returned is based on what pretrained model
            was specified when the class was instanced.
        """
        embed = None
        for _arl in range(self.api_retry_limit):
            try:
                embed = self.model.get_embeddings(input_seq)
            except:
                pass
        embed = [_e.values for _e in embed]
        return embed

    def sequence(self, input_seq) -> list:
        """Create text embeddings for an iterable sequence of strings

        Parameters
        ----------
        input_seq : sequence of str, text to be embedded

        Returns
        -------
        numpy array of float, representing the embeddings for each item
            in input_seq. The first dimension will be the length of the
            input sequence, the second dimension will be the number
            of embeddings based on the pretrained model specified when
            the class was instanced.
        """
        n = len(input_seq)
        if n <= self.api_request_limit:
            return self.get_embeddings_with_backoff(input_seq, input_kind="list")
        else:
            a = np.zeros([n, self.size])
            for i in range(0, n, self.api_request_limit):
                ix1 = i
                ix2 = i + self.api_request_limit
                subset_seq = input_seq[ix1:ix2]
                embed = self.get_embeddings_with_backoff(subset_seq, input_kind="list")
                if embed is None:
                    return a  # might not be a good idea?
                for iy, j in enumerate(range(ix1, ix1 + len(subset_seq))):
                    a[j] = embed[iy]
        return a
