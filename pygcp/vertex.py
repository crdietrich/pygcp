"""Vertex AI on Google Cloud Platform.

Copyright (c) 2025 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""

import time
import uuid

import numpy as np
import pandas as pd

from vertexai.language_models import TextEmbeddingModel, ChatModel, InputOutputTextPair


class Embedding:
    """TODO Class docstring."""

    def __init__(self, model_version="textembedding-gecko@001"):
        """TODO method docstring."""
        self.model_version = model_version
        self.api_request_limit = 5
        self.model = TextEmbeddingModel.from_pretrained(self.model_version)
        self.size = len(self.embed_string("dummy data"))
        self.api_retry_limit = 5  # counts
        self.api_retry_delay = 5  # seconds
        self.api_backoff_factor = 2  # base of exponential backoff multiplier
        self.verbose = False

    def embed_string(self, input_string) -> list:
        """Run one embedding.

        Parameters
        ----------
        input_string : str
            Text to generate embeddings from.

        Returns
        -------
        list of float
            Embedding representation of the string.
        """
        embed = self.get_embeddings_with_backoff(input_string, input_kind="str")
        return embed[0].values

    def _sequence_old(self, input_seq) -> list:
        """Create text embeddings for an iterable sequence of strings.

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
        """TODO method docstring."""
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
        """Create text embeddings for an iterable sequence of strings.

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
                # TODO: assign precise Exception
                pass
        embed = [_e.values for _e in embed]
        return embed

    def sequence(self, input_seq) -> list:
        """Create text embeddings for an iterable sequence of strings.

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


def timestamp():
    """Create a string timestamp in the UTC timezone."""
    return str(pd.Timestamp("now", tz="UTC"))


class TextChat:
    """LLM text chat session."""

    def __init__(
        self,
        model_name="chat-bison",
        temperature=0.2,
        max_output_tokens=256,
        top_p=0.95,
        top_k=40,
    ):
        """TODO: init docstring."""
        self.model_name = model_name
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.top_p = top_p
        self.top_k = top_k
        self.chat_model = None
        self.session = None
        self.uuid = None
        self.session_log = []

    @property
    def model_parameters(self):
        """Parameter kwarg wrapper."""
        return {
            "temperature": self.temperature,
            "max_output_tokens": self.max_output_tokens,
            "top_p": self.top_p,
            "top_k": self.top_k,
        }

    def reset(self):
        """Reset the chat model session and generate a new session uuid."""
        self.chat_model = ChatModel.from_pretrained(self.model_name)
        self.uuid = str(uuid.uuid1())

    def start(self, context, examples=None):
        """Start a chat session.

        Parameters
        ----------
        context : str
            All context to provide the model in the initial prompt
        examples : list of lists of str, optional
            Example input and output text. Formatted as:
            [['first input', 'first output'],
            ['second input', 'second output']]
        """
        self.reset()
        self.session_log.append(
            {"timestamp": timestamp(), "uuid": self.uuid, "reset": True}
        )
        self.session_log.append(
            {"timestamp": timestamp(), "uuid": self.uuid, "context": context}
        )
        if examples:
            self.session_log.append(
                {"timestamp": timestamp(), "uuid": self.uuid, "examples": examples}
            )

        def itp(it, ot):
            return InputOutputTextPair(input_text=it, output_text=ot)

        if examples is not None:
            examples = [itp(i, o) for i, o in examples]
        self.session = self.chat_model.start_chat(context=context, examples=examples)

    def request(self, question):
        """Request an answer to a text question.

        Parameters
        ----------
        question : str
            Question to prompt the LLM with

        Returns
        -------
        str
            Response from the LLM
        """
        self.session_log.append(
            {"timestamp": timestamp(), "uuid": self.uuid, "request": question}
        )
        response = self.session.send_message(question, **self.model_parameters)
        response_text = response.text.strip()
        if len(response.errors) == 0:
            response_errors = None
        else:
            response_errors = response.errors
        self.session_log.append(
            {
                "timestamp": timestamp(),
                "uuid": self.uuid,
                "response_text": response_text,
                "response_safety_attributes": response.safety_attributes,
                "response_is_blocked": response.is_blocked,
                "response_errors": response_errors,
            }
        )
        return response_text
