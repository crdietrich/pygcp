"""Natural Language Processing for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""


import time

import pandas as pd
from google.cloud import language_v1 as gcp_language


class NLP:
    def __init__(self, gcp_credentials=None):
        """Initialize the GCP Natural Language Service.
        Assumes the environmental variable 
        GOOGLE_APPLICATION_CREDENTIALS is set in execution
        environment.
        """
        self.client = gcp_language.LanguageServiceClient(credentials=gcp_credentials)
        self.language = "en"  # auto if not specified
        self.encoding = gcp_language.EncodingType.UTF8  # or None, UTF16, UTF32

        self.api_quota = 600
        self.api_request_count = 0

    @staticmethod
    def _request(text):
        """Create an NLP request dictionary
        
        Parameters
        ----------
        text : str, text to transmit to API
        
        Returns
        -------
        dict : request parameters and text
        """
        if pd.isnull(text):
            return None
        if isinstance(text, bytes):
            text = text.decode("utf-8")
        type_ = gcp_language.Document.Type.PLAIN_TEXT
        document = {"type_": type_, "content": text}
        request={"document": document}
        return request
    
    def sentiment(self, text):
        """Analyze text for sentiment

        Notes
        -----
        From the example: https://cloud.google.com/natural-language/docs/analyzing-sentiment

        Parameters
        ----------
        text : str, text to analyze

        Returns
        -------
        sentiment_score : float
        sentiment_magnitude : float
        """
        request = self._request(text)
        request['encoding_type'] = self.encoding
        response = self.client.analyze_sentiment(request)
        return response.document_sentiment.score, response.document_sentiment.magnitude
    
    def sentiment_df(self, _df, column_name, prefix=""):
        """Compute the sentiment of a pandas DataFrame column
        
        Parameters
        ----------
        _df : Pandas DataFrame
        column_name : str, column with text to evaluate sentiment
        prefix : str, prefix to add to appended sentiment columns

        Returns
        -------
        Input DataFrame with two added columns:
            sentiment_score : float, DESCRIPTION TODO
            sentiment_magnitude : float, DESCRIPTION TODO
        """
        if prefix != "":
            prefix = prefix + "_"

        def _sentiment(text):
            if pd.isnull(text):
                return pd.NA, pd.NA
            else:
                self.api_request_count += 1
                time.sleep(58.0 / self.api_quota)  # 1 min quota rate with 2s safety
                return self.sentiment(text)
        _dfx = _df.apply(lambda r: _sentiment(r[column_name]), 
                         result_type='expand', axis=1)
        _dfx.columns = [prefix+'sentiment_score', prefix+'sentiment_magnitude']
        _df = pd.concat([_df, _dfx], axis=1)
        return _df
    
    def entities(self, text):
        """Find and describe entities in text
        
        Parameters
        ----------
        text : str, text to analyze

        Returns
        -------
        
        """
        request = self._request(text)
        request['encoding_type'] = self.encoding
        response = self.client.analyze_entities(request)
        return response.entities

    def entity_sentiment(self, text):
        """
        self.client.analyze_entity_sentiment
        """
        request = self._request(text)
        request['encoding_type'] = self.encoding
        response = self.client.analyze_entity_sentiment(request)
        return response
    
    def syntax(self, text):
        """
        self.client.analyze_syntax
        """
        request = self._request(text)
        request['encoding_type'] = self.encoding
        response = self.client.analyze_syntax(request)
        return response
    
    def classify(self, text):
        """
        self.client.classify_text
        """
        request = self._request(text)
        response = self.client.classify_text(request)
        return response
    
    def moderate(self, text):
        """
        self.client.moderate_text
        """
        request = self._request(text)
        response = self.client.moderate_text(request)
        return response
    
    def annotate(self, text):
        """
        self.client.annotate_text
        """
        request = self._request(text)
        features = gcp_language.AnnotateTextRequest.Features(
            extract_syntax=True,
            extract_entities=True,
            extract_document_sentiment=True,
            classify_text=True,
            moderate_text=True
            )
        request = gcp_language.AnnotateTextRequest(document=request['document'],
                                                   features=features)
        response = self.client.annotate_text(request)
        return response
    