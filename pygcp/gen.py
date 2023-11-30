"""Generative AI for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.

Requires GOOGLE_APPLICATION_CREDENTIALS be set in your environment
"""


import vertexai

from vertexai.preview.language_models import TextGenerationModel


def number_2_word(n):
    """Convert a number between 0 and 99 to a word.
    Based on user dansalmo's reply on Stackoverflow:
    https://stackoverflow.com/questions/19504350/how-to-convert-numbers-to-words-without-using-num2word-library
    """

    num2words = {
        1: "One",
        2: "Two",
        3: "Three",
        4: "Four",
        5: "Five",
        6: "Six",
        7: "Seven",
        8: "Eight",
        9: "Nine",
        10: "Ten",
        11: "Eleven",
        12: "Twelve",
        13: "Thirteen",
        14: "Fourteen",
        15: "Fifteen",
        16: "Sixteen",
        17: "Seventeen",
        18: "Eighteen",
        19: "Nineteen",
        20: "Twenty",
        30: "Thirty",
        40: "Forty",
        50: "Fifty",
        60: "Sixty",
        70: "Seventy",
        80: "Eighty",
        90: "Ninety",
        0: "Zero",
    }

    try:
        return num2words[n]
    except KeyError:
        try:
            return num2words[n - n % 10] + num2words[n % 10].lower()
        except KeyError:
            return None


class GenAIBase:
    def __init__(self, version=1):
        self.project_id = None
        self.location_id = None
        self.__version__ = version

    def vertex_init(self, project_id, location_id):
        self.project_id = project_id
        self.location_id = location_id
        vertexai.init(project=project_id, location=location_id)


class GenAITextSummary(GenAIBase):
    def __init__(self):
        """Generative AI Summary

        Attributes
        ----------
        project_id : str, GCP project ID to host model prediction
        location : str, GCP location to run the model prediction
        temperature : float, randomness of predictions. Range of 0.0 and 1.0.
        max_output_tokens : int, maximum amount of text output in tokens
        top_p : float, cumulative probability of highest probability vocabulary tokens to keep for nucleus sampling. Range of 0.0 to 1.0.
        top_k : int, The number of highest probability vocabulary tokens to keep for top-k-filtering.
        """
        super().__init__()

        self.temperature = 0.85
        self.max_output = 256
        self.top_p = 0.95
        self.top_k = 40

        self.model = TextGenerationModel.from_pretrained("text-bison@001")

        self.text_input = None
        self.text_response = None

    def text_summary(self, text, sentences=2):
        """Summarize text in a specific number of sentences

        Parameters
        ----------
        text : str, text to summarize
        sentences : int, number of sentences in the output

        Returns
        -------
        str, summarized text
        """

        word_n = number_2_word(sentences)

        self.text_input = f"""Provide a summary with about {word_n} sentences for the following:
        {text}
        Summary:
        """

        response = self.model.predict(
            self.text_input,
            temperature=self.temperature,
            max_output_tokens=self.max_output,
            top_p=self.top_p,
            top_k=self.top_k,
        )

        self.text_response = response.text
        return self.text_response

    def text_df(self, _df, column, join_key, sentences=2):
        """Summarize text in a specific column of a Pandas Dataframe to
        a specific number of sentences

        Parameters
        ----------

        Returns
        -------
        Pandas DataFrame, input dataframe with 'summary' column added
        """
        pass
