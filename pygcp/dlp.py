"""Cloud Digital Loss Protection for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""

import json

import pandas as pd


def utf8len(s):
    """Determine the byte length of a string"""
    return len(s.encode("utf-8"))


class DLPBase:
    def __init__(self, gcp_credentials=None, version=1):
        self.project_id = None
        self.df_info_types = None

        self.client = None
        self.set_version(version, gcp_credentials)

        # attributes that could be checked as properties
        self.dictionary = None
        self.regex = None

    def set_version(self, version, gcp_credentials):
        if version == 1:
            import google.cloud.dlp as gcp_dlp

            self.client = gcp_dlp.DlpServiceClient(credentials=gcp_credentials)

        if version == 2:
            import google.cloud.dlp_v2 as gcp_dlp

            self.client = gcp_dlp.DlpServiceClient(credentials=gcp_credentials)

    def get_info_types(self, language_code=None, result_filter=None):
        """Get info types available on the DLP API

        Parameters
        ----------
        language_code: str, The BCP-47 language code to use, e.g. 'en-US'.
        result_filter: XXX, An optional filter to only return info types supported by
            certain parts of the API. Defaults to "supported_by=INSPECT".

        Returns
        -------
        proto.marshal.collections.repeated.RepeatedComposite
        """

        response = self.client.list_info_types(
            request={"parent": language_code, "filter": result_filter}
        )
        return response.info_types

    @staticmethod
    def info_type_description_flatten(itd):
        """Flatten a InfoTypeDescription class into a tuple

        Note: name: "CROATIA_PERSONAL_ID_NUMBER" has location_category: 42
        """

        def category_extractor(info_type):
            cit = info_type.categories
            output = {}
            for c in cit:
                c = str(c).split(":")
                output[c[0].strip()] = c[1].strip()
            return output

        cat = category_extractor(itd)

        try:
            ic = cat["industry_category"]
        except KeyError:
            ic = None

        try:
            lc = cat["location_category"]
        except KeyError:
            lc = None

        try:
            tc = cat["type_category"]
        except KeyError:
            tc = None

        return (
            itd.name,
            itd.display_name,
            itd.description,
            ic,  # itd.categories
            lc,  # itd.categories
            tc,  # itd.categories
            # itd.supported_by,
            # itd.versions
        )

    def info_types_df(self, contains=None, language_code=None, result_filter=None):
        _it = self.get_info_types(
            language_code=language_code, result_filter=result_filter
        )
        _df = pd.DataFrame(
            [self.info_type_description_flatten(itd) for itd in _it],
            columns=[
                "api_string",
                "display_name",
                "description",
                "industry_category",
                "location_category",
                "type_category",
            ],
        )
        if contains is not None:
            contains = contains.lower()
            display_name_mask = _df.display_name.str.lower().str.contains(contains)
            description_mask = _df.description.str.lower().str.contains(contains)
            _df = _df.loc[display_name_mask | description_mask]
        return _df

    def info_type_builder(
        self,
        info_types=None,
        exclude_types=None,
        industry_category=None,
        location_category=None,
        type_category=None,
    ):
        self.df_info_types = self.info_types_df()
        _df = self.df_info_types

        if location_category is None:
            location_category = ["GLOBAL"]

        if info_types is None:
            if exclude_types is not None:
                _df = _df.loc[~_df.type_category.isin(exclude_types)]
            if industry_category is not None:
                _df = _df.loc[_df.industry_category.isin(industry_category)]
            if location_category is not None:
                _df = _df.loc[_df.location_category.isin(location_category)]
            if type_category is not None:
                _df = _df.loc[_df.type_category.isin(type_category)]

            info_types = [{"name": it} for it in _df["api_string"]]
        else:
            info_types = [{"name": it} for it in info_types]

        return info_types

    @staticmethod
    def custom_regex_builder(pattern_dict):
        """Create custom regex inputs

        Parameters
        ----------
        pattern_dict : dict, with
            keys : str, name for custom regex pattern
            values : str, regular expression for custom info type

        Returns
        -------
        list of dict, with each item:
            {'info_type': {'name': key}, 'regex': {'pattern': value}}
        """
        output = []
        for k, v in pattern_dict:
            output.append({"info_type": {"name": k}, "regex": {"pattern": v}})
        return output

    @staticmethod
    def custom_dictionary_builder(pattern_dict):
        """Create custom regex inputs

        Parameters
        ----------
        pattern_dict : dict, with
            keys : str, name for custom dictionary
            values : list of str, with each item a word

        Returns
        -------
        list of dict, with each item:
            {'info_type': {'name': key}, 'dictionary': {'words': value}}
        """
        output = []
        for k, v in pattern_dict:
            output.append({"info_type": {"name": k}, "dictionary": {"words": v}})
        return output


class DLPContent(DLPBase):
    def __init__(self, gcp_credentials=None, version=1):
        super().__init__(gcp_credentials, version)

        self.mask_character = "#"
        self.mask_max = 0  # 0 for no limit on mask

    def deidentify_with_replace(
        self,
        value,
        replacement=None,
        info_types=None,
        industry_category=None,
        location_category=None,
        type_category=None,
        output="value",
    ):
        # Info Types to inspect config
        info_types = self.info_type_builder(
            info_types=info_types,
            exclude_types=["DOCUMENT"],
            industry_category=industry_category,
            location_category=location_category,
            type_category=type_category,
        )

        inspect_config = {"info_types": info_types}

        if replacement is not None:
            # deidentify config using transformations > replacement
            deidentify_config = {
                "info_type_transformations": {
                    "transformations": [
                        {
                            "primitive_transformation": {
                                "replace_config": {
                                    "new_value": {"string_value": replacement}
                                }
                            }
                        }
                    ]
                }
            }

        elif replacement is not None:
            # deidentify config using transformations > replacement
            deidentify_config = {
                "info_type_transformations": {
                    "transformations": [
                        {
                            "primitive_transformation": {
                                "replace_config": {
                                    "new_value": {"string_value": replacement}
                                }
                            }
                        }
                    ]
                }
            }
        else:
            deidentify_config = {
                "info_type_transformations": {
                    "transformations": [
                        {
                            "primitive_transformation": {
                                "replace_with_info_type_config": {}
                            }
                        }
                    ]
                }
            }

        # request to the DLP API
        request = {
            "inspect_config": inspect_config,
            "parent": f"projects/{self.project_id}",
            "item": {"value": value},
            "deidentify_config": deidentify_config,
        }

        response = self.client.deidentify_content(request=request)

        if output == "value":
            return response.item.value
        if output == "all":
            return response

    def deidentify_series(self, pd_series, raw=False):
        json_str = pd_series.to_json()
        json_after_dlp = self.deidentify_with_replace(value=json_str)
        if raw:
            return json_after_dlp
        dict_after_dlp = json.loads(json_after_dlp)
        pd_series_after_dlp = pd.Series(dict_after_dlp)
        return pd_series_after_dlp


class DLPJob(DLPBase):
    def __init__(self):
        super().__init__()
        self.inspect_job = None
        self.risk_job = None

    def inspect_job_builder(self):
        pass

    def risk_job_builder(self):
        pass


class DLPBQ(DLPJob):
    def __init__(self):
        super().__init__()
        self.dataset_id = None
        self.table_id = None

    def inspect_table(self):
        pass
