"""Google Sheets

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.

For gspread library usage see:
https://gspread-pandas.readthedocs.io/
"""


from gspread_pandas import Spread


def read(gcp_credentials, worksheet_id, sheet_id, **kwargs):
    """Load a Google Sheet into a Pandas DataFrame.

    Notes
    -----
    Google Sheets API must be enabled in the project the credentials are based in.

    Common gspread **kwargs related to header configurations:
        index (int) – col number of index column, 0 or None for no index (default 1)
        header_rows (int) – number of rows that represent headers (default 1)
        start_row (int) – row number for first row of headers or data (default 1)

    Parameters
    ----------
    gcp_credentials : google.oauth2.service_account.Credentials instance
    worksheet_id : str, unique name of worksheet to download
    sheet_id : str, unique ID of Sheet
    **kwargs : keyword arguments to pass to gspread_pandas.spread.Spread.sheet_to_df

    Returns
    -------
    Pandas DataFrame
    """

    spread = Spread(sheet_id, creds=gcp_credentials)
    spread.open_sheet(worksheet_id)

    # Override the defaults so index/column definitions
    # are required like pandas
    # The gspread_pandas defaults are:
    # index = column 1
    # header_rows = row 1
    # start_row = row

    if "index" not in kwargs:
        kwargs["index"] = None

    if "header_rows" not in kwargs:
        kwargs["header_rows"] = 0

    _df = spread.sheet_to_df(**kwargs)
    return _df


def write(gcp_credentials, worksheet_id, sheet_id, df, replace=True, **kwargs):
    """Read a worksheet from a Google Sheet.

    Notes
    -----
    sheet_id can be copied from the URL or retrieved with:
        gspread_pandas.client.Client.list_spreadsheet_files_in_folder

    Common gspread **kwargs related to header configurations:
        headers (bool) – whether to include the headers in the worksheet (default True)
        start (tuple,str) – tuple indicating (row, col) or string like ‘A1’ for top left cell (default (1,1))

    Parameters
    ----------
    gcp_credentials : google.oauth2.service_account.Credentials instance
    worksheet_id : str, unique name of worksheet to download
    sheet_id : str, unique ID of Sheet
    df : Pandas DataFrame to upload
    replace : bool, replace existing data in worksheet
    **kwargs : keyword arguments to pass to gspread_pandas.spread.Spread.df_to_sheet

    Returns
    -------
    None
    """

    spread = Spread(sheet_id, creds=gcp_credentials)
    spread.df_to_sheet(df, replace=replace, sheet=worksheet_id, **kwargs)


def format_header():
    pass
