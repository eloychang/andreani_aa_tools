
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO

class datalake():

    def __init__(self, dl_url, dl_key):

        self._client = BlobServiceClient(account_url = dl_url, credential = dl_key)

        self._import_settings = {
            "parquet" : pd.read_parquet,
            "csv" : pd.read_csv
        }

    def import_file(self, file_system, path, format):
        file_client = self._client.get_blob_client(file_system, path, snapshot=None)

        # descargo los datos
        download = file_client.download_blob()
        downloaded_bytes = download.readall()

        return self._import_settings[format](BytesIO(downloaded_bytes))


    def upload_file(self, data, file_system, path, format, index = False):
        file_client = client.get_blob_client(file_system, path, snapshot=None)

        if format == "parquet":
            file_contents = data.to_parquet(index=index).encode()
        elif format == "csv":
            file_contents = data.to_csv(index=index).encode()

        file_client.upload_blob(file_contents)
