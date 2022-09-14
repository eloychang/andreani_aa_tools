
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from io import BytesIO
import pickle

class datalake():

    def __init__(self, credentials, file_system = "datalake"):
        with open(credentials, "rb") as file:
            storage_account_name, storage_account_key = pickle.load(file)

        # Defino la conexión al datalake
        service_client = DataLakeServiceClient(
                account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name),
                credential=storage_account_key)
        self._client = service_client.get_file_system_client(file_system=file_system)

        self._import_settings = {
            "parquet" : pd.read_parquet,
            "csv" : pd.read_csv
        }

        self._upload_settings = {
            "parquet" : pd.to_parquet,
            "csv" : pd.to_csv
        }


    def import_file(self, path, filename, format):
        directory_client = self._client.get_directory_client(path)
        file_client = directory_client.get_file_client(filename)
        # descargo los datos
        download = file_client.download_file()
        downloaded_bytes = download.readall()

        return self._import_settings[format](BytesIO(downloaded_bytes))


    def upload_file(self, path, filename, format):
        directory_client = self._client.get_directory_client(path)

        file_client = directory_client.create_file(filename)
        file_contents= self._upload_settings[format](index=False).encode()
        file_client.upload_data(file_contents, overwrite=True)
