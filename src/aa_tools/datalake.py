
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd
from io import BytesIO
import pickle
import json

class datalake():

    def __init__(self, tenant_id, app_id, secret_value, storage_account_name):

        # Generamos el token
        token_credential = ClientSecretCredential(
            tenant_id = tenant_id,
            client_id = app_id,
            client_secret = secret_value
        )

        # Defino la conexión al datalake
        self._client = DataLakeServiceClient(
                account_url = "{}://{}.dfs.core.windows.net".format("https", storage_account_name),
                credential = token_credential)

        self._file_system_client = self._client.get_file_system_client("datalake")  
        
        self._import_settings = {
            "parquet" : self._read_parquet,
            "csv" : self._read_csv,
            "json" : self._read_json
        }

    def _read_csv(self, bytes, **kwarg):
        return  pd.read_csv(BytesIO(bytes), sep = kwarg["separator"], decimal = kwarg["decimal"], low_memory = False)

    def _read_parquet(self, bytes, **kwarg):
        return  pd.read_parquet(BytesIO(bytes))

    def _read_json(self, bytes, **kwarg):
        return json.loads(bytes.decode('utf-8'))

    def import_file(self, path, filename, read_format, file_system = "datalake", separator = ',', decimal = '.'):
        directory_client = self._client.get_directory_client(file_system = file_system, directory = path)
        file_client = directory_client.get_file_client(filename)
        # Descargo los datos
        download = file_client.download_file()
        downloaded_bytes = download.readall()

        return self._import_settings[read_format](bytes = downloaded_bytes, separator = separator, decimal = decimal)


    def upload_file(self, data, path, filename, write_format, file_system="datalake"):
        directory_client = self._client.get_directory_client(file_system=file_system, directory=path)

        file_client = directory_client.create_file(filename)
        if write_format == "parquet":
            file_contents = data.to_parquet(index=False)
        elif write_format == "csv":
            file_contents = data.to_csv(index=False).encode()
        elif write_format == "json":
            # Verifica si data es un DataFrame de pandas
            if isinstance(data, pd.DataFrame):
                file_contents = data.to_json(orient='records').encode('utf-8')
            # Verifica si data es un diccionario
            elif isinstance(data, dict):
                file_contents = json.dumps(data).encode('utf-8')
            # Verifica si data es una lista de diccionarios
            elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                file_contents = json.dumps(data).encode('utf-8')
            else:
                raise ValueError("El formato de 'data' no es compatible para la conversión a JSON.")
        file_client.upload_data(file_contents, overwrite=True)

    # Esta funcion lee y devuelve una lista con los nombres de los archivos en una carpeta
    def read_folder(self, prefix, extension):
        import re
        file_names = []
        for file in self._file_system_client.get_paths(path=prefix):
            file_name = file.name
            match = re.match(r'{}(.+)\.{}'.format(prefix, re.escape(extension)), file_name)
            if match:
                file_names.append(match.group(1))
        return file_names

    def create_directory(self, path):
        directory_client = self._file_system_client.get_directory_client(path)
        directory_client.create_directory()
