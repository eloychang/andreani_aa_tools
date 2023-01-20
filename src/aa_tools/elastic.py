import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch

class elastic():

    def __init__(self, url_elastic, indice, clave):
        self._es = Elasticsearch([{"host": url_elastic, "http_auth": (indice, clave), "port": 80, "timeout": 30}])
        self._srch = self._import_data()

    def _import_data(self, tamaño_muestra, fecha, operacion):
        
        bd = {
                    "size":tamaño_muestra,
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"context.date": fecha}},
                                {"match": {"context.operation": operacion}}
                            ]
                        }
                    }
                }
        self._srch = self._es.search(index='unificacion', doc_type='ui_events', body = bd)


    def create_df(self):
        dires = []
        for i in range(len(self._srch["hits"]["hits"])):
            try:
                fecha = self._srch["hits"]["hits"][i]["_source"]["context"]["date"]
                operacion = self._srch["hits"]["hits"][i]["_source"]["context"]["operation"]
                sucursal = self._srch["hits"]["hits"][i]["_source"]["context"]["branchName"]
                user = self._srch["hits"]["hits"][i]["_source"]["context"]["user"]
                

                dt = eval(self._srch["hits"]["hits"][i]["_source"]["data"].replace("null","'null'").replace("false","'false'").replace("true","'true'"))
            
                d = dt["shipments"]
                for j in range(len(d)):
                    preNorm = False
                    if 'preNormalized' in d[j].keys():
                        preNorm = (d[j]["preNormalized"]=="true")
                    dire={
                        "shipmentNumber":d[j]["shipmentNumber"],
                        "calle":d[j]["street"],
                        "localidad":d[j]["location"],
                        "provincia":d[j]["province"],
                        "numero":d[j]["streetNumber"],
                        "codigoPostal":d[j]["postalCode"],
                        "latitude":d[j]["standardAddress"]["latitude"],
                        "longitude":d[j]["standardAddress"]["longitude"],
                        "fecha":fecha,
                        "sucursal":sucursal,
                        "user":user,
                        "operacion":operacion,
                        'preNormalized': preNorm
                        

                    }
                    dires.append(dire)
            except:
                continue

        df = pd.DataFrame(dires).drop_duplicates(["calle","localidad","provincia","numero","codigoPostal","latitude","longitude"])
        df = df.reset_index(drop=True)
        
        return df