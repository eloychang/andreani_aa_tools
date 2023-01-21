import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch

class elastic():

    def __init__(self, url_elastic, indice, clave):
        self._es = Elasticsearch([{"host": url_elastic, "http_auth": (indice, clave), "port": 80, "timeout": 30}])
        self._settings = {
            "unificacion" : self._query_unificacion
        }

    def _query_unificacion(self, tamaño_muestra, fecha, operacion):        
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
        srch = self._es.search(index='unificacion', doc_type='ui_events', body = bd)
        
        dires = []
        for i in range(len(srch["hits"]["hits"])):
            try:
                fecha = srch["hits"]["hits"][i]["_source"]["context"]["date"]
                operacion = srch["hits"]["hits"][i]["_source"]["context"]["operation"]
                sucursal = ssrch["hits"]["hits"][i]["_source"]["context"]["branchName"]
                user = srch["hits"]["hits"][i]["_source"]["context"]["user"]
                

                dt = eval(srch["hits"]["hits"][i]["_source"]["data"].replace("null","'null'").replace("false","'false'").replace("true","'true'"))
            
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
        
        
    def get_data(self, indice, **kwargs, fecha = datetime.now(), tamaño_muestra = 10000):
        reurn self._settings[indice](**kwargs, fecha, tamaño_muestra)
        
