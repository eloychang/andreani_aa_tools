import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch

class elastic():

    def __init__(self, url_elastic, indice, clave):
        self._es = Elasticsearch([{"host": url_elastic, "http_auth": (indice, clave), "port": 80, "timeout": 30}])

    def _query_unificacion(self, tamaño_muestra, fecha, join):        
        bd = {
                "size": tamaño_muestra,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {"context.date": fecha}
                            },
                            {
                                "match": {"context.operation": "generate_definitive_roadmap"}
                            }
                        ]
                    }
                }
            }
        srch = self._es.search(index='unificacion', doc_type='ui_events', body = bd)

        bd = {
                    "size": tamaño_muestra,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {"context.date": fecha}
                                },
                                {
                                    "match": {"context.operation": "normalize_shipment_address_andreani"}
                                }
                            ]
                        }
                    }
                }
        srch_n = self._es.search(index='unificacion', doc_type='ui_events', body = bd)

        dires = []
        for i in range(len(srch["hits"]["hits"])):
            try:
                fecha = srch["hits"]["hits"][i]["_source"]["context"]["date"]
                operation = srch["hits"]["hits"][i]["_source"]["context"]["operation"]
                sucursal=srch["hits"]["hits"][i]["_source"]["context"]["branchName"]
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
                        "operacion": operation,
                        'preNormalized': preNorm
                        

                    }
                    dires.append(dire)
            except:
                continue

        dires_normalize = []
        for i in range(len(srch_n["hits"]["hits"])):
            
            try:
                dt = eval(srch_n["hits"]["hits"][i]["_source"]["data"].replace("null","'null'").replace("false","'false'").replace("true","'true'"))
                dire={
                        "shipmentNumber":dt["response"]['standardAddress']["entityNumber"],
                        "latitude_andreani":dt["response"]['standardAddress']["latitude"],
                        "longitude_andreani":dt["response"]['standardAddress']["longitude"],
                        "mensaje_elastic":dt["response"]['standardAddress']["message"],
                        "mensaje_geolocalizacion_elastic":dt["response"]['standardAddress']["mensaje_geolocalizacion"]       
                    }
                dires_normalize.append(dire)
            except:
                continue

        df = pd.DataFrame(dires).drop_duplicates(["calle","localidad","provincia","numero","codigoPostal","latitude","longitude"])
        df = df.reset_index(drop=True)
        elastic_andreani = pd.DataFrame(dires_normalize)
        df = df.merge(elastic_andreani, how= join, left_on='shipmentNumber', right_on='shipmentNumber')
        
        return df
        
        
    def get_data(self, tamaño_muestra, fecha , join):
        return self._query_unificacion(self, tamaño_muestra, fecha, join)
        
