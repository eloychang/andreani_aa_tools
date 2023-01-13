# Andreani Advanced Analytics tools

## Instalar usando pip

```

pip install andreani-aa-tools

```

## Importación

```

import aa_tools

```

## Ejemplo de uso

- Haversine

```

from aa_tools import logger, haversine

if __name__ == "__main__":

    log = logger("test.py", "main")

    result = haversine(-58.490160, -34.566116, -58.485096, -34.572123)

    log.log_console(f"Haversine distance: {result}", "INFO")

    log.close()

```

- AML Pipeline

```

from aa_tools import aml_pipeline, logger

if __name__ == "__main__":

    log = logger("test.py", "main")

    # Not part of pipeline
    aml_pipeline.create_train_template("test_file.py")
    log.log_console("Template file created as test_file.py", "INFO")

    tags = {
      "scale" : "false",
      "balanced" : "false",
      "outliers" : "false",
      "target" : "target"
    }
    log.log_console("Tags defined", "INFO")
    try:
      Pipeline = aml_pipeline.pipeline("aa_tools_test", "linear_regression", "regression", tags)
    except Exception as e:
      log.log_console(f"Exception initializing pipeline: {e}")
      log.close()
      raise e

    try:
      Pipeline.run("aml/azure.pkl", "aml/environment.yml", "aml", "train_template.py", log)
    except Exception as e:
      log.log_console(f"Exception running pipeline: {e}")
      log.close()
      raise e

    log.close()

```

### Listado de funciones agregadas:

* Haversine: Distancia euclidia entre dos puntos.

* Logger: Maneja el log según los lineamientos de Andreani.

* Datalake: Interfaz de conexión al datalake para descargar y cargar archivos csv, parquet y/o json.

* aml_pipeline: Pipeline de ejecución de experimentos en Azure Machine Learning.


### Listado de funciones a agregar:

* Distancia de ruta entre dos puntos.

* Model training
