
class databricks():

    def upload_file_from_databricks(df,file_name,file_type,path_folder='/advanced_analytics/'):
                       
        #establece path completo en donde se guardará la carpeta output del proceso de spark
        file_datalake = spark.conf.get("datalake.path")+path_folder+file_name 
        
        #Guardamos el output en una carpeta que será temporal
        df_datalake = df.coalesce(1).write.mode("overwrite").option("header","true").format(file_type).save(file_datalake+'.tmp')
        
        #Pickeamos y guardamos solamente el archivo que nos interesa guardar
        path =  list(filter(lambda ar:ar.name.endswith(file_type),dbutils.fs.ls(file_datalake+".tmp/")))[0].path
        dbutils.fs.cp(path,file_datalake)
        
        #Borramos la carpeta temporal que se había creado
        dbutils.fs.rm(file_datalake+".tmp",recurse=True)