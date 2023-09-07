import boto3
from io import StringIO
from io import BytesIO
import pymysql
import pandas as pd

def lambda_handler(event, context):
    
    # Configura las credenciales de AWS
    aws_access_key = '***********'
    aws_secret_key = '***********'
    region_name = 'us-east-1'  # Cambia esto a tu región
    
    # Configura el nombre del bucket de S3 y la ruta dentro del bucket
    bucket_name = 'data-migraciones'
    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)
    
    # Cargar la data de remesas del s3
    obj_hechos = s3.get_object(Bucket=bucket_name, Key='datasql/tabla_de_hechos.csv')
    data1 = obj_hechos['Body'].read()
    df = pd.read_csv(BytesIO(data1))

    #Renombrado de las columnas
    df.rename(columns={'Remittance inflows (US$ million)': 'Remittance'}, inplace=True)
    df.rename(columns={'GDP (current US$': 'GDP'}, inplace=True)
    df.rename(columns={'Inflation, consumer prices (annual %)': 'Inflation'}, inplace=True)
    df.rename(columns={'Net migration': 'Net_migration'}, inplace=True)
    df.rename(columns={'aux': 'Id'}, inplace=True)
    
    #Cambio de tipo en dos columnas a str
    df['pais'] = df['pais'].astype(str)
    df['Id'] = df['Id'].astype(str)

    #Establezco conexion
    conexion = pymysql.connect(
        host = "database-migraciones.c5vthlbc6wtm.us-east-1.rds.amazonaws.com",
        user= "***********",
        passwd = "***********",
        db = "migraciones"

    )

    cursor = conexion.cursor()

    # Consulta SQL para obtener el valor máximo
    consulta = "SELECT MAX(anio) AS maximo_valor FROM anio"

    # Ejecutar la consulta
    cursor.execute(consulta)

    # Obtener el resultado como una lista de tuplas (en este caso, una sola tupla)
    resultado = cursor.fetchone()

    # Asumiendo que resultado es una tupla con al menos un elemento
    max_anio = resultado[0]

    # Filtrar las filas donde el valor en la columna 'mi_columna' sea mayor que valor_limite
    df_filtrado = df[df['anio'] > max_anio]

    # Eliminar duplicados basados en la columna 'anio'
    df_anio = df_filtrado.drop_duplicates(subset='anio')

    # Restablecer el índice en el DataFrame original y eliminar la columna de índice anterior
    df_anio.reset_index(drop=True, inplace=True)
    
    #Asignación de el máximo para el ciclo for
    sp = df_anio.shape
    len = sp[0]

    #Creación de una lista para carga masiva
    df_lista_anio = []
    for i in range (0,len):
        df_lista_anio.append((df_anio.anio[i])) 
        
    #Carga masiva a la tabla anio
    cursor.executemany("""insert into anio (anio)
                    values (%s)""", df_lista_anio
    )

    conexion.commit()

    # Definir la consulta SQL para seleccionar los datos de la tabla
    consulta = "SELECT * FROM anio"

    # Cargar los datos de la tabla en un DataFrame
    df1 = pd.read_sql_query(consulta, conexion)

    # Combinar los DataFrames en función de la columna "anio"
    resultado = pd.merge(df1, df_filtrado, on='anio', how='inner')

    # Definir la consulta SQL para seleccionar los datos de la tabla
    consulta1 = "SELECT * FROM pais"

    # Cargar los datos de la tabla en un DataFrame
    df2 = pd.read_sql_query(consulta1, conexion)

    # Combinar los DataFrames en función de la columna "pais"
    resultado2 = pd.merge(resultado, df2, on='pais', how='inner')

    #Determinar el máximo para el ciclo for
    sp2 = resultado2.shape
    len2 = sp2[0]

    #Creación de una lista para carga masiva
    df_lista = []
    for i in range (0,len2):
        df_lista.append((resultado2.id_anio[i], 
                        resultado2.id_pais[i],
                        resultado2.Remittance[i], 
                        resultado2.Id[i],
                        resultado2.GDP[i],
                        resultado2.Inflation[i],
                        resultado2.Net_migration[i])) 
        
    #Carga masiva a una tabla    
    cursor.executemany("""insert into macroeconomico (id_anio, id_pais, remittance, Id, GDP, Inflation, Net_migration)
                    values (%s, %s, %s, %s, %s, %s, %s)""", df_lista)
                    
    conexion.commit()

    #Cierre de la conexión
    conexion.close()
