import boto3
import pandas as pd
from io import StringIO
from io import BytesIO

def lambda_handler(event, context):
    
    # Configura las credenciales de AWS
    aws_access_key = '*************'
    aws_secret_key = '*************'
    region_name = 'us-east-1'  # Cambia esto a tu región
    
    # Configura el nombre del bucket de S3 y la ruta dentro del bucket
    bucket_name = 'data-migraciones'
    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)
    
    # Cargar la data de remesas del s3
    obj_remesas = s3.get_object(Bucket=bucket_name, Key='data/remesas_raw.xlsx')
    data1 = obj_remesas['Body'].read()
    df_remesas = pd.read_excel(BytesIO(data1))
    
    # Procesar la data
    df_remesas = df_remesas.sort_values(2022, ascending=False)
    df_remesas.rename(columns = {'2022e':2022}, inplace = True)
    df_remesas = df_remesas[(df_remesas["Remittance inflows (US$ million)"] != "World") &
                            (df_remesas["Remittance inflows (US$ million)"] != "Low-and Middle-Income Countries") &
                            (df_remesas["Remittance inflows (US$ million)"] != "Egypt, Arab Rep.")]
    df_remesas = df_remesas.head()
    anios = list(range(2012, 2023))
    paises = df_remesas["Remittance inflows (US$ million)"].tolist()
    df_remesas.set_index("Remittance inflows (US$ million)", inplace=True)
    df_hechos = pd.DataFrame(columns=["anio", "pais", "Remittance inflows (US$ million)", "aux"])
    for pais in paises:
        for anio in anios:
            df_hechos.loc[len(df_hechos.index)] = [anio, pais, df_remesas.loc[pais, anio], str(anio)+pais]
    
    # Cargar la data de pib del s3
    obj_pib = s3.get_object(Bucket=bucket_name, Key='data/pib_raw.xls')
    data2 = obj_pib['Body'].read()
    df_pib= pd.read_excel(BytesIO(data2), sheet_name="Data", header=3)
    
    # Procesar la data 
    df_pib = df_pib[df_pib["Country Name"].isin(paises)]
    df_pib.set_index("Country Name", inplace=True)
    df_pib_aux = pd.DataFrame(columns=["GDP (current US$", "aux"])
    for pais in paises:
        for anio in anios:
            df_pib_aux.loc[len(df_pib_aux.index)] = [df_pib.loc[pais, str(anio)], str(anio)+pais]
    
    # Concatenar la data
    df_hechos = pd.merge(df_hechos, df_pib_aux, on="aux")
    
    # Cargar la data de inflaciones del s3
    obj_inflacion = s3.get_object(Bucket=bucket_name, Key='data/inflacion_raw.xls')
    data3 = obj_inflacion['Body'].read()
    df_inflacion= pd.read_excel(BytesIO(data3), sheet_name="Data", header=3)
    
    # Procesar la data 
    df_inflacion = df_inflacion[df_inflacion["Country Name"].isin(paises)]
    df_inflacion.set_index("Country Name", inplace=True)
    df_inflacion_aux = pd.DataFrame(columns=["Inflation, consumer prices (annual %)", "aux"])
    for pais in paises:
        for anio in anios:
            df_inflacion_aux.loc[len(df_inflacion_aux.index)] = [df_inflacion.loc[pais, str(anio)], str(anio)+pais]
    
    # Concatenar la data
    df_hechos = pd.merge(df_hechos, df_inflacion_aux, on="aux")
    
    # Cargar la data de migraciones netas del s3
    obj_migraciones = s3.get_object(Bucket=bucket_name, Key='data/migraciones_raw.xls')
    data4 = obj_migraciones['Body'].read()
    df_migraciones= pd.read_excel(BytesIO(data4), sheet_name="Data", header=3)
    
    # Procesar la data
    df_migraciones = df_migraciones[df_migraciones["Country Name"].isin(paises)]
    df_migraciones.set_index("Country Name", inplace=True)
    df_migraciones_aux = pd.DataFrame(columns=["Net migration", "aux"])
    for pais in paises:
        for anio in anios:
            df_migraciones_aux.loc[len(df_migraciones_aux.index)] = [df_migraciones.loc[pais, str(anio)], str(anio)+pais]
    
    # Concatenar la data
    df_hechos = pd.merge(df_hechos, df_migraciones_aux, on="aux")
    

    # Guardar los datos preparados de la tabla de hechos en S3
    csv_buffer = StringIO()
    df_hechos.to_csv(csv_buffer, index=False)
    s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket='data-migraciones',
        Key='datasql/tabla_de_hechos.csv',
        ContentType='text/csv')