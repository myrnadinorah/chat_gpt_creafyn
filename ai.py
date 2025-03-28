from datetime import datetime, timedelta
from tabulate import tabulate
import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine
import openai

st.set_page_config(page_title="Análisis Económico", layout="centered")

st.title("Análisis Económico")

engine = create_engine(st.secrets["mysql_connection"])
query = "SELECT rfc, name FROM clients"
dclients = pd.read_sql(query, engine)
engine.dispose()

dc = pd.DataFrame({'CLIENT': dclients['name']})

client_chosen = st.selectbox("Selecciona un cliente:", dc['CLIENT'].tolist())

rfc_key = dclients[dclients['name'] == client_chosen]['rfc'].iloc[0]
st.write(f"**RFC del cliente seleccionado:** {rfc_key}")

aws_access_key = st.secrets["aws_access_key"]
aws_secret_key = st.secrets["aws_secret_key"]
openai_api_key = st.secrets["openai_api_key"]

s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
)

response = s3_client.get_object(Bucket='creafyn', Key='DescripcionEmpresas_BOT.xlsx')
file_stream = BytesIO(response['Body'].read())
df = pd.read_excel(file_stream, engine='openpyxl')

df_seleccionado = df.iloc[:, [1, 3]]
df_seleccionado.columns = ['RFC', 'DESCRIPCION']
descripcion_row = df_seleccionado[df_seleccionado['RFC'] == rfc_key]

if descripcion_row.empty:
    descripcion_text = "No se encontró descripción de la empresa."
    st.warning(descripcion_text)
else:
    descripcion_text = descripcion_row.iloc[0]['DESCRIPCION']
    st.write("**Descripción de la empresa:**")
    st.info(descripcion_text)

st.subheader("Prompt 1:")
prompt = f"""
Considerando que la empresa pertenece a la industria de: {descripcion_text}

Analiza el panorama económico actual con base en los datos macroeconómicos más recientes de México
y dime cómo pudiera afectar el panorama macroeconómico a las ventas de la empresa.

Además, mencióname a qué variables macroeconómicas es más sensible la empresa y por qué.

Evalúa también la situación económica actual/esperada en su región y a nivel nacional,
y explica cómo factores como inflación, tasas de interés, tipo de cambio u otros pueden impactar sus ventas.

Proporciona ejemplos específicos y análisis por industria.

Utiliza fuentes confiables como INEGI, Banxico, Data México, Statista, McKinsey, PwC, Deloitte, KPMG, Bain, EY, SEC, Banco Mundial, FMI, OCDE, Concamin, Canacintra, AMIA, ABM, BBVA Research, Santander, El Economista, Expansión, Forbes, México Industry, Secretaria de Economía, SEC:Edgar, IMF, etc.
Para este análisis considera el año 2025, febrero 2025
"""

client = openai.OpenAI(api_key=openai_api_key)
response = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "Eres un economista experto en análisis macroeconómico y riesgos financieros."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.7
)

st.subheader("Análisis Económico:")
st.markdown(response.choices[0].message.content)


#Prompt 2


def clients_currency_amounts(rfc_key):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=35)
    engine = create_engine(st.secrets["mysql_connection"])
    invoices_query = f'''
        SELECT currency, totalMxn, total, exchangeRate 
        FROM invoices 
        WHERE isIssuer = 1 
        AND status = "VIGENTE" 
        AND issuerRfc = "{rfc_key}" 
        AND issuedAt BETWEEN "{start_date.strftime('%Y-%m-%d')}" AND "{end_date.strftime('%Y-%m-%d')}"
    '''
    invoices_data = pd.read_sql(invoices_query, engine)
    engine.dispose()
    usd_data = invoices_data[invoices_data['currency'] == 'USD']
    mxn_data = invoices_data[invoices_data['currency'] == 'MXN']
    usd_to_mxn = (usd_data['total'] * usd_data['exchangeRate']).sum()
    total_mxn_direct = mxn_data['total'].sum()
    total_mxn_column = invoices_data['totalMxn'].sum()
    print("\nClientes:")
    print(f"Total en MXN directo (currency=MXN): {total_mxn_direct:,.2f} MXN")
    print(f"Total en USD convertido a MXN: {usd_to_mxn:,.2f} MXN")
    print(f"Total general (columna totalMxn): {total_mxn_column:,.2f} MXN")
    return usd_to_mxn, total_mxn_direct, total_mxn_column

def suppliers_currency_amounts(rfc_key):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=35)
    engine = create_engine(st.secrets["mysql_connection"])
    invoices_query = f'''
        SELECT currency, totalMxn, total, exchangeRate 
        FROM invoices 
        WHERE isReceiver = 1 
        AND status = "VIGENTE" 
        AND receiverRfc = "{rfc_key}" 
        AND issuedAt BETWEEN "{start_date.strftime('%Y-%m-%d')}" AND "{end_date.strftime('%Y-%m-%d')}"
    '''
    invoices_data = pd.read_sql(invoices_query, engine)
    engine.dispose()
    usd_data = invoices_data[invoices_data['currency'] == 'USD']
    mxn_data = invoices_data[invoices_data['currency'] == 'MXN']
    usd_to_mxn = (usd_data['total'] * usd_data['exchangeRate']).sum()
    total_mxn_direct = mxn_data['total'].sum()
    total_mxn_column = invoices_data['totalMxn'].sum()
    print("\nProveedores:")
    print(f"Total en MXN directo (currency=MXN): {total_mxn_direct:,.2f} MXN")
    print(f"Total en USD convertido a MXN: {usd_to_mxn:,.2f} MXN")
    print(f"Total general (columna totalMxn): {total_mxn_column:,.2f} MXN")
    return usd_to_mxn, total_mxn_direct, total_mxn_column

def checar(rfc_key):
    engine = create_engine(st.secrets["mysql_connection"])
    query = f'''
        SELECT receiverName, issuedAt, totalMxn, fullyPaidAt
        FROM invoices 
        WHERE isIssuer = 1 AND status = "VIGENTE" 
        AND issuerRfc = "{rfc_key}" AND receiverRfc = "XEXX010101000"
        AND issuedAt >= CURDATE() - INTERVAL 35 DAY
    '''
    df = pd.read_sql(query, engine)
    engine.dispose()
    df['issuedAt'] = pd.to_datetime(df['issuedAt'], errors='coerce')
    grouped = df.groupby('receiverName').agg(total_sales=('totalMxn', 'sum')).reset_index()
    grouped = grouped.sort_values(by='total_sales', ascending=False)
    grouped['total_sales'] = grouped['total_sales'].apply(lambda x: f"${x:,.2f}")
    print("\nVentas agrupadas de los últimos 35 días para RFC XEXX010101000:")
    print(tabulate(grouped.head(5), headers='keys', tablefmt='grid', showindex=False))
    return df

usd_clientes, mxn_clientes, total_clientes = clients_currency_amounts(rfc_key)
usd_prov, mxn_prov, total_prov = suppliers_currency_amounts(rfc_key)
invoices_data = checar(rfc_key)

C1 = descripcion_text
C2 = f"{(usd_clientes + usd_prov) / (total_clientes + total_prov) * 100:.2f}% del total facturado"

if not invoices_data.empty:
    extranjeros = invoices_data[invoices_data['receiverName'].str.contains('INC|LLC|CORP|S.A. DE C.V.|CO|LTD|LIMITED', na=False)]
    top_clientes = extranjeros['receiverName'].value_counts().head(3).index.tolist()
    C3 = ', '.join(top_clientes)
else:
    C3 = "No se encontraron clientes extranjeros relevantes"

st.subheader("Prompt 2:")
prompt2 = f"""
Considerando que la empresa pertenece a la industria de {C1}, su porcentaje de 
transacciones en dólares es {C2} y sus proveedores son {C3}, analiza cómo las 
políticas macroeconómicas propuestas por Donald Trump y las noticias recientes 
más relevantes podrían impactarla. 
Identifica cómo las políticas macroeconómicas pueden afectar tanto a la industria 
en general como a la empresa en particular. Para ello, considera aspectos clave 
como políticas arancelarias, acuerdos comerciales y política monetaria. 
Por ejemplo, si la empresa pertenece a la industria del acero, evalúa el impacto de 
una imposición del 25% en aranceles. 
Además, analiza el efecto de estas políticas en función del nivel de 
transaccionalidad con clientes y proveedores. 
Por ejemplo: 
• Si la empresa está en la industria automotriz y comercializa principalmente 
en dólares, ¿cómo le afectaría un dólar más débil al exportar a EE. UU.? 
• Si la mayoría de sus clientes son extranjeros, ¿cómo influiría la incertidumbre 
sobre aranceles y el tipo de cambio en sus ventas? ¿Podría provocar retrasos 
en proyectos y pedidos? 
Para realizar este análisis, considera las noticias más recientes y relevantes dentro 
de este ámbito. 
Para este análisis considera el año 2025, febrero 2025

"""


response2 = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "Eres un experto en economía y geopolítica."},
        {"role": "user", "content": prompt2}
    ],
    temperature=0.7,
    max_tokens=800
)

st.subheader("Análisis Geopolítico:")
st.markdown(response2.choices[0].message.content)


# ---------- PROMPT 3 ----------
st.subheader("Prompt 3:")

# Leer archivo con LOCACION incluida
response = s3_client.get_object(Bucket='creafyn', Key='DescripcionEmpresas_BOT.xlsx')
file_stream = BytesIO(response['Body'].read())
df = pd.read_excel(file_stream, engine='openpyxl')

df_seleccionado = df.iloc[:, [1, 3, 4]]
df_seleccionado.columns = ['RFC', 'DESCRIPCION', 'LOCACION']
descripcion = df_seleccionado[df_seleccionado['RFC'] == rfc_key]

if not descripcion.empty:
    industria = descripcion.iloc[0]['DESCRIPCION']
    locacion = descripcion.iloc[0]['LOCACION']
else:
    industria = "Industria no encontrada"
    locacion = "Locación no encontrada"

# Función para obtener diferencia entre ventas y creditedAmount
def ventas_credit(rfc_key):
    engine = create_engine(st.secrets["mysql_connection"])
    invoices_query = f'''
        SELECT clientName, issuedAt, totalMxn, creditedAmount 
        FROM invoices 
        WHERE isIssuer = 1 AND status = "VIGENTE" AND issuerRfc = "{rfc_key}"
    '''
    invoices_data = pd.read_sql(invoices_query, engine)
    engine.dispose()
    
    invoices_data['issuedAt'] = pd.to_datetime(invoices_data['issuedAt'], format='mixed', errors='coerce')
    current_month = pd.Timestamp.now().month
    data_2025 = invoices_data[invoices_data['issuedAt'].dt.year == 2025]

    if current_month > 1:
        data_2025 = data_2025[data_2025['issuedAt'].dt.month < current_month]

    mes_totalMxn = data_2025.groupby(data_2025['issuedAt'].dt.month)['totalMxn'].sum()
    mes_totalcredit = data_2025.groupby(data_2025['issuedAt'].dt.month)['creditedAmount'].sum()
    mes_diferencia = mes_totalMxn - mes_totalcredit

    diferencia_df = mes_diferencia.reset_index()
    diferencia_df.columns = ['Mes', 'Ventas - creditedAmount']
    return diferencia_df

# Generar DataFrame de diferencias y calcular % de cambio
ventas_df = ventas_credit(rfc_key)
ventas_df['Porcentaje Cambio'] = ventas_df['Ventas - creditedAmount'].pct_change() * 100
ventas_df['Porcentaje Cambio'] = ventas_df['Porcentaje Cambio'].round(2)

# Crear texto resumen de cambios
c2_texto = ', '.join([
    f"Mes {int(row['Mes'])}: {row['Porcentaje Cambio']}%"
    for idx, row in ventas_df.iterrows() if not pd.isna(row['Porcentaje Cambio'])
])

# Prompt final
prompt3 = f"""
Considerando que la empresa opera en la industria de {industria} y se encuentra ubicada en {locacion}, realiza un análisis actualizado con corte exclusivamente en febrero de 2025 sobre las principales oportunidades y riesgos en su sector.

Además, compara su desempeño ({c2_texto}) con el de otras empresas del mismo sector durante el mismo periodo. Quiero que identifiques:

Tendencias clave del sector

Riesgos y desafíos actuales

Avances tecnológicos relevantes

Cambios o nuevas regulaciones

Factores macroeconómicos que puedan representar oportunidades o amenazas

Para esto, basa tu análisis exclusivamente en fuentes confiables y datos actualizados al año de 2025. Específicamente utiliza:

Reportes de empresas públicas competidoras, similares o del mismo sector

Informes de consultoras reconocidas como KPMG, Bain, Deloitte, McKinsey, PwC, EY, etc.

Datos macroeconómicos nacionales e internacionales de INEGI, Banxico, Data México, Banco Mundial, FMI, OCDE, Concamin, Canacintra, AMIA, ABM, BBVA Research, Santander, Secretaría de Economía, México Industry, El Economista, Expansión, Forbes, Statista, SEC:Edgar, IMF, etc.

En caso de que la ubicación específica no tenga información puntual (por ejemplo, si se busca de Tijuana y no hay datos disponibles), utiliza la región más cercana (por ejemplo, zona fronteriza), y si tampoco se encuentra, usa datos representativos del país.

También quiero que expliques si la empresa está aprovechando las oportunidades del sector como lo están haciendo sus competidores, o si por el contrario, se está quedando rezagada. Justifica esto con base en datos concretos y análisis comparativo.

Es obligatorio incluir las fuentes y bibliografía utilizadas al final del análisis para validar la veracidad y confiabilidad de la información.

Para este análisis considera el año 2025, febrero 2025.
"""

response3 = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "Eres un analista financiero experto en análisis sectorial y macroeconómico."},
        {"role": "user", "content": prompt3}
    ],
    temperature=0.7,
    max_tokens=1500
)

st.subheader("Análisis Sectorial:")
st.markdown(response3.choices[0].message.content)



