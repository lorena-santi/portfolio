import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from dotenv import load_dotenv

print("Starting \n")

load_dotenv()
path = os.getenv("MYPATH")

#creating an empty table
order_columns =['dt_venda','hora_venda','qtd_clientes','produto','qtd_produtos_unid','valor_vendas','maior_venda','tipo_corte','loja']
vendas_dw_mac = pd.DataFrame(columns=order_columns)
vendas_dw_cam = pd.DataFrame(columns=order_columns)

#loading sales data from the mac store
mac_vendas = pd.read_excel(path+'Planilhas_MAC.xlsx', sheet_name='VENDAS')

#processing data
mac_vendas['PESCADO'] = mac_vendas['PESCADO'].str.title()
mac_vendas['OBSERVAÇÃO'] = mac_vendas['OBSERVAÇÃO'].str.title()
mac_obs_uniq = mac_vendas['OBSERVAÇÃO'].drop_duplicates()
mac_prod_uniq = mac_vendas['PESCADO'].drop_duplicates()
mac_vendas.replace('Inteiro (F)','Inteiro', inplace=True)
mac_vendas.replace('Fresco','Inteiro', inplace=True)
mac_vendas.replace('-','Inteiro', inplace=True)
mac_vendas.replace('C/Casca','Com Casca', inplace=True)
mac_vendas.replace('Sirigado (M)','Sirigado', inplace=True)
mac_vendas.replace('Camarão (G)','Camarão', inplace=True)
mac_vendas.replace('Camarão (M)','Camarão', inplace=True)
mac_vendas.replace('Camarão (P)','Camarão', inplace=True)
mac_vendas['VALOR'] = pd.to_numeric(mac_vendas['VALOR'])
mac_vendas['QUANTIDADE'] = pd.to_numeric(mac_vendas['QUANTIDADE'])
mac_vendas['VALOR'] = mac_vendas['VALOR'] * mac_vendas['QUANTIDADE'] 

#populating table
vendas_dw_mac['dt_venda'] = mac_vendas['DATA']
vendas_dw_mac['valor_vendas'] = mac_vendas['VALOR']
vendas_dw_mac['qtd_produtos_unid'] = mac_vendas['QUANTIDADE']
vendas_dw_mac['produto'] = mac_vendas['PESCADO']
vendas_dw_mac['tipo_corte'] = mac_vendas['OBSERVAÇÃO']
vendas_dw_mac['loja'] = 'Mac'
vendas_dw_mac['hora_venda'] = np.nan
vendas_dw_mac['qtd_clientes'] = np.nan
vendas_dw_mac['maior_venda'] = np.nan

#loading sales data from the camocim store
cam_vendas = pd.read_excel(path+'Planilhas_Camocim.xlsx', sheet_name='VENDAS')

#processing data
cam_vendas['DT_VENDA'] = pd.to_datetime(cam_vendas['DT_VENDA'], format='%d/%m/%Y')

#populating table
vendas_dw_cam['dt_venda']=cam_vendas['DT_VENDA']
vendas_dw_cam['hora_venda']=cam_vendas['HORA']
vendas_dw_cam['valor_vendas']=cam_vendas['VALOR_VENDAS']
vendas_dw_cam['maior_venda']=cam_vendas['MAIOR_VENDA']
vendas_dw_cam['qtd_clientes']=cam_vendas['QUANTIDADE_CLIENTES']
vendas_dw_cam['loja']='Camocim'
vendas_dw_cam['produto'] = np.nan
vendas_dw_cam['qtd_produtos_unid'] = np.nan
vendas_dw_cam['tipo_corte'] = np.nan

#makes sure the columns are in the same order
vendas_dw_mac = vendas_dw_mac[order_columns]
vendas_dw_cam = vendas_dw_cam[order_columns]

#concatenating tables
vendas_dw = pd.concat([vendas_dw_mac,vendas_dw_cam])

#creating an empty table
order_columns =['dt_estoque','id_fornecedor','fornecedor','id_produto','produto','valor_compra','qtd_estoque_kg','qtd_saida_kg','tipo_corte','loja']
estoque_dw_mac = pd.DataFrame(columns=order_columns)
estoque_dw_cam = pd.DataFrame(columns=order_columns)

#loading inventory data from the mac store
mac_estoque = pd.read_excel(path+'Planilhas_MAC.xlsx', sheet_name='ESTOQUE')

#processing data
mac_estoque.replace('P/POSTA','POSTA', inplace=True)
mac_estoque.replace('CORTE','POSTA', inplace=True)
mac_estoque.replace('FILÉ (CONGELADO)','FILÉ', inplace=True)
mac_estoque.replace('INTEIRA','INTEIRO', inplace=True)
mac_estoque.replace('FRESCO','INTEIRO', inplace=True)
mac_estoque['OBSERVAÇÃO'] = mac_estoque['OBSERVAÇÃO'].str.title()
mac_estoque['PESCADO'] = mac_estoque['PESCADO'].str.title()
mac_estoque['QUANTIDADE'] = pd.to_numeric(mac_estoque['QUANTIDADE'])/1000
mac_estoque_duplicado = mac_estoque.iloc[[-1]]
mac_estoque.replace('Ariacó / Pargo','Ariacó', inplace=True)
mac_estoque = pd.concat([mac_estoque,mac_estoque_duplicado])
mac_estoque.replace('Ariacó / Pargo','Pargo', inplace=True)

#populating table
estoque_dw_mac['id_fornecedor'] = mac_estoque['ID FORNECEDOR']
estoque_dw_mac['produto'] = mac_estoque['PESCADO']
estoque_dw_mac['dt_estoque'] = mac_estoque['DATA']
estoque_dw_mac['valor_compra'] = mac_estoque['VALOR DA COMPRA']
estoque_dw_mac['tipo_corte'] = mac_estoque['OBSERVAÇÃO']
estoque_dw_mac['qtd_estoque_kg'] = mac_estoque['QUANTIDADE']
estoque_dw_mac['loja'] = 'Mac'
estoque_dw_mac['fornecedor'] = np.nan
estoque_dw_mac['id_produto'] = np.nan
estoque_dw_mac['qtd_saida_kg'] = np.nan

#loading inventory data from the camocim store
cam_estoque = pd.read_excel(path+'Planilhas_Camocim.xlsx', sheet_name='ESTOQUE')

#processing data
cam_estoque['FORNECEDOR'] = cam_estoque['FORNECEDOR'].fillna('FORNECEDOR DESCONHECIDO')
cam_estoque['ID_FORNECEDOR'] = cam_estoque['ID_FORNECEDOR'].fillna(0)
cam_estoque['DT_ESTOQUE'] = pd.to_datetime(cam_estoque['DT_ESTOQUE'], format='%d/%m/%Y')
cam_estoque['FORNECEDOR'] = cam_estoque['FORNECEDOR'].str.title()
cam_estoque['PRODUTO'] = cam_estoque['PRODUTO'].str.title()
unique_produto = list(cam_estoque['PRODUTO'].drop_duplicates())
cam_estoque['TIPO_CORTE'] = np.where(cam_estoque['PRODUTO'].str.contains('File'),'Filé','Inteiro')
cam_estoque['TIPO_CORTE'] = np.where(cam_estoque['PRODUTO'].str.contains('Carne De Caranguejo'),'Filé',cam_estoque['TIPO_CORTE'])
cam_estoque['TIPO_CORTE'] = np.where(cam_estoque['PRODUTO'].str.contains('Bacalhau Desfiado Cong Dessalgado C'),'Filé',cam_estoque['TIPO_CORTE'])
cam_estoque['TIPO_CORTE'] = np.where(cam_estoque['PRODUTO'].str.contains('Arraia Desf'),'Filé',cam_estoque['TIPO_CORTE'])
cam_estoque['TIPO_CORTE'] = np.where(cam_estoque['PRODUTO'].str.contains('Posta'),'Posta',cam_estoque['TIPO_CORTE'])
cam_estoque['TIPO_CORTE'] = np.where(cam_estoque['PRODUTO'].str.contains('Bacalhau Saithe Lombo Riberalves  1'),'Posta',cam_estoque['TIPO_CORTE'])
cam_estoque.replace('Peixe Tilapia','Tilápia', inplace=True)
cam_estoque.replace('Camarao C/Cab','Camarão', inplace=True)
cam_estoque.replace('File De Sirigado M','Sirigado', inplace=True)
cam_estoque.replace('File De Sirigado G','Sirigado', inplace=True)
cam_estoque.replace('File De Camarão','Camarão', inplace=True)
cam_estoque.replace('Peixe Atum','Atum', inplace=True)
cam_estoque.replace('File De Atum','Atum', inplace=True)
cam_estoque.replace('File De Pescada Amarela','Pescada Amarela', inplace=True)
cam_estoque.replace('File De Cioba','Cioba', inplace=True)
cam_estoque.replace('File De Bejupira G','Bejupira', inplace=True)
cam_estoque.replace('File De Arabaiana','Arabaiana', inplace=True)
cam_estoque.replace('Posta De Caçao','Cação', inplace=True)
cam_estoque.replace('Posta De Pescada B','Pescada Branca',inplace=True)
cam_estoque.replace('File De Vermelho M/G','Vermelho', inplace=True)
cam_estoque.replace('Posta De Pescada Am','Pescada Amarela', inplace=True)
cam_estoque.replace('File De Galo','Galo', inplace=True)
cam_estoque.replace('Camarao','Camarão', inplace=True)
cam_estoque.replace('File De Dourado','Dourado', inplace=True)
cam_estoque.replace('File De Cavala','Cavala', inplace=True)
cam_estoque.replace('Posta Cavala','Cavala', inplace=True)
cam_estoque.replace('File De Surubin','Surubin', inplace=True)
cam_estoque.replace('Tucunare','Tucunaré', inplace=True)
cam_estoque.replace('Sardinha Int','Sardinha', inplace=True)
cam_estoque.replace('Lula Aneis Cong Gr10 Kg','Lula', inplace=True)
cam_estoque.replace('Sardinha Lage Espalmada Pct 1 Kg Co','Sardinha', inplace=True)
cam_estoque.replace('Lula Tubo Intefolhada','Lula', inplace=True)
cam_estoque.replace('Palombeta Evisc Cong','Palombeta', inplace=True)
cam_estoque.replace('Cavalinha Imp 300-500 Int Cong','Cavalinha', inplace=True)
cam_estoque.replace('File De Buchexa De Peixe','Buchexa de Peixe', inplace=True)
cam_estoque.replace('File De Abadejo','Abadejo', inplace=True)
cam_estoque.replace('File De Pescadilha','Pescadilha', inplace=True)
cam_estoque.replace('File De Garopa','Garopa', inplace=True)
cam_estoque.replace('File De Pescada B','Pescada', inplace=True)
cam_estoque.replace('File De Serra','Serra', inplace=True)
cam_estoque.replace('File De Cara','Tilápia', inplace=True)
cam_estoque.replace('Cavala Kg','Cavala', inplace=True)
cam_estoque.replace('Pargo Cong','Pargo', inplace=True)
cam_estoque.replace('Arraia Ped','Arraia', inplace=True)
cam_estoque.replace('Bejupira','Beijupirá', inplace=True)
cam_estoque.replace('Arabaina M','Arabaiana', inplace=True)
cam_estoque.replace('Xareu','Xaréu', inplace=True)
cam_estoque.replace('File De Salmao Chile','Salmão', inplace=True)
cam_estoque.replace('File Truta 500','Truta', inplace=True)
cam_estoque.replace('Bolinho De Bacalhau Ribeira Alves','Bolinho de Bacalhau', inplace=True)
cam_estoque.replace('Bacalhau Saithe Lombo Riberalves  1','Bacalhau', inplace=True)
cam_estoque.replace('Polaca Do Alasca Salg Resf Lascas 1','Polaca do Alasca', inplace=True)
cam_estoque.replace('Polaca Do Alasca Dess Tipo Bacalhau','Polaca do Alasca', inplace=True)
cam_estoque.replace('Bolinho De Bacalhau Cong 20X270G','Bolinho de Bacalhau', inplace=True)
cam_estoque.replace('Bolinha Bacalhau (Ban C/370G)','Bolinho de Bacalhau', inplace=True)
cam_estoque.replace('Bacalhau Desfiado Cong Dessalgado C','Bacalhau', inplace=True)
cam_estoque.replace('Patinha De Caranguejo Emp C/12 Un','Patinha de Caranguejo', inplace=True)
cam_estoque.replace('Bolinha De Peixe (Bandeja C/370G)','Bolinha de Peixe', inplace=True)
cam_estoque.replace('Bolinha C/Do Sol (Band C/370G)','Bolinha Carne de Sol', inplace=True)
cam_estoque.replace('Bolinha Camarão (Band C/370G)','Bolinha Camarão', inplace=True)
cam_estoque.replace('File De Camarão Pct 800Kg','Camarão', inplace=True)
cam_estoque.replace('Arraia Desf','Arraia', inplace=True)
cam_estoque.replace('Manjubimha','Manjubinha', inplace=True)
cam_estoque.replace('Tilapia','Tilápia', inplace=True)
cam_estoque.replace('Cavala Fresca','Cavala', inplace=True)
cam_estoque.replace('File De Camarao','Camarão', inplace=True)
cam_estoque.replace('File De Camarão 800G','Camarão', inplace=True)
cam_estoque.replace('Peixe Cavala','Cavala', inplace=True)
cam_estoque.replace('Kani Pct 200G','Kani', inplace=True)
cam_estoque.replace('Paella Ingredientes Cong 800 G','Paella Ingredientes', inplace=True)
cam_estoque.replace('Gelo Em Escama','Gelo em Escama', inplace=True)
cam_estoque.replace('Ariaco','Ariacó', inplace=True)
cam_estoque.replace('Peixe Timbiro','Tibiro', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco Corvina','Corvina', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco Beijupira','Beijupirá', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco Cara Acu','Acará Açu', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco Serra','Serra', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco Cururuca','Corvina', inplace=True)
cam_estoque.replace('Cara Tilapia','Tilápia', inplace=True)
cam_estoque.replace('File De Tilapia','Tilápia', inplace=True)
cam_estoque.replace('Posta De Serra','Serra', inplace=True)
cam_estoque.replace('Peixe Fresco Xareu','Xaréu', inplace=True)
cam_estoque.replace('Peixe Fresco Dentao','Dentão', inplace=True)
cam_estoque.replace('Peixe Fresco Ariaco','Ariacó', inplace=True)
cam_estoque.replace('Ariaco Kg','Ariacó', inplace=True)
cam_estoque.replace('Peixe Fresco Guaiuba','Guaiuba', inplace=True)
cam_estoque.replace('Pargo Cong F','Pargo', inplace=True)
cam_estoque.replace('Pargo M Cong','Pargo', inplace=True)
cam_estoque.replace('File De Panga','Panga', inplace=True)
cam_estoque.replace('Lula Em Aneis Congelado','Lula', inplace=True)
cam_estoque.replace('File De Salmao C/ P Vacuo Cong','Salmão', inplace=True)
cam_estoque.replace('Macaxeira Pre Cozida','Macaxeira', inplace=True)
cam_estoque.replace('File De Camarão Pp 800G','Camarão', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco - Bejupira','Beijupirá', inplace=True)
cam_estoque.replace('Peixe Inteiro Fresco - Serra','Serra', inplace=True)
cam_estoque.replace('Tainha 1Kg Up','Tainha', inplace=True)
cam_estoque.replace('Mexilhão Desc 100/200','Mexilhão', inplace=True)
cam_estoque.replace('Charro 3/4 Pcs','Charro', inplace=True)
cam_estoque.replace('Pampo Real 800 1200Kg','Pampo Real', inplace=True)
cam_estoque.replace('File De Merluza','Merluza', inplace=True)
cam_estoque.replace('Peixe Bonito','Bonito', inplace=True)
cam_estoque.replace('Garajuba','Guarajuba', inplace=True)
cam_estoque.replace('Almondegas De Peixe Congelada (400G','Almôndegas de Peixe', inplace=True)
cam_estoque.replace('Carne De Caranguejo','Carne de Caranguejo', inplace=True)
cam_estoque.replace('Ova De Peixe','Ova de Peixe', inplace=True)
cam_estoque.replace('Cabeça De Peixe','Cabeça de Peixe', inplace=True)
cam_estoque.replace('Galo Do Sul','Galo do Sul', inplace=True)
cam_estoque['TOTAL_ENTRADAS'] = pd.to_numeric(cam_estoque['TOTAL_ENTRADAS'])
cam_estoque['ID_FORNECEDOR'] = cam_estoque['ID_FORNECEDOR'].astype(int)
cam_estoque['ID_PRODUTO'] = cam_estoque['ID_PRODUTO'].astype(int)

#populating table
estoque_dw_cam['id_fornecedor'] = cam_estoque['ID_FORNECEDOR']
estoque_dw_cam['fornecedor'] = cam_estoque['FORNECEDOR']
estoque_dw_cam['id_produto'] = cam_estoque['ID_PRODUTO']
estoque_dw_cam['produto'] = cam_estoque['PRODUTO']
estoque_dw_cam['dt_estoque'] = cam_estoque['DT_ESTOQUE']
estoque_dw_cam['tipo_corte'] = cam_estoque['TIPO_CORTE']
estoque_dw_cam['qtd_estoque_kg'] = cam_estoque['TOTAL_ENTRADAS']
estoque_dw_cam['qtd_saida_kg'] = cam_estoque['TOTAL_SAIDAS']
estoque_dw_cam['loja'] = 'Camocim'
estoque_dw_cam['valor_compra'] = np.nan

#makes sure the columns are in the same order
estoque_dw_mac = estoque_dw_mac[order_columns]
estoque_dw_cam = estoque_dw_cam[order_columns]

#concatenating tables
estoque_dw = pd.concat([estoque_dw_mac,estoque_dw_cam])

#list of numeric columns in the vendas_dw dataframe
colunas_numericas_vendas = ['qtd_clientes', 'qtd_produtos_unid', 'valor_vendas', 'maior_venda']

#for each column, replace empty strings with NaN, convert to numeric
for col in colunas_numericas_vendas:
    vendas_dw[col] = pd.to_numeric(vendas_dw[col].where(vendas_dw[col] != '', np.nan), errors='coerce')

#list of numeric columns in the estoque_dw dataframe
colunas_numericas_estoque = ['id_fornecedor','id_produto','valor_compra','qtd_estoque_kg','qtd_saida_kg']

#for each column, replace empty strings with NaN, convert to numeric
for col in colunas_numericas_estoque:
    estoque_dw[col] = pd.to_numeric(estoque_dw[col].where(estoque_dw[col] != '', np.nan), errors='coerce')

print('Data transformation completed \n')

print('Uploading tables to BigQuery \n')

#setting up bigquery
os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = bigquery.Client()
project_id = 'portfolio-408419'
dataset = 'pescados'
id_tabela_vendas = f"{project_id}.{dataset}.vendas"
id_tabela_estoque = f"{project_id}.{dataset}.estoque"

#function to create the BigQuery schema
def create_schema_vendas():
    return [
        bigquery.SchemaField("dt_venda", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("hora_venda", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("qtd_clientes", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("produto", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("qtd_produtos_unid", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("valor_vendas", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("maior_venda", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("tipo_corte", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loja", "STRING", mode="NULLABLE"),
    ]

def create_schema_estoque():
    return [
        bigquery.SchemaField("dt_estoque", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("id_fornecedor", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("fornecedor", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_produto", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("produto", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("valor_compra", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("qtd_estoque_kg", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("qtd_saida_kg", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("tipo_corte", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loja", "STRING", mode="NULLABLE"),
    ]

#function to load the dataframe into BigQuery
def load_bigquery(df, tabela_id, schema):
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, tabela_id, job_config=job_config)
    job.result() 

#uploading 'vendas' table
try:
    client.delete_table(id_tabela_vendas, not_found_ok=True)  
    schema_vendas = create_schema_vendas()
    tabela_vendas = bigquery.Table(id_tabela_vendas, schema=schema_vendas)
    client.create_table(tabela_vendas)  
    load_bigquery(vendas_dw, id_tabela_vendas, schema_vendas)  
except Exception as e:
    print(f"Error uploading 'vendas' table: {e}")

#uploading 'estoque' table
try:
    client.delete_table(id_tabela_estoque, not_found_ok=True)  
    schema_estoque = create_schema_estoque()
    tabela_estoque = bigquery.Table(id_tabela_estoque, schema=schema_estoque)
    client.create_table(tabela_estoque)  
    load_bigquery(estoque_dw, id_tabela_estoque, schema_estoque) 
except Exception as e:
    print(f"Error uploading 'estoque' table: {e}")

print('Completed')
