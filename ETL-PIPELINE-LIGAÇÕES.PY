import pandas as pd 
from datetime import datetime

# Lendo o arquivo excel, definindo as colunas que serão utilizadas e pulando as 6 primeiras linhas da tabela 

arquivo = pd.ExcelFile('Planilha organização das ligações.xlsx')



# Lendo o valor da célula B2 na aba padrão

aba_padrao = pd.read_excel(arquivo, sheet_name=None, usecols=[1], header=None)

# Obtém a aba padrão (primeira aba)

primeira_aba = list(aba_padrao.keys())[0]

# Lê a string da célula B2 na aba padrão (primeira aba)

string_celula_B2 = aba_padrao[primeira_aba].iloc[1, 0]

# Transformar a string da celula B2 em data, que tem o valor "7/3/2023 a 7/3/2023" em que o formato utilizado é MM/DD/YYYY

data_celula_b2 = string_celula_B2.split(" a ")

data = datetime.strptime(data_celula_b2[0], '%m/%d/%Y')

# selecionando apenas o mês encontrado na célula

mes = data.strftime('%m')



# criando uma lista vazio para armazenar os dados

lista_abas = []

# iterando sobre as abas do arquivo 

for aba in arquivo.sheet_names:

    # lendo a aba atual 

    df_aba = arquivo.parse(aba)

    # Excluindo linhas de 1 a 6

    df_aba = df_aba.drop([0, 1, 2, 3, 4, 5])

    # excluindo as colunas não utilizaveis

    df_aba = df_aba.drop(columns=['Unnamed: 1', 'Unnamed: 4'])

    # Excluindo a última linha de cada aba

    df_aba = df_aba.iloc[:-1]

     # Adicionando o DataFrame da aba à lista

    lista_abas.append(df_aba)

    # Concatenando todos os DataFrames em um único DataFrame

df_final = pd.concat(lista_abas, ignore_index=True)

# renomeando as colunas 

df_final.rename(columns={
    "Ligação atendida/ não atendida dia Luis": "Lead",
    "Unnamed: 2": "Não atendidas",
    "Unnamed: 3": "Atendidas"
    }, inplace=True)

# substituindo todos valores NaN para 0 no df final.

df_final = df_final.fillna(0)

# Transforma os valores das colunas "Atendidas" e "Não atendidas" em inteiros

df_final['Não atendidas'] = df_final['Não atendidas'].astype(int, errors='ignore')
df_final['Atendidas'] = df_final['Atendidas'].astype(int, errors='ignore')

# Somando os valores das colunas

total_não_atendidas = df_final['Não atendidas'].sum()
total_atendidas = df_final['Atendidas'].sum()

# Criando nova linha para informar o valor total por coluna

nova_linha = {'Lead': 'Total',
              'Não atendidas': total_não_atendidas,
              'Atendidas': total_atendidas}

# Adicionando a linha ao DataFrame final 

df_final = df_final._append(nova_linha, ignore_index=True)

# Salvando o DataFrame final em um novo arquivo Excel informando o mês que foi trabalhdo.

df_final.to_excel(f'Planilha com Ligações do mês {mes}.xlsx', index=False)
