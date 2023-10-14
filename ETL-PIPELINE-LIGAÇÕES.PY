import pandas as pd 

# Lendo o arquivo excel, definindo as colunas que serão utilizadas e pulando as 6 primeiras linhas da tabela 

arquivo = pd.ExcelFile('Planilha organização das ligações.xlsx')

# criando uma lista vazio para armazenar os dados

dfs = []

# iterando sobre as abas do arquivo 

for aba in arquivo.sheet_names:

    # lendo a aba atual 

    df_aba = arquivo.parse(aba)

    # Excluindo as linhas de 1 a 7

    df_aba = df_aba.drop([0, 1, 2, 3, 4, 5])

    # excluindo as colunas não utilizaveis

    df_aba = df_aba.drop(columns=['Unnamed: 1', 'Unnamed: 4'])

    # Excluindo a última linha de cada aba

    df_aba = df_aba.iloc[:-1]

     # Adicionando o DataFrame da aba à lista

    dfs.append(df_aba)

    # Concatenando todos os DataFrames em um único DataFrame

df_final = pd.concat(dfs, ignore_index=True)

# renomeando as colunas 

df_final.rename(columns={
    "Ligação atendida/ não atendida dia Luis": "Lead",
    "Unnamed: 2": "Não atendidas",
    "Unnamed: 3": "Atendidas"
}, inplace=True)

# substituindo todos valores NaN para 0 no df final.

df_final = df_final.fillna(0)

# transformando os valores em inteiros

df_final['Não atendidas'] = df_final['Não atendidas'].astype(int, errors='ignore')
df_final['Atendidas'] = df_final['Atendidas'].astype(int, errors='ignore')


total_não_atendidas = df_final['Não atendidas'].sum()
total_atendidas = df_final['Atendidas'].sum()

# Criando nova linha com valor total

nova_linha = {'Lead': 'Total',
              'Não atendidas': total_não_atendidas,
              'Atendidas': total_atendidas}

df_final = df_final._append(nova_linha, ignore_index=True)

# Salvar o DataFrame final em um novo arquivo Excel

df_final.to_excel('Planilha com Ligações do mês.xlsx', index=False)