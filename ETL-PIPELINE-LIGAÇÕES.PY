import pandas as pd 

# Lendo o arquivo excel, definindo as colunas que serão utilizadas e pulando as 6 primeiras linhas da tabela 

df = pd.read_excel('Planilha organização das ligações.xlsx', usecols=[0, 2, 3], skiprows=6)

# Transforma células com dados NaN em 0

df['Não'].fillna(0, inplace = True)
df['Sim'].fillna(0, inplace = True)

# Conversão dos dados float para int

conversao_inteiros = ['Não', 'Sim']
df[conversao_inteiros] = df[conversao_inteiros].astype(int)


print(df)