import pandas as pd
import requests
import json
import firebase_admin
from firebase_admin import credentials, firestore

url_metadados = "https://drive.google.com/uc?export=download&id=19cJ7avNtsziaYkrrYuW7FeFdvgrxoNLc"

cred = credentials.Certificate("tcc-ufape-jose-daniel-firebase-adminsdk-fbsvc-ae66b7718d.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

resposta = requests.get(url_metadados)
resposta.raise_for_status()

dados_brutos = [linha for linha in resposta.text.strip().split('\n') if linha.strip()]

dados_processados = []
for i, linha in enumerate(dados_brutos):
    try:
        dados_processados.append(json.loads(linha))
    except json.JSONDecodeError as e:
        print(f"Erro na linha {i + 1}: {e}")
        print(f"Conteudo da linha com erro: {linha}")
        continue

metadados = pd.DataFrame(dados_processados)

metadados.to_csv('dados_completos.csv', index=False, sep=',', encoding='utf-8')

if 'project' in metadados.columns:
    contagem_commits = metadados['project'].value_counts()

    contagem_commits_df = contagem_commits.reset_index()
    contagem_commits_df.columns = ['Projeto', 'Numero de Commits']

    contagem_commits_df.to_csv('quantidade_commits.csv', index=False, sep=',', encoding='utf-8')

    for _, linha in contagem_commits_df.iterrows():
        projeto_ref = db.collection('commits-vunerabilidades-diversevul-2').document() 
        projeto_ref.set({
            'Projeto': linha['Projeto'],
            'Numero de Commits': linha['Numero de Commits']
        })
    
    for _, linha in metadados.iterrows():
        metadados_ref = db.collection('base-dados-diversevul').document()
        metadados_ref.set(linha.to_dict())

    print("Dados enviados para o Firestore com sucesso!")

    with open('quantidade_commits.txt', 'w', encoding='utf-8') as f:
        for _, linha in contagem_commits_df.iterrows():
            f.write(f"Projeto: {linha['Projeto']} - Quantidade de Commits: {linha['Numero de Commits']}\n")
