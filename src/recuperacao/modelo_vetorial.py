import os
import sys
import joblib
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Tuple

# --- Início: Correção de Caminho (sys.path) ---
CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_SRC = os.path.dirname(CAMINHO_ATUAL)
CAMINHO_BASE_PROJETO = os.path.dirname(CAMINHO_SRC)

if CAMINHO_BASE_PROJETO not in sys.path:
    sys.path.append(CAMINHO_BASE_PROJETO)
# --- Fim: Correção de Caminho ---

# --- Caminhos para os arquivos de modelo ---
CAMINHO_VETORIZADOR = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'vectorizer.joblib')
CAMINHO_MATRIZ_TFIDF = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'tfidf_matrix.joblib')
CAMINHO_MAPA_DOCID = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'doc_id_map.joblib')
# ---------------------------------------------

# --- Carregamento dos Modelos (feito uma vez na importação) ---
try:
    VETORIZADOR = joblib.load(CAMINHO_VETORIZADOR)
    MATRIZ_TFIDF = joblib.load(CAMINHO_MATRIZ_TFIDF)
    MAPA_DOCID = joblib.load(CAMINHO_MAPA_DOCID)
    print("Modelo Vetorial (Vetorizador, Matriz, MapaDocId) carregado com sucesso.")
except FileNotFoundError:
    print(f"ERRO: Arquivos de modelo não encontrados na pasta 'data/'.")
    print("Execute o script 'src/recuperacao/treinar_vetorizador.py' primeiro.")
    VETORIZADOR, MATRIZ_TFIDF, MAPA_DOCID = None, None, None
# -------------------------------------------------------------


def buscar_vetorial(query_bruta: str) -> List[Tuple[int, float]]:
    """
    Executa a busca vetorial para uma query.
    Retorna uma lista de tuplas (DocId, Score) ordenada por relevância.
    """
    if not VETORIZADOR:
        print("ERRO: Modelo vetorial não foi carregado.")
        return []

    # 1. Vetoriza a query
    # Usamos VETORIZADOR.transform() (NÃO fit_transform)
    # A query deve estar dentro de uma lista, pois 'transform' espera um iterável
    query_vetor = VETORIZADOR.transform([query_bruta])
    
    # 2. Calcula a Similaridade do Cosseno
    # Compara o vetor da query (1xN) com a matriz de todos os docs (20xN)
    similaridades = cosine_similarity(query_vetor, MATRIZ_TFIDF)
    
    # 'similaridades' é uma matriz 2D (ex: [[0.1, 0.5, 0.0, ...]])
    # Pegamos apenas a primeira (e única) linha de scores
    scores = similaridades[0]
    
    # 3. Combina DocIds com Scores
    # Usamos o MAPA_DOCID para mapear o índice (0, 1, 2...) para o DocId real (1, 2, 5...)
    resultados = []
    for i, score in enumerate(scores):
        if score > 0.01: # Filtra resultados com relevância mínima
            doc_id = MAPA_DOCID[i]
            resultados.append((doc_id, score))
            
    # 4. Ordena os resultados pelo score (do maior para o menor)
    resultados.sort(key=lambda item: item[1], reverse=True)
    
    return resultados