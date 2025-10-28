import sqlite3
import os
import sys
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer

# --- Início: Correção de Caminho (sys.path) ---
CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_SRC = os.path.dirname(CAMINHO_ATUAL)
CAMINHO_BASE_PROJETO = os.path.dirname(CAMINHO_SRC)

if CAMINHO_BASE_PROJETO not in sys.path:
    sys.path.append(CAMINHO_BASE_PROJETO)

try:
    from src.pipeline.processador import processar
except ImportError:
    print("Erro: Não foi possível importar 'processador'.")
    sys.exit(1)
# --- Fim: Correção de Caminho ---

# --- Definição de Caminhos ---
CAMINHO_DB = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'sri.db')
CAMINHO_VETORIZADOR = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'vectorizer.joblib')
CAMINHO_MATRIZ_TFIDF = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'tfidf_matrix.joblib')
CAMINHO_MAPA_DOCID = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'doc_id_map.joblib')
# -----------------------------

def treinar_e_salvar_modelo():
    """
    Lê os resumos do banco de dados, treina o TfidfVectorizer e 
    salva o vetorizador e a matriz TF-IDF em disco.
    """
    print("Iniciando treinamento do modelo vetorial...")
    
    if not os.path.exists(CAMINHO_DB):
        print(f"ERRO: Banco de dados '{CAMINHO_DB}' não encontrado.")
        print("Certifique-se que o 'construtor_indice.py' (Pessoa A) foi executado.")
        return

    conn = sqlite3.connect(CAMINHO_DB)
    cursor = conn.cursor()
    
    # 1. Carrega todos os resumos e DocIds
    # É CRUCIAL manter a ordem entre resumos e DocIds
    cursor.execute("SELECT DocId, ResumoOriginal FROM Documentos ORDER BY DocId ASC")
    documentos = cursor.fetchall()
    conn.close()
    
    if not documentos:
        print("ERRO: Nenhum documento encontrado no banco de dados.")
        return
        
    # Separa os DocIds e os textos dos resumos em listas ordenadas
    doc_id_map = [doc[0] for doc in documentos]
    resumos_originais = [doc[1] for doc in documentos]
    
    print(f"Carregados {len(resumos_originais)} resumos do banco de dados.")

    # 2. Configura o Vetorizador
    # Usamos nosso 'processador.py' como o tokenizer!
    # O Scikit-learn vai:
    # 1. Pegar o resumo (string)
    # 2. Entregar para 'processar'
    # 3. Receber a lista de tokens limpos
    # 4. Calcular o TF-IDF
    vectorizer = TfidfVectorizer(
        tokenizer=processar,  # Nossa função customizada
        lowercase=False,      # Nosso 'processar' já faz isso
        stop_words=None       # Nosso 'processar' já faz isso
    )

    # 3. Treina o modelo e cria a matriz TF-IDF
    print("Treinando o TfidfVectorizer e criando a matriz...")
    tfidf_matrix = vectorizer.fit_transform(resumos_originais)
    
    # 4. Salva os artefatos em disco usando joblib
    joblib.dump(vectorizer, CAMINHO_VETORIZADOR)
    print(f"Vetorizador salvo em '{CAMINHO_VETORIZADOR}'")
    
    joblib.dump(tfidf_matrix, CAMINHO_MATRIZ_TFIDF)
    print(f"Matriz TF-IDF salva em '{CAMINHO_MATRIZ_TFIDF}'")
    
    joblib.dump(doc_id_map, CAMINHO_MAPA_DOCID)
    print(f"Mapeamento de DocId salvo em '{CAMINHO_MAPA_DOCID}'")
    
    print("\n[SUCESSO] Treinamento do Modelo Vetorial concluído.")
    print("Os arquivos de modelo foram gerados na pasta 'data/'.")


if __name__ == "__main__":
    # Para rodar este script, execute no terminal:
    # python src/recuperacao/treinar_vetorizador.py
    treinar_e_salvar_modelo()