import sqlite3
import os
import sys
from typing import List, Dict, Any

# --- Início: Correção de Caminho (sys.path) ---
CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_SRC = os.path.dirname(CAMINHO_ATUAL)
CAMINHO_BASE_PROJETO = os.path.dirname(CAMINHO_SRC)

if CAMINHO_BASE_PROJETO not in sys.path:
    sys.path.append(CAMINHO_BASE_PROJETO)

try:
    from src.recuperacao.modelo_booleano import executar_busca_booleana
    from src.recuperacao.modelo_vetorial import buscar_vetorial
except ImportError as e:
    print(f"Erro ao importar módulos de recuperação: {e}")
    sys.exit(1)
# --- Fim: Correção de Caminho ---

CAMINHO_DB = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'sri.db')

def _enriquecer_resultados(resultados: List[Dict[str, Any]], conexao: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Pega uma lista de resultados (com DocId) e adiciona Título e Autor
    buscando no banco de dados.
    """
    cursor = conexao.cursor()
    resultados_finais = []
    
    for item in resultados:
        doc_id = item['DocId']
        cursor.execute(
            "SELECT Titulo, Autor FROM Documentos WHERE DocId = ?",
            (doc_id,)
        )
        row = cursor.fetchone()
        if row:
            item['Titulo'] = row[0]
            item['Autor'] = row[1]
            resultados_finais.append(item)
            
    return resultados_finais


def buscar(query_bruta: str, modelo: str) -> List[Dict[str, Any]]:
    """
    Função principal de busca que será usada pela interface gráfica (Pessoa C).
    
    Args:
        query_bruta (str): A string de busca do usuário (ex: "redes AND seguranca").
        modelo (str): "booleano" ou "vetorial".
        
    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários, cada um contendo:
                               {'DocId', 'Titulo', 'Autor', 'Score'}
    """
    
    if not os.path.exists(CAMINHO_DB):
        return [{"Erro": "Banco de dados não encontrado."}]
        
    conn = sqlite3.connect(CAMINHO_DB)
    resultados_com_score = []
    
    try:
        if modelo == 'booleano':
            # 1. Executa a busca booleana
            doc_ids = executar_busca_booleana(query_bruta, conn)
            # 2. Atribui score 1.0 (relevância binária)
            resultados_com_score = [{'DocId': doc_id, 'Score': 1.0} for doc_id in doc_ids]
        
        elif modelo == 'vetorial':
            # 1. Executa a busca vetorial
            # Retorna lista de (DocId, score)
            resultados_tuplas = buscar_vetorial(query_bruta)
            # 2. Converte para lista de dicionários
            resultados_com_score = [{'DocId': doc_id, 'Score': score} for doc_id, score in resultados_tuplas]
            
        else:
            return [{"Erro": f"Modelo '{modelo}' desconhecido."}]

        # 3. Adiciona Título e Autor aos resultados
        resultados_finais = _enriquecer_resultados(resultados_com_score, conn)
        
        return resultados_finais
        
    except Exception as e:
        print(f"Erro durante a busca: {e}")
        return [{"Erro": str(e)}]
    finally:
        conn.close()


# Bloco de teste
if __name__ == "__main__":
    print("--- Testando o Buscador (API para Pessoa C) ---")
    
    query1 = "estádios" # Coloque termos que existam no seu DB
    query2 = "pessoas" # Teste booleano
    
    print(f"\nBuscando (Vetorial) por: '{query1}'")
    resultados_vet = buscar(query1, "vetorial")
    for res in resultados_vet[:5]: # Mostra os 5 primeiros
        print(f"  [Score: {res.get('Score'):.4f}] {res.get('Titulo')} (DocId: {res.get('DocId')})")

    print(f"\nBuscando (Booleano) por: '{query2}'")
    resultados_bool = buscar(query2, "booleano")
    for res in resultados_bool:
        print(f"  {res.get('Titulo')} (DocId: {res.get('DocId')})")