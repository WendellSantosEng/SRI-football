import sqlite3
import re
import os
import sys
from typing import Set, List

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


def _get_docs_por_termo(termo: str, cursor: sqlite3.Cursor) -> Set[int]:
    """Busca no índice invertido todos os DocIds que contêm um termo."""
    cursor.execute(
        "SELECT DocId FROM IndiceInvertido WHERE Termo = ?",
        (termo,)
    )
    # Usa set comprehension para criar o conjunto de DocIds
    return {row[0] for row in cursor.fetchall()}

def _get_todos_docs(cursor: sqlite3.Cursor) -> Set[int]:
    """Retorna um conjunto com todos os DocIds da coleção."""
    cursor.execute("SELECT DocId FROM Documentos")
    return {row[0] for row in cursor.fetchall()}


def executar_busca_booleana(query_bruta: str, conexao: sqlite3.Connection) -> List[int]:
    """
    Executa a busca booleana.
    Suporta operadores AND, OR, e NOT (case-insensitive).
    Exemplos: "termo1 AND termo2", "termo1 OR termo2", "termo1 AND NOT termo2"
    """
    cursor = conexao.cursor()
    
    # Parser simples. Divide a query por operadores, mantendo-os.
    # Ex: "redes AND NOT segurança" -> ['redes', 'AND', 'NOT', 'segurança']
    tokens = re.split(r'\s+(AND|OR|NOT)\s+', query_bruta, flags=re.IGNORECASE)
    
    # Limpa espaços e processa os termos
    termos_processados = []
    operadores = []
    
    # Processa o primeiro termo
    termos_processados.append(processar(tokens[0])[0] if tokens else "")
    
    i = 1
    while i < len(tokens):
        # O token na posição 'i' é o operador (AND, OR, NOT)
        op = tokens[i].upper()
        # O token na posição 'i+1' é o termo
        termo_bruto = tokens[i+1]
        
        # Se for "AND NOT", trata como um operador só
        if op == 'AND' and (i + 2 < len(tokens)) and tokens[i+2].upper() == 'NOT':
            operadores.append('AND NOT')
            termo_bruto = tokens[i+3]
            i += 3 # Pula 3 (AND, NOT, termo)
        else:
            operadores.append(op)
            i += 2 # Pula 2 (OPERADOR, termo)
            
        # Processa o termo e pega só o primeiro (ignora consultas multi-palavra sem operador)
        termos_processados.append(processar(termo_bruto)[0] if termo_bruto else "")

    # --- Lógica de Conjuntos ---
    
    # Caso especial: "NOT termo1"
    if operadores and operadores[0] == 'NOT':
        todos_docs = _get_todos_docs(cursor)
        docs_termo = _get_docs_por_termo(termos_processados[1], cursor) # Pega o termo depois do NOT
        resultado_final = todos_docs - docs_termo
        # (Ignora o resto da query por simplicidade)
        return sorted(list(resultado_final))

    # Pega o conjunto de resultados do primeiro termo
    if not termos_processados[0]:
        return [] # Query vazia
        
    resultado_final = _get_docs_por_termo(termos_processados[0], cursor)

    # Aplica os operadores seguintes
    for k, op in enumerate(operadores):
        # Pega o próximo termo (índice k+1, pois termos_processados[0] já foi usado)
        termo_seguinte = termos_processados[k+1]
        docs_termo_seguinte = _get_docs_por_termo(termo_seguinte, cursor)
        
        if op == 'AND':
            resultado_final = resultado_final.intersection(docs_termo_seguinte)
        elif op == 'OR':
            resultado_final = resultado_final.union(docs_termo_seguinte)
        elif op == 'AND NOT':
            resultado_final = resultado_final.difference(docs_termo_seguinte)
        elif op == 'NOT': # Trata "termo1 NOT termo2" como "termo1 AND NOT termo2"
            resultado_final = resultado_final.difference(docs_termo_seguinte)

    return sorted(list(resultado_final))

# Bloco de teste
if __name__ == "__main__":
    print("--- Testando Modelo Booleano ---")
    # Crie um banco 'sri.db' de teste para rodar isso
    # CAMINHO_DB_TESTE = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'sri.db')
    # try:
    #     conn = sqlite3.connect(CAMINHO_DB_TESTE)
    #     # Teste 1: "redes"
    #     # Teste 2: "redes AND seguranca"
    #     # Teste 3: "redes AND NOT seguranca"
    #     conn.close()
    #     print("Teste concluído (simulação).")
    # except Exception as e:
    #     print(f"Erro ao conectar no DB de teste: {e}")
    pass