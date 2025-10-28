import sqlite3
import json
import os
import sys
from collections import Counter
from typing import List, Dict

# --- Início: Correção de Caminho (sys.path) ---
# Este bloco permite que o script encontre a pasta 'src' e importe o 'processador'
# quando você roda o script diretamente (ex: python src/pipeline/construtor_indice.py)

# Pega o caminho absoluto da pasta 'src/pipeline/' (onde este arquivo está)
CAMINHO_ATUAL = os.path.dirname(os.path.abspath(__file__))
# Pega o caminho da pasta 'src/'
CAMINHO_SRC = os.path.dirname(CAMINHO_ATUAL)
# Pega o caminho da pasta raiz do projeto (um nível acima de 'src/')
CAMINHO_BASE_PROJETO = os.path.dirname(CAMINHO_SRC)

# Adiciona a pasta raiz do projeto ao sys.path para permitir importações
if CAMINHO_BASE_PROJETO not in sys.path:
    sys.path.append(CAMINHO_BASE_PROJETO)

# Agora podemos importar o 'processador' com segurança
try:
    from src.pipeline.processador import processar
except ImportError:
    print("Erro: Não foi possível importar 'processador'.")
    print("Certifique-se de que 'src/pipeline/processador.py' existe.")
    sys.exit(1)
# --- Fim: Correção de Caminho ---


# --- Definição de Caminhos ---
CAMINHO_DB = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'sri.db')
CAMINHO_METADADOS = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'metadata.json')
CAMINHO_RESUMOS_DIR = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'resumos_txt')
# -----------------------------


def criar_tabelas(conexao: sqlite3.Connection):
    """
    Cria a estrutura de tabelas no banco de dados SQLite, conforme Módulo 2.
    """
    cursor = conexao.cursor()
    
    # Tabela 1: Tabela de Documentos
    # <DocId, Título, Autor, Total de termos significativos> + Resumo Original
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Documentos (
        DocId INTEGER PRIMARY KEY,
        Titulo TEXT NOT NULL,
        Autor TEXT,
        TotalTermos INTEGER,
        ResumoOriginal TEXT
    );
    ''')
    
    # Tabela 2: Dicionário de Termos (Índice Global)
    # <Termo, Quantidade total de ocorrências (TF global), Frequência nos Documentos (DF)>
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DicionarioTermos (
        Termo TEXT PRIMARY KEY,
        TotalOcorrencias INTEGER,
        DF INTEGER
    );
    ''')
    
    # Tabela 3: Índice Invertido (Mapeia Termo -> Documento)
    # Armazena o TF (Term Frequency) de cada termo em cada documento
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS IndiceInvertido (
        Termo TEXT,
        DocId INTEGER,
        TF INTEGER,
        PRIMARY KEY (Termo, DocId),
        FOREIGN KEY (DocId) REFERENCES Documentos (DocId)
        FOREIGN KEY (Termo) REFERENCES DicionarioTermos (Termo)
    );
    ''')

    # Tabela 4: Metadados da Coleção (Registro <DocId, TotPal>)
    # Armazena informações globais sobre a coleção
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Metadados (
        Chave TEXT PRIMARY KEY,
        Valor TEXT
    );
    ''')
    
    conexao.commit()
    print("Tabelas do banco de dados criadas com sucesso.")


def carregar_metadados() -> List[Dict]:
    """Carrega o arquivo JSON de metadados."""
    try:
        with open(CAMINHO_METADADOS, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Arquivo de metadados não encontrado em '{CAMINHO_METADADOS}'.")
        print("Certifique-se de criar o 'metadata.json' na pasta 'data/'.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERRO: O arquivo 'metadata.json' está mal formatado (JSON inválido).")
        sys.exit(1)


def construir_indice():
    """
    Função principal. Lê os metadados e resumos, processa-os, 
    e popula o banco de dados SQLite.
    """
    print("Iniciando construção do índice...")
    
    # 1. Limpa o banco de dados antigo, se existir
    if os.path.exists(CAMINHO_DB):
        os.remove(CAMINHO_DB)
        print(f"Banco de dados antigo '{CAMINHO_DB}' removido.")
        
    # 2. Conecta e cria as tabelas
    conn = sqlite3.connect(CAMINHO_DB)
    criar_tabelas(conn)
    
    # 3. Carrega os metadados
    documentos_meta = carregar_metadados()
    if not documentos_meta:
        print("Nenhum documento encontrado em 'metadata.json'. Encerrando.")
        conn.close()
        return

    # 4. Estruturas de dados temporárias para o Dicionário Global
    # {termo: count global}
    ocorrencias_totais_global = Counter()
    # {termo: [lista de DocIds onde aparece]}
    documentos_por_termo_global = {} 
    
    total_palavras_colecao = 0
    total_documentos = 0
    ultimo_doc_id = 0

    # 5. Loop Principal: Processa cada documento
    print(f"Processando {len(documentos_meta)} documentos...")
    for doc_meta in documentos_meta:
        doc_id = doc_meta.get('DocId')
        titulo = doc_meta.get('Titulo')
        autor = doc_meta.get('Autor')
        
        if not doc_id or not titulo:
            print(f"AVISO: Documento com metadados incompletos. Pulando: {doc_meta}")
            continue

        ultimo_doc_id = max(ultimo_doc_id, doc_id)
        
        # 5a. Carrega o texto do resumo original
        caminho_resumo = os.path.join(CAMINHO_RESUMOS_DIR, f"{doc_id}.txt")
        try:
            with open(caminho_resumo, 'r', encoding='utf-8') as f:
                resumo_original = f.read()
        except FileNotFoundError:
            print(f"ERRO: Arquivo de resumo 'data/resumos_txt/{doc_id}.txt' não encontrado. Pulando DocId {doc_id}.")
            continue
            
        # 5b. Processa o texto (processador.py)
        tokens_limpos = processar(resumo_original)
        total_termos_significativos = len(tokens_limpos)
        total_palavras_colecao += total_termos_significativos
        total_documentos += 1
        
        # 5c. Calcula o TF (Term Frequency) para este documento
        tf_documento = Counter(tokens_limpos)
        
        # 5d. Atualiza acumuladores globais
        ocorrencias_totais_global.update(tokens_limpos)
        for termo in tf_documento.keys():
            documentos_por_termo_global.setdefault(termo, set()).add(doc_id)
            
        # 5e. Insere dados no Banco de Dados (Tabelas 'Documentos' e 'IndiceInvertido')
        cursor = conn.cursor()
        
        # Insere na Tabela Documentos
        cursor.execute(
            "INSERT INTO Documentos (DocId, Titulo, Autor, TotalTermos, ResumoOriginal) VALUES (?, ?, ?, ?, ?)",
            (doc_id, titulo, autor, total_termos_significativos, resumo_original)
        )
        
        # Insere no Índice Invertido (TF de cada termo para este DocId)
        entradas_indice_invertido = [
            (termo, doc_id, tf) for termo, tf in tf_documento.items()
        ]
        cursor.executemany(
            "INSERT INTO IndiceInvertido (Termo, DocId, TF) VALUES (?, ?, ?)",
            entradas_indice_invertido
        )
        
        print(f"  [OK] Indexado DocId {doc_id}: '{titulo}' ({total_termos_significativos} termos)")

    # 6. Pós-Loop: Popula o Dicionário de Termos Global
    print("Populando Dicionário de Termos global...")
    entradas_dicionario = []
    for termo, doc_ids_set in documentos_por_termo_global.items():
        total_ocorrencias = ocorrencias_totais_global[termo]
        df = len(doc_ids_set) # Document Frequency
        entradas_dicionario.append((termo, total_ocorrencias, df))
        
    cursor.executemany(
        "INSERT INTO DicionarioTermos (Termo, TotalOcorrencias, DF) VALUES (?, ?, ?)",
        entradas_dicionario
    )
    print(f"Dicionário de Termos populado com {len(entradas_dicionario)} termos únicos.")

    # 7. Pós-Loop: Salva os Metadados da Coleção
    cursor.execute("INSERT INTO Metadados (Chave, Valor) VALUES (?, ?)", ('UltimoDocId', str(ultimo_doc_id)))
    cursor.execute("INSERT INTO Metadados (Chave, Valor) VALUES (?, ?)", ('TotalPalavras', str(total_palavras_colecao)))
    cursor.execute("INSERT INTO Metadados (Chave, Valor) VALUES (?, ?)", ('TotalDocumentos', str(total_documentos)))
    print("Metadados da coleção salvos.")

    # 8. Finaliza
    conn.commit()
    conn.close()
    print(f"\n[SUCESSO] Índice construído e salvo em '{CAMINHO_DB}'.")


if __name__ == "__main__":
    construir_indice()