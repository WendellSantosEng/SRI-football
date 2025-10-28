import re
import os
from typing import List

# -----------------------------------------------------------------
# IMPORTANTE: Carregamento da Lista de Stop-words
# -----------------------------------------------------------------

# Define o caminho do arquivo de stop-words
# __file__ é o caminho deste arquivo (processador.py)
# os.path.dirname(__file__) é a pasta 'src/pipeline/'
# os.path.abspath() resolve o caminho completo
# Vamos "subir" dois níveis (de 'src/pipeline/' para 'sri_projeto/') 
# e depois entrar em 'data/'.
CAMINHO_BASE_PROJETO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
CAMINHO_LISTA_PROFESSOR = os.path.join(CAMINHO_BASE_PROJETO, 'data', 'stopwords.txt')


def carregar_stopwords_do_arquivo(caminho_arquivo: str) -> set:
    """
    Lê um arquivo .txt de stop-words (uma por linha) e retorna um set.
    Se o arquivo não for encontrado, levanta um erro.
    """
    if not os.path.exists(caminho_arquivo):
        # Se o arquivo não existir, falha imediatamente e avisa o usuário.
        raise FileNotFoundError(
            f"\n\nERRO: O arquivo de stop-words não foi encontrado."
            f"\nCaminho esperado: {caminho_arquivo}"
            f"\nCertifique-se de que o arquivo 'stopwords.txt' está na pasta 'data/'.\n"
        )
        
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            # .strip() remove espaços em branco/quebras de linha
            # .lower() garante que a stop-word esteja em minúsculas
            palavras = [linha.strip().lower() for linha in f if linha.strip()]
            if not palavras:
                # O arquivo existe, mas está vazio.
                print(f"AVISO: O arquivo de stop-words '{caminho_arquivo}' foi encontrado, mas está vazio.")
            return set(palavras)
    except Exception as e:
        print(f"Erro ao ler o arquivo de stopwords: {e}")
        return set()

# --- Carregamento Principal ---
# Agora, o script VAI falhar se o arquivo não for encontrado.
STOP_WORDS = carregar_stopwords_do_arquivo(CAMINHO_LISTA_PROFESSOR)
print(f"Sucesso: {len(STOP_WORDS)} stop-words carregadas de '{CAMINHO_LISTA_PROFESSOR}'.")

# -----------------------------------------------------------------

def processar(texto: str) -> List[str]:
    """
    Processa um texto bruto e retorna uma lista de tokens (palavras) significativos.
    
    Etapas do Pipeline:
    1. Tokenização: Separa em palavras, mantendo termos com hífen (ex: 'palavra-chave').
    2. Normalização: Converte tudo para minúsculas.
    3. Filtragem: Remove stop-words e tokens não-alfabéticos (números, pontuações).
    """
    
    # 1. Tokenização (RegEx)
    # [\w-]+ -> Encontra qualquer sequência de caracteres de palavra (letras, 
    #              acentos, números, _) OU hífens.
    tokens_brutos = re.findall(r'[\w-]+', texto)
    
    tokens_limpos = []
    for token in tokens_brutos:
        # 2. Normalização (minúsculas)
        token_lower = token.lower()
        
        # 3. Filtragem
        # Condição 1: O token não pode ser uma stop-word
        if token_lower not in STOP_WORDS:
            # Condição 2: O token deve conter pelo menos UMA letra.
            # Isso remove números puros ('12345'), hífens isolados ('-')
            if any(c.isalpha() for c in token_lower):
                tokens_limpos.append(token_lower)
                
    return tokens_limpos

# -----------------------------------------------------------------
# Bloco de Teste
# -----------------------------------------------------------------
# Rode este arquivo diretamente no terminal para testar:
# python src/pipeline/processador.py
# (Se 'data/stopwords.txt' não existir, ele vai falhar e te avisar.)
# -----------------------------------------------------------------
if __name__ == "__main__":
    
    print(f"\n--- Testando o processador.py ---")
    
    # Para teste, vamos simular que 'exemplo' e 'teste' são stop-words
    # (Adicionamos ao set apenas para este teste)
    STOP_WORDS.add('exemplo')
    STOP_WORDS.add('teste')
    
    texto_exemplo = (
        "Este é um RESUMO de exemplo sobre Organização e Recuperação de Informação (ORI). "
        "O objetivo é testar o processador de texto, removendo stop-words como 'a', 'o', 'de', "
        "e mantendo termos como palavra-chave. "
        "Números como 12345 ou 2025 devem ser removidos. Um hífen isolado - também."
    )
    
    tokens = processar(texto_exemplo)
    
    print("\n--- Texto Original ---")
    print(texto_exemplo)
    
    print("\n--- Tokens Processados ---")
    print(tokens)
    
    # Verificações esperadas
    print("\n--- Verificações ---")
    assert "resumo" in tokens, "Falha: 'resumo' deveria estar nos tokens"
    assert "organização" in tokens, "Falha: 'organização' deveria estar nos tokens"
    assert "palavra-chave" in tokens, "Falha: 'palavra-chave' deveria estar nos tokens"
    assert "ori" in tokens, "Falha: 'ori' deveria estar nos tokens"
    print("OK: Termos principais mantidos.")
    
    # Testa as stop-words que foram adicionadas manualmente
    assert "exemplo" not in tokens, "Falha: 'exemplo' (stop-word) deveria ser removida"
    assert "teste" not in tokens, "Falha: 'teste' (stop-word) deveria ser removida"
    print("OK: Stop-words de teste removidas.")
    
    assert "12345" not in tokens, "Falha: '12345' (número) deveria ser removido"
    assert "2025" not in tokens, "Falha: '2025' (número) deveria ser removido"
    assert "-" not in tokens, "Falha: '-' (hífen isolado) deveria ser removido"
    print("OK: Tokens não-alfabéticos removidos.")
    
    print("\n[SUCESSO] O processador.py passou em todos os testes.")