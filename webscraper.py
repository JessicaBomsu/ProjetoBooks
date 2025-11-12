import requests
import time
import os
import psycopg2
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()


def get_db_connection():
    """Tenta se conectar ao banco de dados PostgreSQL no Render."""
    try:
        conn_string = os.getenv('DATABASE_URL')
        if not conn_string:
            print("Erro: DATABASE_URL não foi encontrada. Verifique seu arquivo .env")
            return None
            
        conn = psycopg2.connect(conn_string)
        print("Conexão com o banco de dados PostgreSQL (Render) bem-sucedida!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Não foi possível conectar ao banco de dados: {e}")
        return None

def setup_database(conn):
    """Cria a tabela 'livros' se ela ainda não existir."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS livros (
                id SERIAL PRIMARY KEY,
                titulo TEXT NOT NULL,
                preco REAL
            );
        """)
        cur.execute("TRUNCATE TABLE livros RESTART IDENTITY;")
        print("Tabela 'livros' verificada/criada e limpa.")
        conn.commit()

def insert_books_data(conn, livros_lista):
    """Insere a lista de livros no banco de dados de forma eficiente."""
    if not livros_lista:
        print("Nenhum dado para inserir.")
        return

    dados_para_inserir = [(livro['titulo'], livro['preco']) for livro in livros_lista]
    
    with conn.cursor() as cur:
        query = "INSERT INTO livros (titulo, preco) VALUES %s"
        
        execute_values(cur, query, dados_para_inserir)
        conn.commit()
    
    print(f"SUCESSO! {len(dados_para_inserir)} livros inseridos no banco de dados.")


def baixar_pagina(url):
    """Baixa o conteúdo HTML de uma URL e retorna um objeto 'soup'."""
    print(f"Baixando página: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status() 
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except requests.RequestException as e:
        print(f"Erro ao baixar a página: {e}")
        return None

def extrair_dados_livros(soup):
    """Encontra e extrai dados de todos os livros em um 'soup' de página."""
    livros_na_pagina = soup.find_all('article', class_='product_pod')
    print(f"Encontrados {len(livros_na_pagina)} livros nesta página.")
    
    dados_extraidos = []
    for livro in livros_na_pagina:
        titulo_tag = livro.find('h3').find('a')
        titulo = titulo_tag['title'].strip()
        
        preco_str = livro.find('p', class_='price_color').text.strip()
        preco = float(preco_str.replace('£', '')) 
        
        dados_extraidos.append({'titulo': titulo, 'preco': preco})
    
    return dados_extraidos

if __name__ == "__main__":
    
    conn = get_db_connection()
    
    if conn is None:
        print("Encerrando o script. Verifique a conexão com o banco.")
        exit()
    
    try:
        setup_database(conn)
        
        print("\n--- INICIANDO SCRAPING ---")
        todos_os_livros = []
        url_atual = "https://books.toscrape.com/"
        
        while url_atual:
            soup_da_pagina = baixar_pagina(url_atual)
            
            if soup_da_pagina:
                dados_da_pagina = extrair_dados_livros(soup_da_pagina)
                todos_os_livros.extend(dados_da_pagina)
                
                next_tag = soup_da_pagina.find('li', class_='next')
                if next_tag:
                    link_relativo = next_tag.find('a')['href']
                    url_atual = urljoin(url_atual, link_relativo)
                    time.sleep(0.5) 
                else:
                    print("Não há mais páginas. Encerrando o scraping.")
                    url_atual = None
            else:
                print("Falha ao baixar a página. Abortando.")
                url_atual = None
        
        print(f"--- SCRAPING COMPLETO ---")
        print(f"Total de livros encontrados: {len(todos_os_livros)}")

        if todos_os_livros:
            print("\n--- INICIANDO INSERÇÃO NO BANCO DE DADOS ---")
            insert_books_data(conn, todos_os_livros)
        
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Conexão com o PostgreSQL fechada.")