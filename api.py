import os
import psycopg2
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="API de Livros",
    description="Uma API para consultar os livros do projeto de scraping."
)

def get_db_connection():
    """Tenta se conectar ao banco de dados PostgreSQL no Render."""
    try:
        conn_string = os.getenv('DATABASE_URL')
        if not conn_string:
            print("Erro: DATABASE_URL não foi encontrada.")
            return None
        conn = psycopg2.connect(conn_string)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Não foi possível conectar ao banco de dados: {e}")
        return None

def formatar_resultado(cursor, rows):
    """Converte o resultado do psycopg2 (lista de tuplas) em uma lista de dicionários."""
    colunas = [desc[0] for desc in cursor.description]
    return [dict(zip(colunas, row)) for row in rows]

# --- Endpoints da API ---

@app.get("/")
def read_root():
    """Endpoint inicial da API."""
    return {"Olá": "Bem-vindo à API de Livros! Acesse /docs para ver a documentação."}

@app.get("/livros")
def get_todos_os_livros():
    """Retorna uma lista de todos os livros da base de dados."""
    
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados.")
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, titulo, preco FROM livros;")
            livros = cur.fetchall()
            
            livros_formatados = formatar_resultado(cur, livros)
            return livros_formatados
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco: {e}")
    finally:
        conn.close()

@app.get("/livros/buscar")
def buscar_livro_por_nome(nome: str):
    """
    Busca livros que contenham o termo de pesquisa no título.
    Exemplo de uso: /livros/buscar?nome=Attic
    """
    
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco de dados.")
        
    try:
        with conn.cursor() as cur:
            termo_de_busca = f"%{nome}%"
            
            query = "SELECT id, titulo, preco FROM livros WHERE titulo ILIKE %s;"
            cur.execute(query, (termo_de_busca,))
            
            livros = cur.fetchall()
            livros_formatados = formatar_resultado(cur, livros)
            
            if not livros_formatados:
                return {"message": "Nenhum livro encontrado com esse termo."}
                
            return livros_formatados
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco: {e}")
    finally:
        conn.close()

print("API pronta para ser executada. Rode com: uvicorn api:app --reload")