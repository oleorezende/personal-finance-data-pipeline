# %%
import pandas as pd
import glob
import sqlite3

# caminho dos arquivos
arquivos = glob.glob(r"C:\Users\Usuario\Desktop\gastos\*.csv")

print(f"Arquivos encontrados: {len(arquivos)}")
print(arquivos)

# juntar todos os csvs
df = pd.concat([pd.read_csv(arq) for arq in arquivos], ignore_index=True)

# padronização
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["title"] = df["title"].astype(str).str.strip().str.lower()
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

# criar colunas úteis
df["mes"] = df["date"].dt.month
df["ano"] = df["date"].dt.year
df["mes_ano"] = df["date"].dt.to_period("M").astype(str)

# categorização automática
def categorizar(titulo):
    titulo = str(titulo).lower()

    if "ifood" in titulo or "pizza" in titulo or "restaurante" in titulo or "japanese" in titulo or "pao" in titulo:
        return "Alimentacao"
    elif "uber" in titulo or "99" in titulo or "posto" in titulo or "moto" in titulo:
        return "Transporte"
    elif "cinemark" in titulo or "netflix" in titulo or "spotify" in titulo:
        return "Lazer"
    elif "farmacia" in titulo or "droga" in titulo:
        return "Saude"
    elif "mercado" in titulo or "supermercado" in titulo:
        return "Supermercado"
    else:
        return "Outros"

df["categoria"] = df["title"].apply(categorizar)

# análises
gastos_categoria = df.groupby("categoria")["amount"].sum().sort_values(ascending=False)
gastos_mes = df.groupby("mes_ano")["amount"].sum().sort_index()
negativos = df[df["amount"] < 0]

print("\n=== Gastos por categoria ===")
print(gastos_categoria)

print("\n=== Gastos por mês ===")
print(gastos_mes)

print("\n=== Valores negativos ===")
print(negativos.head(20))

# salvar csv tratado
df.to_csv(r"C:\Users\Usuario\Desktop\gastos\gastos_tratados.csv", index=False)
print("\nCSV salvo com sucesso")

# salvar em banco sqlite
conn = sqlite3.connect(r"C:\Users\Usuario\Desktop\gastos\gastos.db")
df.to_sql("gastos", conn, if_exists="replace", index=False)
print("Banco criado com sucesso")

# consultas SQL
query_categoria = """
SELECT categoria, ROUND(SUM(amount), 2) AS total
FROM gastos
GROUP BY categoria
ORDER BY total DESC
"""

query_mes = """
SELECT mes_ano, ROUND(SUM(amount), 2) AS total
FROM gastos
GROUP BY mes_ano
ORDER BY mes_ano
"""

query_top = """
SELECT date, title, categoria, amount
FROM gastos
ORDER BY amount DESC
LIMIT 10
"""

print("\n=== SQL: Categoria ===")
print(pd.read_sql(query_categoria, conn))

print("\n=== SQL: Mês ===")
print(pd.read_sql(query_mes, conn))

print("\n=== SQL: Top gastos ===")
print(pd.read_sql(query_top, conn))
# %%
