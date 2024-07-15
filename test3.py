import mysql.connector
import pandas as pd

# Connexion à la base de données BASE1 (contenant T1)
conn_base1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="BASE1"
)

# Connexion à la base de données BASE2 (contenant T2)
conn_base2 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="BASE2"
)

# Connexion à la base de données BASE3 (contenant T3)
conn_base3 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="BASE3"
)

# Récupération des données de T1 et T2
query_t1 = "SELECT C2T1 FROM BASE1.T1"
query_t2 = "SELECT C2T2 FROM BASE2.T2"

# Charger les données dans des DataFrames pandas
df_t1 = pd.read_sql(query_t1, con=conn_base1)
df_t2 = pd.read_sql(query_t2, con=conn_base2)

# Fermer les connexions aux bases de données BASE1 et BASE2
conn_base1.close()
conn_base2.close()

# Manipulation des données pour remplir T3 dans BASE3
# Par exemple, remplir C1T3 à partir de C2T1 de T1
df_t3 = df_t1.copy()  # Copier les données de T1

df_t3['C1T3'] = df_t1['C2T1']  # Exemple hypothétique de calcul pour C1T3

# Remplir C2T2 de T3 à partir de C2T2 de T2
df_t3['C2T3'] = df_t2['C2T2']

# Écrire df_t3 dans la table T3 de BASE3
cursor = conn_base3.cursor()

# Suppression des données existantes dans T3
delete_query = "DELETE FROM T3"
cursor.execute(delete_query)

# Écriture des nouvelles données dans T3
for row in df_t3.itertuples(index=False):
    insert_query = "INSERT INTO T3 (C1T3, C2T3) VALUES (%s, %s)"
    cursor.execute(insert_query, (row.C1T3, row.C2T3))

# Valider et fermer la transaction
conn_base3.commit()

# Fermer la connexion à BASE3
conn_base3.close()

print("Les colonnes C1T3 et C2T2 de la Table T3 dans BASE3 ont été mises à jour avec succès.")
