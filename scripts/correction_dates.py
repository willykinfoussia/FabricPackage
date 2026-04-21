from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Extraction de la partie numérique de n_commande pour le tri
df = defaults.withColumn("_n_ord", F.regexp_extract(F.col("n_commande"), r"^\*?(\d+)$", 1).cast("int"))

# 2. DataFrame agrégé : 1 ligne par site et par numéro de commande unique
# On prend la date la PLUS RÉCENTE (max) pour chaque commande
orders = df.filter(F.col("_n_ord").isNotNull()) \
           .groupBy("site", "_n_ord") \
           .agg(
               F.max("cree").alias("ord_cree")
           )

# 3. Récupération de la date de la commande PRÉCÉDENTE et SUIVANTE (au niveau de la commande, pas de la ligne)
w_orders = Window.partitionBy("site").orderBy("_n_ord")
orders = orders.withColumn("prev_ord_cree", F.lag("ord_cree").over(w_orders)) \
               .withColumn("next_ord_cree", F.lead("ord_cree").over(w_orders))

# 4. Jointure des infos sur les commandes avant/après vers notre dataframe détaillé (lignes)
df = df.join(orders.select("site", "_n_ord", "prev_ord_cree", "next_ord_cree"), on=["site", "_n_ord"], how="left")

# 5. Conditions pour la correction
# Est-ce que la COMMANDE précédente et la COMMANDE suivante ont le même mois/année ?
same_surrounding = (F.year("prev_ord_cree") == F.year("next_ord_cree")) & (F.month("prev_ord_cree") == F.month("next_ord_cree"))

# Est-ce que la LIGNE actuelle a un mois différent de ces commandes environnantes ?
is_different = (F.year("cree") != F.year("prev_ord_cree")) | (F.month("cree") != F.month("prev_ord_cree"))

# L'anomalie est confirmée
is_anomaly = same_surrounding & is_different

# 6. Construction de la date corrigée
# On prend le mois de l'environnement, et on plafonne le jour au dernier jour valide
corrected_date = F.make_date(
    F.year("prev_ord_cree"), 
    F.month("prev_ord_cree"), 
    F.least(F.dayofmonth("cree"), F.dayofmonth(F.last_day("prev_ord_cree")))
)

# 7. Application de la correction
result = df.withColumn(
    "cree_corrige",
    F.when(is_anomaly, corrected_date).otherwise(F.col("cree"))
)

# 8. Nettoyage des colonnes temporaires
result = result.drop("_n_ord", "prev_ord_cree", "next_ord_cree")

display(result)