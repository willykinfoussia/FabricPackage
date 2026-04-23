def filter_invalid_commands(df, n_commande_col="n_commande"):
    """
    Supprime toutes les lignes dont le numéro de commande ne correspond pas
    au format r"^\*?(\d+)$" (optionnellement une étoile, suivie uniquement de chiffres).
    """
    return df.filter(F.col(n_commande_col).rlike(r"^\*?(\d+)$"))

def process_command_dates(
    defaults,
    site_col="site",
    n_commande_col="n_commande",
    cree_col="cree"):
    """
    Corrige la colonne 'cree' en fonction des commandes précédentes et suivantes
    pour les anomalies sur les dates de prise de commande.

    Args:
        defaults (DataFrame): DataFrame Spark d'entrée avec au minimum les colonnes
                              'n_commande', 'cree', et 'site'
        site_col (str): Nom de la colonne du site
        n_commande_col (str): Nom de la colonne du numéro de commande
        cree_col (str): Nom de la colonne de date de création (timestamp)

    Returns:
        DataFrame Spark avec une colonne 'cree_fixed' (date potentiellement corrigée)
    """

    # 1. Extraction de la partie numérique de n_commande pour le tri
    df = defaults.withColumn(
        "_n_ord", F.regexp_extract(F.col(n_commande_col), r"^\*?(\d+)$", 1).cast("int")
    )

    # 2. DataFrame agrégé : 1 ligne par site et par numéro de commande unique
    # On prend la date la PLUS RÉCENTE (max) pour chaque commande
    orders = (
        df.filter(F.col("_n_ord").isNotNull())
        .groupBy(site_col, "_n_ord")
        .agg(F.max(cree_col).alias("ord_cree"))
    )

    # 3. Récupération de la date de la commande PRÉCÉDENTE et SUIVANTE (commande, pas ligne)
    w_orders = Window.partitionBy(site_col).orderBy("_n_ord")
    orders = (
        orders.withColumn("prev_ord_cree", F.lag("ord_cree").over(w_orders))
        .withColumn("next_ord_cree", F.lead("ord_cree").over(w_orders))
    )

    # 4. Jointure des infos sur les commandes avant/après vers notre dataframe détaillé (lignes)
    df = df.join(
        orders.select(site_col, "_n_ord", "prev_ord_cree", "next_ord_cree"),
        on=[site_col, "_n_ord"],
        how="left"
    )

    # 5. Conditions pour la correction
    # Est-ce que la COMMANDE précédente et la COMMANDE suivante ont le même mois/année ?
    same_surrounding = (
        (F.year("prev_ord_cree") == F.year("next_ord_cree")) &
        (F.month("prev_ord_cree") == F.month("next_ord_cree"))
    )

    # Est-ce que la LIGNE actuelle a un mois différent de ces commandes environnantes ?
    is_different = (
        (F.year(cree_col) != F.year("prev_ord_cree")) |
        (F.month(cree_col) != F.month("prev_ord_cree"))
    )

    # L'anomalie est confirmée
    is_anomaly = same_surrounding & is_different

    # 6. Construction de la date corrigée
    # On prend le mois de l'environnement, et on plafonne le jour au dernier jour valide
    corrected_date = F.make_date(
        F.year("prev_ord_cree"),
        F.month("prev_ord_cree"),
        F.least(F.dayofmonth(cree_col), F.dayofmonth(F.last_day("prev_ord_cree")))
    )

    # 7. Application de la correction
    # Si la date corrigée est supérieure à la date de création originale, on annule la modification
    df = df.withColumn(
        "cree_fixed",
        F.when(is_anomaly & (corrected_date <= F.col(cree_col)), corrected_date).otherwise(F.col(cree_col))
    )

    # 8. Nettoyage des colonnes temporaires
    df = df.drop("_n_ord", "prev_ord_cree", "next_ord_cree")

    return df

df_corrige = process_command_dates(defaults)
display(df_corrige)