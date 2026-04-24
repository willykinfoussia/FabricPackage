# fabrictools

Bibliotheque Python pour simplifier le travail de donnees dans Microsoft Fabric.  
Vous utilisez des fonctions courtes pour lire, nettoyer, fusionner et publier vos tables, sans gerer des chemins techniques complexes.

Documentation detaillee (reference API et guides Sphinx) : [https://willykinfoussia.github.io/FabricPackage/](https://willykinfoussia.github.io/FabricPackage/)

---

## Table des matieres

- [Pourquoi utiliser fabrictools](#pourquoi-utiliser-fabrictools)
- [Prerequis](#prerequis)
- [Installation](#installation)
- [Documentation en ligne](#documentation-en-ligne)
- [Premiers pas (5 minutes)](#premiers-pas-5-minutes)
- [Tutoriel interactif : projet fictif NovaRetail](#tutoriel-interactif--projet-fictif-novaretail)
- [Index rapide : toutes les fonctions publiques](#index-rapide--toutes-les-fonctions-publiques)
- [Transform DataFrame (filtre / jointure)](#transform-dataframe)
- [FAQ](#faq)
- [Support](#support)
- [Ressources mainteneur](#ressources-mainteneur)
- [Licence](#licence)

---

## Pourquoi utiliser fabrictools

- Vous passez le nom du Lakehouse/Warehouse, pas une URL longue.
- Vous avez des operations courantes pretes a l'emploi (read, write, merge, clean).
- Vous pouvez lancer un pipeline de preparation en plusieurs etapes claires.
- Vous disposez d’aides generiques sur DataFrame (filtrer par liste de valeurs, jointure avec colonnes prefixees).
- Vous gagnez du temps avec des fonctions d'orchestration (table unique ou bulk).
- Vous gardez un code notebook lisible pour toute l'equipe.

---

## Prerequis

- Python `>= 3.9`
- Un environnement Microsoft Fabric (recommande)
- Un notebook attache a un Lakehouse pour les operations Lakehouse

Bon a savoir :

- Dans Fabric, `pyspark` et `delta-spark` sont deja disponibles.
- Hors Fabric, certaines fonctions de resolution de chemins peuvent echouer (ex: absence de `notebookutils`).

---

## Installation

```bash
# Cas standard (notebook Fabric)
pip install fabrictools

# Cas local avec Spark + Delta
pip install "fabrictools[spark]"

# Option visualisation (graphiques pour scan qualite)
pip install "fabrictools[visualization]"
```

---

## Documentation en ligne

- Site Sphinx (API, guides) : [https://willykinfoussia.github.io/FabricPackage/](https://willykinfoussia.github.io/FabricPackage/)

---

## Premiers pas (5 minutes)

```python
import fabrictools as ft

# Lire une table/fichier depuis un Lakehouse
df = ft.read_lakehouse("BronzeLakehouse", "dbo/orders")
df.show(5)
```

Ensuite, vous pouvez faire :

1. Nettoyer les donnees (`clean_data`)
2. Ajouter des metadonnees (`add_silver_metadata`)
3. Ecrire vers un Lakehouse cible (`write_lakehouse`)

---

## Tutoriel interactif : projet fictif NovaRetail

Objectif : partir de donnees brutes de ventes et finir avec des tables preparees pour le reporting.

### Vue d'ensemble (visuel)

```mermaid
flowchart LR
    sourceLakehouse["BronzeLakehouse (brut)"] --> cleanStep["Nettoyage"]
    cleanStep --> silverStep["Enrichissement Silver"]
    silverStep --> curatedLakehouse["SilverLakehouse (curated)"]
    curatedLakehouse --> preparedStep["Preparation semantique"]
    preparedStep --> preparedLakehouse["PreparedLakehouse"]
    preparedLakehouse --> warehouseStep["Warehouse + BI"]
```



### Etape 1 - Lire les ventes brutes

```python
import fabrictools as ft

orders_raw = ft.read_lakehouse("BronzeLakehouse", "dbo/orders_raw")
orders_raw.show(5)
```

### Etape 2 - Nettoyer les donnees

```python
orders_clean = ft.clean_data(orders_raw)
```

### Etape 3 - Enrichir en metadonnees Silver

```python
orders_silver = ft.add_silver_metadata(
    orders_clean,
    source_lakehouse_name="BronzeLakehouse",
    source_relative_path="dbo/orders_raw",
    source_layer="bronze",
)
```

### Etape 4 - Ecrire en Silver

```python
ft.write_lakehouse(
    orders_silver,
    lakehouse_name="SilverLakehouse",
    relative_path="dbo/orders",
    mode="overwrite",
    partition_by=["year", "month", "day"],
)
```

### Etape 5 - Scanner la qualite

```python
quality = ft.scan_data_errors(orders_silver, include_samples=True, display_results=True)
quality["summary_df"].show(truncate=False)
```

### Etape 6 - Fusion incrementale (upsert)

```python
orders_updates = ft.read_lakehouse("BronzeLakehouse", "dbo/orders_updates")

ft.merge_lakehouse(
    source_df=orders_updates,
    lakehouse_name="SilverLakehouse",
    relative_path="dbo/orders",
    merge_condition="src.order_id = tgt.order_id",
)
```

### Etape 7 - Ecriture dans un Warehouse

```python
ft.write_warehouse(
    df=orders_silver,
    warehouse_name="RetailWarehouse",
    table="dbo.orders",
    mode="overwrite",
)
```

### Etape 8 - Pipeline prepare (table unique)

```python
prepared_df = ft.prepare_and_write_data(
    source_lakehouse_name="SilverLakehouse",
    source_relative_path="Tables/dbo/orders",
    target_lakehouse_name="PreparedLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
    mode="overwrite",
)
```

### Etape 9 - Pipeline prepare (bulk)

```python
bulk_result = ft.prepare_and_write_all_tables(
    source_lakehouse_name="SilverLakehouse",
    target_lakehouse_name="PreparedLakehouse",
    include_schemas=["dbo"],
    continue_on_error=True,
)
print(bulk_result["successful_tables"], bulk_result["failed_tables"])
```

### Etape 10 - Dimensions pour reporting

```python
dims = ft.generate_dimensions(
    lakehouse_name="PreparedLakehouse",
    warehouse_name="RetailWarehouse",
    include_date=True,
    include_country=True,
    include_city=True,
)
```

---

## Index rapide : toutes les fonctions publiques

Chaque fonction ci-dessous est exportee directement depuis `import fabrictools as ft`.

### Lakehouse

#### `read_lakehouse`

```python
df = ft.read_lakehouse("BronzeLakehouse", "dbo/customers")
```

#### `write_lakehouse`

```python
ft.write_lakehouse(df, "SilverLakehouse", "dbo/customers", mode="overwrite")
```

#### `merge_lakehouse`

```python
ft.merge_lakehouse(
    source_df=df_updates,
    lakehouse_name="SilverLakehouse",
    relative_path="dbo/customers",
    merge_condition="src.customer_id = tgt.customer_id",
)
```

#### `delete_all_lakehouse_tables`

```python
ft.delete_all_lakehouse_tables(
    lakehouse_name="SandboxLakehouse",
    include_schemas=["dbo"],
    dry_run=True,
)
```

#### `clean_data`

```python
df_clean = ft.clean_data(df)
```

#### `add_silver_metadata`

```python
df_silver = ft.add_silver_metadata(df_clean, "BronzeLakehouse", "dbo/customers_raw")
```

#### `scan_data_errors`

```python
scan = ft.scan_data_errors(df_silver, include_samples=True, display_results=False)
scan["summary_df"].show()
```

#### `clean_and_write_data`

```python
df_out = ft.clean_and_write_data(
    source_lakehouse_name="BronzeLakehouse",
    source_relative_path="dbo/customers_raw",
    target_lakehouse_name="SilverLakehouse",
    target_relative_path="dbo/customers",
    mode="overwrite",
)
```

#### `clean_and_write_all_tables`

```python
result = ft.clean_and_write_all_tables(
    source_lakehouse_name="BronzeLakehouse",
    target_lakehouse_name="SilverLakehouse",
    include_schemas=["dbo"],
    continue_on_error=True,
)
```

### Warehouse

#### `read_warehouse`

```python
df_wh = ft.read_warehouse("RetailWarehouse", "SELECT TOP 100 * FROM dbo.orders")
```

#### `write_warehouse`

```python
ft.write_warehouse(df_wh, warehouse_name="RetailWarehouse", table="dbo.orders_snapshot", mode="append")
```

### Dimensions

#### `build_dimension_date`

```python
dim_date = ft.build_dimension_date(start_date="2020-01-01", end_date="2030-12-31")
```

#### `build_dimension_country`

```python
dim_country = ft.build_dimension_country(countries_limit=100)
```

#### `build_dimension_city`

```python
dim_city = ft.build_dimension_city(
    regions=["Europe"],
    countries=["FR", "DEU", "Belgium"],
)
```

#### `generate_dimensions`

```python
all_dims = ft.generate_dimensions(
    lakehouse_name="PreparedLakehouse",
    warehouse_name="RetailWarehouse",
    include_date=True,
    include_country=True,
    include_city=True,
)
```

### Source -> Prepared

#### `snapshot_source_schema`

```python
schema_hash = ft.snapshot_source_schema("SilverLakehouse", "Tables/dbo/orders")
```

#### `resolve_columns`

```python
mappings = ft.resolve_columns(
    df=orders_silver,
    source_lakehouse_name="SilverLakehouse",
    schema_hash=schema_hash,
)
```

#### `transform_to_prepared`

```python
prepared_df = ft.transform_to_prepared(
    df=orders_silver,
    resolved_mappings=mappings,
    source_lakehouse_name="SilverLakehouse",
)
```

#### `write_prepared_table`

```python
ft.write_prepared_table(
    df=prepared_df,
    resolved_mappings=mappings,
    target_lakehouse_name="PreparedLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
    mode="overwrite",
)
```

#### `generate_prepared_aggregations`

```python
agg_tables = ft.generate_prepared_aggregations(
    source_lakehouse_name="SilverLakehouse",
    target_lakehouse_name="PreparedLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
    resolved_mappings=mappings,
)
```

#### `publish_semantic_model`

```python
publish_result = ft.publish_semantic_model(
    target_lakehouse_name="PreparedLakehouse",
    agg_tables=agg_tables,
    resolved_mappings=mappings,
    semantic_workspace="<workspace-id-ou-nom>",
    semantic_model_name="novaretail_dataset",
)
```

#### `prepare_and_write_data`

```python
one_table = ft.prepare_and_write_data(
    source_lakehouse_name="SilverLakehouse",
    source_relative_path="Tables/dbo/orders",
    target_lakehouse_name="PreparedLakehouse",
    target_relative_path="Tables/dbo/orders_prepared",
)
```

#### `prepare_and_write_all_tables`

```python
all_tables = ft.prepare_and_write_all_tables(
    source_lakehouse_name="SilverLakehouse",
    target_lakehouse_name="PreparedLakehouse",
    include_schemas=["dbo"],
    continue_on_error=True,
)
```

#### `make_business_ready`

Transforme les tables pour le métier : renommage des colonnes (`snake_case` -> `Normal Case`), nettoyage du nom des tables (retrait de Cleaned/Processed et conversion en PascalCase) et mise à jour des métadonnées d'ingestion.

```python
business_result = ft.make_business_ready(
    source_lakehouse_name="SilverLakehouse",
    target_lakehouse_name="GoldLakehouse",
    tables=["Tables/dbo/Cleaned_orders", "Tables/dbo/Processed_clients"],
    custom_table_names={"Tables/dbo/Cleaned_orders": "CommandesBusiness"},
)
```

### Transform (DataFrame)

Helpers reutilisables **DataFrame → DataFrame** (notebooks, Bronze/Silver/Gold). Pour `merge_dataframes`, le **prefixe** des colonnes ajoutees suit l’ordre : nom de variable `join_df` a l’appel si l’introspection reussit, sinon **alias** logique Spark du DataFrame de droite (ex. `join_df.alias("projets")`), sinon la valeur par defaut `join` ; vous pouvez forcer avec `join_prefix=...`. Les suffixes sont **normalises** (snake_case, comme `clean_data`).

#### `build_tcd`

Simule la création d'un Tableau Croisé Dynamique (TCD) à la manière d'Excel, en spécifiant les lignes, colonnes, valeurs à agréger (via un nom ou un dictionnaire), filtres optionnels, et renommage des colonnes.

```python
tcd_df = ft.build_tcd(
    df,
    rows="Region",
    columns="Year",
    values={"Sales": "sum"},
    filters="Product IN ('A', 'C') AND Year > 2022",
    custom_names={"2023": "Year 2023", "2024": "Year 2024"}
)
```

#### `filter_by_value_list`

Filtre sur une colonne et une liste de valeurs : **pas de cast** ; **trim** uniquement si la colonne est de type chaine ; les `str` dans la liste sont `strip()`’es. Avec `exclude=True` (defaut), les lignes dont la valeur est dans la liste sont exclues.

```python
df2 = ft.filter_by_value_list(df, "Compte", ("70830000", "70840000"), exclude=True)
```

#### `merge_dataframes`

Joint `main` a `join_df` sur une ou plusieurs paires de cles `(colonne_main, colonne_droite)` ; apporte les colonnes listees dans `join_columns`, renommees en `{prefix_snake}_{colonne_snake_unique}` (prefixe = nom de variable a l’appel, sinon alias Spark du `join_df`, sinon `join`, ou `join_prefix="..."` pour forcer).

```python
out = ft.merge_dataframes(
    main=detail,
    join_df=projets,
    join_columns=["Client", "Type projet", "Nom client"],
    keys=[("Code projet", "ID projet")],
    how="left",
)
# Ex. colonnes : projets_client, projets_type_projet, projets_nom_client
```

#### `drop_rows_over_empty_percent`

Supprime les lignes dont la part de cellules vides (null ou chaine vide apres trim, voir `empty_or_null`) sur les colonnes evaluees est **strictement superieure** a `max_empty_percent` (`0` a `1`).

```python
df_kept = ft.drop_rows_over_empty_percent(df, 0.5)
```

#### `remove_columns`

Retire une ou plusieurs colonnes ; les noms sont resolus comme pour `merge_dataframes` (nom physique, libelle normalise ou snake_case).

```python
df2 = ft.remove_columns(df, "Colonne inutile")
```

#### `rename_columns_normalized`

Renomme toutes les colonnes en snake_case avec des suffixes `_2`, `_3`, … si besoin (meme logique que l’etape de renommage de `clean_data`).

```python
df2 = ft.rename_columns_normalized(df)
```

#### `rename_columns_pq_serial_to_dates`

Detecte les noms de colonnes contenant un numero de serie Excel / Power Query et les renomme avec une date (`strftime` configurable).

```python
df2 = ft.rename_columns_pq_serial_to_dates(df, date_format="%Y-%m-%d")
```

#### `rename_columns_pq_serial_to_mois_annee`

Comme `rename_columns_pq_serial_to_dates`, avec des etiquettes *mois annee* en francais (ex. `janvier_2024`).

```python
df2 = ft.rename_columns_pq_serial_to_mois_annee(df)
```

#### `rename_columns_month_year_block_labels`

Repere des blocs contigus de colonnes *mois annee* (francais) et ajoute un marqueur de bloc dans le nom (libelles ordonnes par defaut pour des projections type couts / CA).

```python
df2 = ft.rename_columns_month_year_block_labels(df)
```

#### `month_start_from_ca_monthly_col`

Parse le premier jour du mois a partir d’un nom de colonne large (tete *mois annee* francais, suffixe optionnel `[label]`).

```python
m = ft.month_start_from_ca_monthly_col("janvier_2024 [CA Monthly]")
```

#### `resolve_dataframe_column`

Retourne le nom physique d’une colonne a partir d’un libelle logique, normalise comme apres `clean_data`, ou snake_case.

```python
col = ft.resolve_dataframe_column(df, "Nom affiche")
```

#### `wide_value_columns`

Liste les colonnes dont le nom se termine par un suffixe donne (ex. bloc `[CA Monthly]`), hors exclusions.

```python
value_cols = ft.wide_value_columns(df, suffix=" [CA Monthly]")
```

#### `dataframe_unpivot_wide_month_suffix`

Passe du format large (plusieurs colonnes mois) au format long : identifiants + colonne variable + valeur + `MonthStart` deduit du nom de colonne.

```python
long_df = ft.dataframe_unpivot_wide_month_suffix(
    df,
    id_columns=["Projet"],
    value_columns_suffix=" [CA Monthly]",
)
```

#### `dataframe_last_nonnull_wide_month_from_long`

Sur un long (ex. sortie d’`dataframe_unpivot_wide_month_suffix`), pour chaque mois variable garde la ligne avec le plus grand `order_column` ou la valeur est non nulle ; produit annee, mois et valeur.

```python
last_df = ft.dataframe_last_nonnull_wide_month_from_long(
    long_df,
    order_column="DateReference",
)
```

#### `dataframe_pivot_category_wide_month_from_long`

A partir d’un long avec categorie, somme les mesures par mois et pivote les categories en colonnes larges (`Year`, `Month`, une colonne par categorie).

```python
wide_df = ft.dataframe_pivot_category_wide_month_from_long(
    long_df,
    category_column="TypeCout",
    pivot_categories=["Prevu", "Reel"],
)
```

#### `transform_wide_month_suffix`

Enchaine unpivot puis soit `last_nonnull` (avec `order_column`), soit `pivot_sum` (avec `category_column` et `pivot_categories`).

```python
out = ft.transform_wide_month_suffix(
    df,
    id_columns=["Projet"],
    aggregation="last_nonnull",
    value_columns_suffix=" [CA Monthly]",
    order_column="DateOrdre",
)
```

#### `norm_text`

Expression Spark : chaine en minuscules, caracteres de controle retires, espaces supprimes (style Power Query `Text.Clean`).

```python
from pyspark.sql import functions as F

df2 = df.withColumn("cle", ft.norm_text(F.col("Libelle")))
```

#### `empty_or_null`

Expression Spark : vrai si null ou chaine vide apres cast string et trim (utilise par `drop_rows_over_empty_percent`).

```python
from pyspark.sql import functions as F

df2 = df.filter(~ft.empty_or_null(F.col("Commentaire")))
```

#### `coalesce_dim`

Cast string ; null ou vide devient la chaine `"0"` (pratique pour des cles dimension).

```python
from pyspark.sql import functions as F

df2 = df.withColumn("id_dim", ft.coalesce_dim(F.col("code")))
```

---

## FAQ

### 1) Est-ce que je peux utiliser fabrictools sans Microsoft Fabric ?

Partiellement oui. Les fonctions purement Spark peuvent marcher en local avec `fabrictools[spark]`, mais les fonctions de resolution de chemins Lakehouse dependent de `notebookutils` (disponible dans Fabric).

### 2) Y a-t-il une commande CLI (`fabrictools ...`) ?

Non. L'usage est en Python, via `import fabrictools as ft`.

### 3) Plotly est-il obligatoire ?

Non. C'est utile pour les graphiques de `scan_data_errors`. Sans Plotly, vous gardez la partie tabulaire.

### 4) Comment choisir entre `clean_and_write_data` et `clean_and_write_all_tables` ?

- `clean_and_write_data` : une table cible
- `clean_and_write_all_tables` : plusieurs tables en lot

### 5) `delete_all_lakehouse_tables` est-il dangereux ?

Oui, c'est une action destructive. Commencez avec `dry_run=True` pour verifier la liste avant suppression.

### 6) Je debute : quel chemin minimum recommandez-vous ?

`read_lakehouse` -> `clean_data` -> `add_silver_metadata` -> `write_lakehouse`.

---

## Support

- Documentation : [https://willykinfoussia.github.io/FabricPackage/](https://willykinfoussia.github.io/FabricPackage/)
- Ouvrir une issue GitHub : [Issues](https://github.com/willykinfoussia/FabricPackage/issues)
- Consulter le depot : [Repository](https://github.com/willykinfoussia/FabricPackage)

Pour aider rapidement, partagez :

- la fonction utilisee
- un exemple de parametres
- le message d'erreur complet

---

## Licence

MIT