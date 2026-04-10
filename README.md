# fabrictools

Bibliotheque Python pour simplifier le travail de donnees dans Microsoft Fabric.  
Vous utilisez des fonctions courtes pour lire, nettoyer, fusionner et publier vos tables, sans gerer des chemins techniques complexes.

---

## Table des matieres

- [Pourquoi utiliser fabrictools](#pourquoi-utiliser-fabrictools)
- [Prerequis](#prerequis)
- [Installation](#installation)
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

### Transform (DataFrame)

Helpers reutilisables **DataFrame → DataFrame** (notebooks, Bronze/Silver/Gold). Pour `merge_dataframes`, le **prefixe** des colonnes ajoutees est deduit du **nom de la variable** `join_df` a l’appel (ou `join_prefix=...`) ; les suffixes sont **normalises** (snake_case, comme `clean_data`). Pas besoin de `.alias()` Spark sur le DataFrame de droite.

#### `filter_by_value_list`

Filtre sur une colonne et une liste de valeurs : **pas de cast** ; **trim** uniquement si la colonne est de type chaine ; les `str` dans la liste sont `strip()`’es. Avec `exclude=True` (defaut), les lignes dont la valeur est dans la liste sont exclues.

```python
df2 = ft.filter_by_value_list(df, "Compte", ("70830000", "70840000"), exclude=True)
```

#### `merge_dataframes`

Joint `main` a `join_df` sur une ou plusieurs paires de cles `(colonne_main, colonne_droite)` ; apporte les colonnes listees dans `join_columns`, renommees en `{prefix_snake}_{colonne_snake_unique}` (prefixe = nom de variable `projets` ci-dessous, ou `join_prefix="..."`). Dans un notebook Jupyter, si l’inférence du préfixe échoue encore, passez explicitement `join_prefix="..."` (souvent le cas si `inspect.getsource` n’est pas disponible pour la cellule).

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

- Ouvrir une issue GitHub : [Issues](https://github.com/willykinfoussia/FabricPackage/issues)
- Consulter le depot : [Repository](https://github.com/willykinfoussia/FabricPackage)

Pour aider rapidement, partagez :
- la fonction utilisee
- un exemple de parametres
- le message d'erreur complet

---

## Ressources mainteneur

Guide de publication PyPI : [docs/PYPI_PUBLISH.md](docs/PYPI_PUBLISH.md)

---

## Licence

MIT
