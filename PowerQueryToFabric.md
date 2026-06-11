# Conversion Power Query (M) → fabrictools

Guide pour convertir les scripts M de ce dossier en Python **fabrictools**, notamment pour une utilisation par une IA.

## Règle d'or

Avec `read_lakehouse`, **ignorer** tout le préambule de chargement Excel :

| Étape M à ignorer | Raison |
|-------------------|--------|
| `Excel.Workbook` / `File.Contents` | Remplacé par `read_lakehouse` |
| Sélection de feuille | Une table Lakehouse = une feuille |
| `Table.Skip` | En-têtes déjà corrects |
| `Table.PromoteHeaders` | Colonnes déjà nommées |
| `Table.TransformColumnTypes` (initial) | Types déjà définis (Delta/Parquet) |

**Point d'entrée Python :**

```python
from fabrictools import read_lakehouse, Table, Text, Date, Number, List, Order, Percentage, type

df = read_lakehouse("MonLakehouse", "Tables/dbo/ma_table")
```

Import explicite du package : `from fabrictools.powerquery import Table`.

## Correspondance 1:1 des noms

Les noms Python sont **identiques** à Power Query :

| Power Query M | Python fabrictools |
|---------------|-------------------|
| `Table.Group` | `Table.Group` |
| `Table.SelectRows` | `Table.SelectRows` |
| `Table.AddColumn` | `Table.AddColumn` |
| `Text.Clean` | `Text.Clean` |
| `Date.Year` | `Date.Year` |
| `Number.FromText` | `Number.FromText` |
| `List.Sum` / `List.Max` | `List.Sum` / `List.Max` |

---

## Table.*

Fonctions présentes dans les scripts de ce dossier :

| Fonction | Description |
|----------|-------------|
| `Table.Group` | Agrégation par clés |
| `Table.SelectRows` | Filtrer lignes (`not_null`, `any_not_null`, ou prédicat Spark) |
| `Table.AddColumn` | Colonne calculée |
| `Table.Sort` | Tri (`Order.Ascending` / `Order.Descending`) |
| `Table.SelectColumns` | Garder colonnes dans l'ordre |
| `Table.ReorderColumns` | Réordonner (autres colonnes conservées en fin) |
| `Table.RenameColumns` | Renommer |
| `Table.RemoveColumns` | Supprimer |
| `Table.ReplaceValue` | Remplacer valeurs (`old=None` pour les null) |
| `Table.TransformColumnTypes` | Cast (`type.text`, `Percentage.Type`, `Int64.Type`, …) |
| `Table.TransformColumns` | Transformer via fonction (ex. `Number.FromText`) |

Fonctions populaires **hors scripts** (disponibles dans `fabrictools.powerquery`) :

| Fonction | Description |
|----------|-------------|
| `Table.Distinct` | Dédupliquer |
| `Table.Combine` | Union de tables |
| `Table.NestedJoin` / `Table.Join` | Jointures |
| `Table.FillDown` / `Table.FillUp` | Propager valeurs |
| `Table.FirstN` / `Table.Skip` / `Table.LastN` / `Table.Range` | Pagination |
| `Table.DuplicateColumn` | Copier une colonne |
| `Table.SplitColumn` | Scinder par délimiteur |
| `Table.Pivot` / `Table.Unpivot` | Pivot / unpivot |
| `Table.ReplaceErrorValues` | Remplacer null/erreurs |
| `Table.Buffer` | Cache Spark |

---

## Text.*

| Fonction | Usage |
|----------|-------|
| `Text.Clean` | Normalisation (`fnText`, style `Text.Clean`) |
| `Text.Select` | Garder certains caractères (montants invoicing) |
| `Text.Trim` / `Lower` / `Upper` / `Proper` | Manipulation texte |
| `Text.Combine` / `Text.From` | Concat / cast string |

---

## Date.*

| Fonction | Usage |
|----------|-------|
| `Date.Year` | Extraire l'année (script turnover) |
| `Date.Month` / `Date.Day` | Extraire mois / jour |
| `Date.From` / `Date.AddDays` | Créer / décaler une date |

---

## Number.*

| Fonction | Usage |
|----------|-------|
| `Number.FromText` | Parser un nombre depuis texte (logique `fxToNumber` invoicing) |

---

## Extraits par script M

### customer project — démarrer à `Table.Group`

```python
from fabrictools import read_lakehouse, Table, List, Percentage

df = read_lakehouse("ChinaBacklog", "Tables/dbo/customer_projects")

df = Table.Group(df, {"RAO CODE"}, [
    ("Client", "END USER", List.Max),
    ("Amount (Adjusted)", "OI ADJUSTED", List.Sum),
    ("Turnover 2021", "TURNOVER 2021", List.Sum),
    ("Turnover 2022", "TURNOVER 2022", List.Sum),
    ("Turnover 2023", "TURNOVER 2023", List.Sum),
    ("Turnover 2024", "TURNOVER 2024", List.Sum),
    ("Turnover 2025", "TOTAL TURNOVER 2025", List.Sum),
    ("Backlog 2024", "BACKLOG AT 31/12/2024", List.Sum),
    ("Backlog End Period", "BACKLOG AT END OF PERIOD", List.Sum),
    ("Backlog 2025", "BACKLOG AT 31/12/2025", List.Sum),
    ("% Completion", "% completion at end of year", List.Sum),
    ("Currency", "CURRENCY", List.Max),
    ("Interco", "Interco", List.Max),
    ("Description", "DESCRIPTION", List.Max),
    ("Status", "STATUT", List.Max),
    ("Year", "YEAR OI", List.Max),
    ("Date", "BUYER PO DATE", List.Max),
    ("Amount", "AMOUNT CNY", List.Sum),
])
df = Table.TransformColumnTypes(df, {"% Completion": Percentage.Type})
df = Table.ReplaceValue(df, None, "External", ["Interco"])
df = Table.SelectColumns(df, [
    "Date", "Year", "RAO CODE", "Client", "Interco", "Description",
    "Currency", "Amount", "Amount (Adjusted)",
    "Turnover 2021", "Turnover 2022", "Turnover 2023", "Turnover 2024", "Turnover 2025",
    "Backlog 2024", "Backlog End Period", "Backlog 2025", "% Completion", "Status",
])
df = Table.RenameColumns(df, [
    ("Amount (Adjusted)", "Total Contract (Adjusted)"),
    ("Amount", "Total Contract"),
])
df = Table.RemoveColumns(df, [
    "Backlog 2024", "Backlog End Period", "Backlog 2025", "% Completion", "Turnover 2021",
])
```

### turnover — démarrer à `Table.AddColumn` Year

```python
from fabrictools import read_lakehouse, Table, Text, Date, List, Order
from pyspark.sql import functions as F

df = read_lakehouse("ChinaBacklog", "Tables/dbo/turnover_follow")

df = Table.AddColumn(df, "Year", Date.Year(F.col("Date of Revenue recognition")))
df = Table.Group(df, {"Project No.", "Year"}, [
    ("Turnover", "Turnover", List.Sum),
    ("Client", "Client", List.Max),
    ("Type of customer", "Type of customer", List.Max),
    ("Date", "Date of Revenue recognition", List.Max),
])
df = Table.SelectRows(df, not_null=["Year"])
df = Table.Sort(df, [("Date", Order.Ascending)])
df = Table.AddColumn(df, "Project No. 2", Text.Clean(F.col("Project No.")))
df = Table.SelectColumns(df, [
    "Year", "Project No.", "Project No. 2", "Turnover", "Client", "Type of customer", "Date",
])
df = Table.RemoveColumns(df, ["Project No."])
df = Table.RenameColumns(df, [("Project No. 2", "Project No.")])
```

### invoicing — démarrer à `Table.SelectRows`

```python
from fabrictools import read_lakehouse, Table, Number, List, Percentage

df = read_lakehouse("ChinaBacklog", "Tables/dbo/invoicing_2026")

df = Table.SelectRows(df, not_null=["Project Number"])
df = Table.TransformColumns(df, [
    ("Total contrat amount within VAT", Number.FromText),
    ("Total contrat amount without VAT", Number.FromText),
    ("Total Invoice amount without VAT", Number.FromText),
    ("Total Invoice amount within VAT", Number.FromText),
])
df = Table.Group(df, {"Project Number", "Year"}, [
    ("Total Invoice Amount", "Total Invoice amount without VAT", List.Sum),
    ("Total Invoice Amount (VAT)", "Total Invoice amount within VAT", List.Max),
    ("Percent VAT Invoice completed", "Percent of VAT Invoice", List.Sum),
    ("Total contract amount", "Total contrat amount without VAT", List.Max),
    ("Total contract amount (VAT)", "Total contrat amount within VAT", List.Max),
    ("Client Name", "Client name\n客户中文名称", List.Max),
    ("Currency", "Currency", List.Max),
])
df = Table.TransformColumnTypes(df, {"Percent VAT Invoice completed": Percentage.Type})
df = Table.SelectColumns(df, [
    "Year", "Project Number", "Total Invoice Amount", "Total Invoice Amount (VAT)",
    "Percent VAT Invoice completed", "Total contract amount", "Total contract amount (VAT)",
    "Client Name", "Currency",
])
```

### oi backup — démarrer à `Table.SelectRows`

```python
from fabrictools import read_lakehouse, Table, List, Order

df = read_lakehouse("ChinaBacklog", "Tables/dbo/oi_backup")

df = Table.SelectRows(df, any_not_null=["BUYER", "RAO CODE"])
df = Table.Group(df, {"RAO CODE", "END USER"}, [
    ("Client", "END USER", List.Max),
    ("Amount", "AMOUNT", List.Sum),
    ("Amount (VAT)", "AMOUNT TOTAL", List.Sum),
    ("Year", "PO YEAR", List.Max),
])
df = Table.SelectRows(df, not_null=["Year"])
df = Table.Sort(df, [("Year", Order.Ascending)])
```

---

## Anti-patterns

- Ne pas réécrire `Excel.Workbook` ni `Table.PromoteHeaders` après `read_lakehouse`.
- Ne pas dupliquer le `Table.TransformColumnTypes` initial si les types Lakehouse sont déjà corrects.
- Préférer `Table.SelectRows(not_null=[...])` à la place de `each [col] <> null`.
- Utiliser `resolve_dataframe_column` (fabrictools) si un nom de colonne ne matche pas (accents, snake_case).

## Résolution des noms de colonnes

fabrictools accepte les noms **physiques**, **normalisés** (style `clean_data`) ou **snake_case**. Les fonctions `Table.*` résolvent automatiquement les libellés Power Query.
