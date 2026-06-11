# Conversion Excel (formules) → fabrictools

Guide pour convertir les formules d'un tableau Excel structuré en Python **fabrictools**, notamment pour une utilisation par une IA.

## Règle d'or

Avec `read_lakehouse`, **ignorer** tout ce qui relève de la feuille Excel ou des connexions déjà migrées :

| Élément Excel à ignorer | Raison |
|-------------------------|--------|
| Tableau structuré / nom de feuille | Une table Lakehouse = une source |
| `SUBTOTAL(9, …)` (ligne de totaux) | Agrégations Spark explicites si besoin |
| `CUSTOMER_PROJECTS__32[[#This Row],[Col]]` | Colonnes du DataFrame pilote (`[@[Col]]`) |
| Connexions Power Query des 4 sources | Déjà couvertes par `scripts_fabrictools/` |
| Saisie manuelle `RAO CODE` dans Excel | Table Lakehouse dédiée `backlog_projects` |

**Point d'entrée Python :**

```python
from fabrictools import read_lakehouse, Table, Excel
from pyspark.sql import functions as F

projects = read_lakehouse("ChinaBacklog", "Tables/dbo/backlog_projects")
customer_projects = read_lakehouse("ChinaBacklog", "Tables/dbo/customer_projects")
turnover_follow = read_lakehouse("ChinaBacklog", "Tables/dbo/turnover_follow")
oi_backup = read_lakehouse("ChinaBacklog", "Tables/dbo/oi_backup")
invoicing = read_lakehouse("ChinaBacklog", "Tables/dbo/invoicing_2026")
```

Import explicite : `from fabrictools.excel import Excel`.

## Table pilote `backlog_projects`

Schéma minimal :

| Colonne | Type | Rôle |
|---------|------|------|
| `RAO CODE` | text | Clé projet (équivalent saisie manuelle Excel) |
| `Description` | text (optionnel) | Fallback `SUMIFS` quand `RAO CODE` est vide |
| `Client` | text (optionnel) | Fallback `SUMIFS` quand `RAO CODE` est vide |

Alimentation : export Excel → Lakehouse, ou table maintenue manuellement dans Fabric.

## Correspondance 1:1 des noms

Les noms Python sont **identiques** à Excel EN (affichage FR entre parenthèses) :

| Excel (FR / EN) | Python fabrictools |
|-----------------|-------------------|
| `RECHERCHEX` / `XLOOKUP` | `Excel.XLookup` |
| `SOMME.SI` / `SUMIF` | `Excel.SumIf` |
| `SOMME.SI.ENS` / `SUMIFS` | `Excel.SumIfs` |
| `SI` / `IF` | `Excel.If` |
| `ARRONDI` / `ROUND` | `Excel.Round` |
| `JOINDRE.TEXTE` / `TEXTJOIN` | `Excel.TextJoin` |
| `[@[Colonne]]` | `F.col("Colonne")` |
| `TABLE[Col]` | colonne du `DataFrame` source |
| Additions / soustractions | `Table.AddColumn` |

**Principe clé :** en Excel, `SUMIF` / `SUMIFS` s'évaluent ligne par ligne ; en Spark, `Excel.SumIf` / `Excel.SumIfs` **pré-agrègent** la table source puis **joignent** sur la table pilote.

---

## Excel.*

| Fonction | Description |
|----------|-------------|
| `Excel.XLookup` | Lookup gauche (premier match), colonne ajoutée |
| `Excel.SumIf` | Somme conditionnelle sur une clé (`groupBy` + join) |
| `Excel.SumIfs` | Somme multi-critères (clés = colonnes left, littéraux = filtres) |
| `Excel.If` | `F.when(…).otherwise(…)` |
| `Excel.Round` | `F.round` |
| `Excel.TextJoin` | `F.concat_ws` avec option ignorer les vides |

Combiner avec `Table.AddColumn`, `Table.SelectColumns`, `Table.RemoveColumns` (module Power Query).

---

## Extrait Backlog Monitoring

Référence complète : `scripts_fabrictools/backlog_monitoring.py`.

### Lookups (RECHERCHEX)

```python
for col in ("Date", "Year", "Client", "Interco", "Description", "Currency"):
    df = Excel.XLookup(df, "RAO CODE", customer_projects, "RAO CODE", col)
```

Équivalent Excel :

```
Date=RECHERCHEX([@[RAO CODE]]; CUSTOMER_PROJECTS__2[RAO CODE]; CUSTOMER_PROJECTS__2[Date])
```

### Contrats (SUMIF + branche SI)

```python
def _has_rao_code():
    col = F.col("RAO CODE")
    return col.isNotNull() & (F.trim(col.cast("string")) != "")

df = Excel.SumIf(df, "RAO CODE", customer_projects, "RAO CODE", "Total Contract",
                 output_name="_si_contract")
df = Excel.SumIfs(df, "Total Contract", customer_projects,
    {"Description": "Description", "Client": "Client", "RAO CODE": ""},
    output_name="_sis_contract")
df = Table.AddColumn(df, "Total Contract (Customer Projects)",
    Excel.If(_has_rao_code(), F.col("_si_contract"), F.col("_sis_contract")))
```

### Turnover follow par année (SUMIFS)

```python
df = Excel.SumIfs(df, "Turnover", turnover_follow,
    {"Project No.": "RAO CODE", "Year": 2022},
    output_name="Turnover Follow 2022")
```

### Colonnes dérivées

```python
df = Table.AddColumn(df, "var/Contract",
    F.col("Total Contract (Customer Projects)") - F.col("Total Contract (Backup)"))
df = Table.AddColumn(df, "Total Turnover",
    F.col("Turnover 2022") + F.col("Turnover 2023") + F.col("Turnover 2024") + F.col("Turnover 2025"))
df = Table.AddColumn(df, "Backlog 2024",
    F.col("Total Contract (Adjusted)") - F.col("Turnover 2022")
    - F.col("Turnover 2023") - F.col("Turnover 2024"))
```

### Status et Data Issues

```python
df = Table.AddColumn(df, "Status",
    Excel.If(Excel.Round(F.col("TO still to recognize"), 0) != 0, "OPEN", "CLOSED"))

df = Table.AddColumn(df, "Data Issues", Excel.TextJoin(" | ", True,
    Excel.If(~_has_rao_code(), "Missing project code", ""),
    Excel.If(F.col("Date").isNull(), "Missing PO date", ""),
    # … autres alertes
))
```

### Mapping des 33 colonnes

| Groupe | Colonnes | Stratégie |
|--------|----------|-----------|
| Pilote | `RAO CODE` | `backlog_projects` |
| Lookup CP | Date, Year, Client, Interco, Description, Currency | `Excel.XLookup` |
| Contrats | Total Contract (CP/Backup), var/Contract, Total Contract (Adjusted) | `Excel.SumIf` + `Excel.If` / `Excel.SumIfs` |
| Turnover | Turnover 20xx, Turnover Follow 20xx, var/20xx | `Excel.SumIf` + `Excel.SumIfs` |
| Totaux | Total Turnover, Total Turnover Follow, var/Turnover | `Table.AddColumn` |
| Facturation | Total Invoicing, TO Follow vs Invoicing | `Excel.SumIf` + soustraction |
| Backlog | Backlog 2024, TO still to recognize | `Table.AddColumn` |
| Contrôle | Status, Data Issues | `Excel.If`, `Excel.Round`, `Excel.TextJoin` |

---

## Anti-patterns

- Ne pas réécrire les 4 scripts Power Query dans le script Backlog : lire les tables **déjà préparées**.
- Ne pas simuler `SUBTOTAL` filtré Excel ; utiliser `groupBy` / `agg` Spark si des totaux sont nécessaires.
- Ne pas faire de `collect()` / UDF row-wise pour `SUMIF`.
- Ne pas dupliquer les colonnes jointes sans alias : préférer les helpers `Excel.*` qui nettoient les colonnes temporaires.
- Préférer `Excel.SumIfs(…, {"Year": 2022})` (littéral) plutôt qu'un filtre manuel dispersé.

## Résolution des noms de colonnes

fabrictools accepte les noms **physiques**, **normalisés** (style `clean_data`) ou **snake_case**. Les fonctions `Excel.*` et `Table.*` résolvent automatiquement les libellés Excel.
