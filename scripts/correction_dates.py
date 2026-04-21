import pandas as pd
import numpy as np
import datetime as dt
from calendar import monthrange
from sklearn.isotonic import IsotonicRegression
import re
import pyspark.sql.types as T

_NCOM_RE = re.compile(r"^\*(\d+)$")

def _correct_group(pdf: pd.DataFrame) -> pd.DataFrame:
    # Extraction de n_ord
    def _to_ord(x):
        if x is None: return None
        m = _NCOM_RE.match(str(x))
        return int(m.group(1)) if m else None
    
    pdf["_n_ord"] = pdf["n_commande"].map(_to_ord)
    mask = pdf["_n_ord"].notna() & pdf["cree"].notna()
    pdf["cree_corrige"] = pdf["cree"]
    
    if mask.sum() >= 2:
        sub = pdf.loc[mask].copy()
        sub["cree"] = pd.to_datetime(sub["cree"])
        
        # Calcul du mois absolu (Année * 12 + Mois)
        sub["mk"] = sub["cree"].dt.year * 12 + sub["cree"].dt.month
        
        # 1. Agrégation par numéro de commande
        # On prend la médiane du mois pour cette commande afin d'ignorer les anomalies
        agg_sub = sub.groupby("_n_ord")["mk"].median().reset_index()
        agg_sub = agg_sub.sort_values("_n_ord")
        
        # 2. Application de la régression isotone (PAVA) sur les commandes uniques
        fit = IsotonicRegression(increasing=True).fit_transform(agg_sub["_n_ord"], agg_sub["mk"])
        
        # 3. Préparation d'un dictionnaire/mapping pour retrouver le mois corrigé
        agg_sub["mk_corr"] = np.round(fit).astype(int)
        mk_corr_map = agg_sub.set_index("_n_ord")["mk_corr"]

        # 4. Redescente de la correction sur toutes les lignes originales
        for idx in sub.index:
            n_ord = sub.at[idx, "_n_ord"]
            orig_mk = sub.at[idx, "mk"]
            corr_mk = mk_corr_map.loc[n_ord] # Récupère le mois lissé de cette commande
            
            # Si le mois corrigé est différent du mois d'origine pour cette ligne
            if orig_mk != corr_mk:
                k = int(corr_mk)
                y, m = divmod(k - 1, 12)
                y, m = int(y), int(m) + 1
                d = pdf.at[idx, "cree"]
                
                # Sécurité : On s'assure que le jour existe dans le nouveau mois 
                # (ex: 31 oct corrigé vers novembre -> devient 30 nov)
                day = min(d.day, monthrange(y, m)[1])
                
                if isinstance(d, pd.Timestamp):
                    pdf.at[idx, "cree_corrige"] = pd.Timestamp(
                        year=y, month=m, day=day, 
                        hour=d.hour, minute=d.minute, 
                        second=d.second, microsecond=d.microsecond
                    )
                else:
                    pdf.at[idx, "cree_corrige"] = dt.date(y, m, day)

    return pdf.drop(columns=["_n_ord"])

schema = T.StructType(list(defaults.schema.fields) + [T.StructField("cree_corrige", T.DateType(), True)])
result = defaults.groupBy("site").applyInPandas(_correct_group, schema=schema)
display(result)