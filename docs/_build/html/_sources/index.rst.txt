FabricTools
===========

**fabrictools** fournit des helpers PySpark orientés `Microsoft Fabric`_ pour lire et écrire dans les **Lakehouses** et **Warehouses**, préparer des données, construire des dimensions et appliquer des transformations courantes sur des ``DataFrame``.

.. _Microsoft Fabric: https://learn.microsoft.com/en-us/fabric/

Prérequis
---------

- Environnement **Spark** (typiquement un notebook Fabric) avec **PySpark** déjà disponible.
- Pour un usage hors Fabric, installez l’extra ``spark`` du paquet (voir ci-dessous).

Installation
------------

Depuis le dépôt (développement)::

   pip install -e ".[spark]"

Pour générer cette documentation localement::

   pip install -e ".[docs]"

Référence API
--------------

**API recommandée** : importez depuis le paquet racine, par exemple
``from fabrictools import read_lakehouse, write_lakehouse``. Sous **Référence API**,
« API publique » et les pages par domaine (I/O, Qualité, etc.) listent ces symboles ; la section **Implémentation** regroupe
les sous-packages Python pour contribuer ou déboguer.

.. toctree::
   :maxdepth: 2
   :caption: Contenu

   modules
