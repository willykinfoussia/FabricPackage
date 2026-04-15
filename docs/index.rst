FabricTools
===========

**fabrictools** fournit des helpers PySpark orientés `Microsoft Fabric`_ pour lire et écrire dans les **Lakehouses** et **Warehouses**, préparer des données, construire des dimensions et appliquer des transformations courantes sur des ``DataFrame``.

.. _Microsoft Fabric: https://learn.microsoft.com/en-us/fabric/

Dépôt et suivi :

- `Dépôt GitHub <https://github.com/willykinfoussia/FabricPackage>`_
- `Issues <https://github.com/willykinfoussia/FabricPackage/issues>`_

Prérequis
---------

- Python ``>= 3.9``.
- Environnement **Spark** (typiquement un notebook Fabric) avec **PySpark** déjà disponible.
- Pour un usage hors Fabric, installez l’extra ``spark`` du paquet (voir ci-dessous).

Installation
------------

Depuis PyPI (usage courant)::

   pip install fabrictools

Avec Spark et Delta en local::

   pip install "fabrictools[spark]"

Option visualisation (graphiques pour le scan qualité)::

   pip install "fabrictools[visualization]"

Depuis le dépôt (développement)::

   pip install -e ".[spark]"

Pour générer cette documentation localement::

   pip install -e ".[docs]"

Référence API
-------------

**API recommandée** : importez depuis le paquet racine, par exemple
``from fabrictools import read_lakehouse, write_lakehouse``. Sous **Référence API**,
« API publique » et les pages par domaine (I/O, Qualité, etc.) listent ces symboles ; la section **Implémentation** regroupe
les sous-packages Python pour contribuer ou déboguer.

.. toctree::
   :maxdepth: 2
   :caption: Guides

   guide_premiers_pas
   guide_tutoriel_novaretail
   guide_faq

.. toctree::
   :maxdepth: 2
   :caption: Contenu

   modules
