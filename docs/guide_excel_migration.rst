Migration Excel → fabrictools
=============================

Ce guide explique comment convertir les **formules Excel** d'un tableau structuré en Python
avec le package :mod:`fabrictools.excel`, en partant d'une lecture Lakehouse plutôt que d'Excel.

Principe
--------

Avec :func:`fabrictools.read_lakehouse`, ignorez les éléments suivants :

* Tableau structuré Excel et ligne de totaux ``SUBTOTAL(9, …)``
* Références ``TABLE[[#This Row],[Col]]`` → colonnes du DataFrame pilote
* Connexions Power Query déjà migrées (voir :doc:`guide_powerquery_migration`)
* Saisie manuelle des clés projet → table Lakehouse ``backlog_projects``

Imports
-------

.. code-block:: python

   from fabrictools import read_lakehouse, Table, Excel
   from pyspark.sql import functions as F

   projects = read_lakehouse("ChinaBacklog", "Tables/dbo/backlog_projects")
   customer_projects = read_lakehouse("ChinaBacklog", "Tables/dbo/customer_projects")

Les noms Python sont **identiques** à Excel EN : ``Excel.XLookup``, ``Excel.SumIf``, ``Excel.SumIfs``.

Module Excel
------------

* :py:meth:`fabrictools.excel.Excel.XLookup` — ``XLOOKUP`` / ``RECHERCHEX``
* :py:meth:`fabrictools.excel.Excel.SumIf` — ``SUMIF`` / ``SOMME.SI``
* :py:meth:`fabrictools.excel.Excel.SumIfs` — ``SUMIFS`` / ``SOMME.SI.ENS``
* :py:meth:`fabrictools.excel.Excel.If` — ``IF`` / ``SI``
* :py:meth:`fabrictools.excel.Excel.Round` — ``ROUND`` / ``ARRONDI``
* :py:meth:`fabrictools.excel.Excel.TextJoin` — ``TEXTJOIN`` / ``JOINDRE.TEXTE``

Combiner avec :class:`~fabrictools.powerquery.table.Table` (``Table.AddColumn``, ``Table.SelectColumns``, …).

Guide IA détaillé
-----------------

Voir le fichier ``ExcelToFabric.md`` à la racine du dépôt (extraits Backlog Monitoring, anti-patterns).

Exemples
--------

Script de référence : ``scripts_fabrictools/backlog_monitoring.py``.

Référence API
-------------

:doc:`api_publique_excel`
