Migration Power Query → fabrictools
====================================

Ce guide explique comment convertir un script **Power Query M** en Python avec le package
:mod:`fabrictools.powerquery`, en partant d'une lecture Lakehouse plutôt que d'Excel.

Principe
--------

Avec :func:`fabrictools.read_lakehouse`, ignorez le préambule M suivant :

* ``Excel.Workbook`` / ``File.Contents``
* ``Table.Skip``
* ``Table.PromoteHeaders``
* ``Table.TransformColumnTypes`` (étape initiale de typage)

Les en-têtes et types sont déjà corrects dans Delta/Parquet. Commencez à la première
transformation métier : ``Table.Group``, ``Table.SelectRows``, ``Table.AddColumn``, etc.

Imports
-------

.. code-block:: python

   from fabrictools import read_lakehouse, Table, Text, Date, Number, List, Order, Percentage, type

   df = read_lakehouse("MonLakehouse", "Tables/dbo/ma_table")

Les noms Python sont **identiques** à Power Query : ``Table.Group``, ``Text.Clean``, ``Date.Year``.

Module Table
------------

Fonctions des scripts ``scripts powerquery/`` :

* :py:meth:`fabrictools.powerquery.table.Table.Group`
* :py:meth:`fabrictools.powerquery.table.Table.SelectRows`
* :py:meth:`fabrictools.powerquery.table.Table.AddColumn`
* :py:meth:`fabrictools.powerquery.table.Table.Sort`
* :py:meth:`fabrictools.powerquery.table.Table.SelectColumns`
* :py:meth:`fabrictools.powerquery.table.Table.RenameColumns`
* :py:meth:`fabrictools.powerquery.table.Table.RemoveColumns`
* :py:meth:`fabrictools.powerquery.table.Table.ReplaceValue`
* :py:meth:`fabrictools.powerquery.table.Table.TransformColumnTypes`
* :py:meth:`fabrictools.powerquery.table.Table.TransformColumns`

Fonctions populaires supplémentaires :

* ``Table.Distinct``, ``Table.Combine``, ``Table.NestedJoin``, ``Table.Join``
* ``Table.FillDown``, ``Table.FillUp``
* ``Table.FirstN``, ``Table.Skip``, ``Table.LastN``, ``Table.Range``
* ``Table.DuplicateColumn``, ``Table.SplitColumn``
* ``Table.Pivot``, ``Table.Unpivot``
* ``Table.ReplaceErrorValues``, ``Table.Buffer``

Module Text
-----------

* ``Text.Clean`` — équivalent ``Text.Clean`` / ``fnText``
* ``Text.Select`` — filtrer les caractères (montants invoicing)
* ``Text.Trim``, ``Text.Lower``, ``Text.Upper``, ``Text.Proper``
* ``Text.Combine``, ``Text.From``

Module Date
-----------

* ``Date.Year``, ``Date.Month``, ``Date.Day``
* ``Date.From``, ``Date.AddDays``

Module Number
-------------

* ``Number.FromText`` — parse US/FR (script invoicing, ``fxToNumber``)

Exemples
--------

Voir aussi ``scripts powerquery/README.md`` pour les extraits complets des quatre scripts M
(customer project, turnover, invoicing, oi backup).

Référence API
-------------

:doc:`api_publique_powerquery`
