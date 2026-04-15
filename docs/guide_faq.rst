FAQ
===

Questions fréquentes (alignées sur le ``README.md`` du dépôt).

1) Utiliser fabrictools sans Microsoft Fabric ?
-----------------------------------------------

Partiellement oui. Les fonctions purement Spark peuvent fonctionner en local avec l’extra ``spark``, mais les fonctions de résolution de chemins Lakehouse dépendent de ``notebookutils`` (disponible dans Fabric).

2) Existe-t-il une CLI ``fabrictools ...`` ?
-------------------------------------------

Non. L’usage est en Python, via ``import fabrictools as ft``.

3) Plotly est-il obligatoire ?
-----------------------------

Non. C’est utile pour les graphiques de :py:func:`fabrictools.scan_data_errors`. Sans Plotly, vous gardez la partie tabulaire.

4) ``clean_and_write_data`` vs ``clean_and_write_all_tables`` ?
---------------------------------------------------------------

- :py:func:`fabrictools.clean_and_write_data` : une table cible.
- :py:func:`fabrictools.clean_and_write_all_tables` : plusieurs tables en lot.

5) ``delete_all_lakehouse_tables`` est-il dangereux ?
-----------------------------------------------------

Oui, c’est une action destructive. Commencez avec ``dry_run=True`` pour vérifier la liste avant suppression.

6) Par où commencer ?
---------------------

Un chemin minimal : :py:func:`fabrictools.read_lakehouse` → :py:func:`fabrictools.clean_data` → :py:func:`fabrictools.add_silver_metadata` → :py:func:`fabrictools.write_lakehouse`.
