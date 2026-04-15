Premiers pas (5 minutes)
========================

.. note::

   Ce guide reprend la section du même nom du fichier ``README.md`` du dépôt.

Installez le paquet (voir :doc:`index`), puis dans un notebook Fabric :

.. code-block:: python

   import fabrictools as ft

   # Lire une table/fichier depuis un Lakehouse
   df = ft.read_lakehouse("BronzeLakehouse", "dbo/orders")
   df.show(5)

Ensuite, vous pouvez enchaîner :

1. Nettoyer les données (:py:func:`fabrictools.clean_data`)
2. Ajouter des métadonnées (:py:func:`fabrictools.add_silver_metadata`)
3. Écrire vers un Lakehouse cible (:py:func:`fabrictools.write_lakehouse`)

Pour le détail des signatures et paramètres, voir la :doc:`api_publique`.
