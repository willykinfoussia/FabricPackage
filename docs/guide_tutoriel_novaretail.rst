Tutoriel : projet fictif NovaRetail
===================================

Objectif : partir de données brutes de ventes et finir avec des tables préparées pour le reporting.

Ce tutoriel est aligné sur la section « Tutoriel interactif » du ``README.md``.

Vue d'ensemble
--------------

.. mermaid::

   flowchart LR
      sourceLakehouse["BronzeLakehouse (brut)"] --> cleanStep["Nettoyage"]
      cleanStep --> silverStep["Enrichissement Silver"]
      silverStep --> curatedLakehouse["SilverLakehouse (curated)"]
      curatedLakehouse --> preparedStep["Préparation sémantique"]
      preparedStep --> preparedLakehouse["PreparedLakehouse"]
      preparedLakehouse --> warehouseStep["Warehouse + BI"]

Étape 1 — Lire les ventes brutes
---------------------------------

.. code-block:: python

   import fabrictools as ft

   orders_raw = ft.read_lakehouse("BronzeLakehouse", "dbo/orders_raw")
   orders_raw.show(5)

Étape 2 — Nettoyer les données
------------------------------

.. code-block:: python

   orders_clean = ft.clean_data(orders_raw)

Étape 3 — Enrichir en métadonnées Silver
----------------------------------------

.. code-block:: python

   orders_silver = ft.add_silver_metadata(
       orders_clean,
       source_lakehouse_name="BronzeLakehouse",
       source_relative_path="dbo/orders_raw",
       source_layer="bronze",
   )

Étape 4 — Écrire en Silver
---------------------------

.. code-block:: python

   ft.write_lakehouse(
       orders_silver,
       lakehouse_name="SilverLakehouse",
       relative_path="dbo/orders",
       mode="overwrite",
       partition_by=["year", "month", "day"],
   )

Étape 5 — Scanner la qualité
-----------------------------

.. code-block:: python

   quality = ft.scan_data_errors(orders_silver, include_samples=True, display_results=True)
   quality["summary_df"].show(truncate=False)

Étape 6 — Fusion incrémentale (upsert)
--------------------------------------

.. code-block:: python

   orders_updates = ft.read_lakehouse("BronzeLakehouse", "dbo/orders_updates")

   ft.merge_lakehouse(
       source_df=orders_updates,
       lakehouse_name="SilverLakehouse",
       relative_path="dbo/orders",
       merge_condition="src.order_id = tgt.order_id",
   )

Étape 7 — Écriture dans un Warehouse
-------------------------------------

.. code-block:: python

   ft.write_warehouse(
       df=orders_silver,
       warehouse_name="RetailWarehouse",
       table="dbo.orders",
       mode="overwrite",
   )

Étape 8 — Pipeline préparé (table unique)
------------------------------------------

.. code-block:: python

   prepared_df = ft.prepare_and_write_data(
       source_lakehouse_name="SilverLakehouse",
       source_relative_path="Tables/dbo/orders",
       target_lakehouse_name="PreparedLakehouse",
       target_relative_path="Tables/dbo/orders_prepared",
       mode="overwrite",
   )

Étape 9 — Pipeline préparé (bulk)
----------------------------------

.. code-block:: python

   bulk_result = ft.prepare_and_write_all_tables(
       source_lakehouse_name="SilverLakehouse",
       target_lakehouse_name="PreparedLakehouse",
       include_schemas=["dbo"],
       continue_on_error=True,
   )
   print(bulk_result["successful_tables"], bulk_result["failed_tables"])

Étape 10 — Dimensions pour reporting
-------------------------------------

.. code-block:: python

   dims = ft.generate_dimensions(
       lakehouse_name="PreparedLakehouse",
       warehouse_name="RetailWarehouse",
       include_date=True,
       include_country=True,
       include_city=True,
   )

Pour aller plus loin, consultez la :doc:`api_publique` et la :doc:`guide_faq`.
