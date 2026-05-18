Dimensions (calendrier et géographie)
=====================================

Construction des dimensions date, pays, ville, attributs dérivés des données et orchestration.

.. currentmodule:: fabrictools

.. autofunction:: build_dimension_date

.. autofunction:: build_dimension_country

.. autofunction:: build_dimension_city

.. autofunction:: build_dimension_from_columns

Exemples dimension composite::

   dim_compagnie = ft.build_dimension_from_columns(
       [(correspondant_client_df, "trigramme"), (correspondant_client_df, "nom")],
       dimension_columns=["ID", "Compagnie"],
   )

   dim_compagnie = ft.build_dimension_from_columns(
       [(correspondant_client_df, "trigramme", "nom")],
       dimension_columns=["ID", "Compagnie"],
   )

.. autofunction:: generate_dimensions
