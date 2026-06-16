Intégration IFS Cloud
=====================

Fonctions réexportées par ``fabrictools`` pour lire des données IFS Cloud via OData et les charger dans Spark ou un Lakehouse Fabric.

.. currentmodule:: fabrictools

.. autoclass:: IFSConfig
   :members:

.. autoclass:: IFSClient
   :members:

.. autoexception:: IFSError

.. autofunction:: ifs_config_with_keyvault_secret

.. autofunction:: read_ifs_entity

.. autofunction:: read_ifs_to_lakehouse
