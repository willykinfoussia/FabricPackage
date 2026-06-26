Inférence IA (agent LangChain)
==============================

Fonctions réexportées par ``fabrictools`` pour l'inférence via un agent LangChain 1.x
(``create_agent``, OpenRouter + recherche DuckDuckGo) et l'enrichissement de DataFrames Spark.

``ai_response`` exécute un agent avec tool-calling : il peut interroger DuckDuckGo lorsque la
question nécessite des informations récentes ou factuelles. Chaque appel peut
déclencher plusieurs requêtes LLM et web (latence plus élevée qu'un chat simple).

.. currentmodule:: fabrictools

.. autoexception:: AIError

.. autofunction:: ai_response

.. autofunction:: with_ai_column

.. autofunction:: transform_ai_column
