# **DT_Projekt MovieLens Dataset**
Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z MovieLens datasetu. Projekt sa zameriava na filmy, žánre filmov, pozícií, hodnotení a údaje o používateľoch. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu dát.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa kníh, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v čitateľských preferenciách, najpopulárnejšie knihy a správanie používateľov.

Zdrojové dáta pochádzajú z Kaggle datasetu dostupného [tu](https://edu.ukf.sk/mod/folder/view.php?id=252867). Dataset obsahuje osem hlavných tabuliek:
- `books`
- `ratings`
- `users`
- `occupations`
- `education_levels`
