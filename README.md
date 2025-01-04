# **DT Projekt MovieLens Dataset**
Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z MovieLens datasetu. Projekt sa zameriava na filmy, žánre filmov, pozícií, hodnotení a údaje o používateľoch. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu dát.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa kníh, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v čitateľských preferenciách, najpopulárnejšie knihy a správanie používateľov.

Zdrojové dáta pochádzajú z Kaggle datasetu dostupného [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje osem hlavných tabuliek:
- `age_group`
- `genres`
- `genres_movies`
- `movies`
- `occupations`
- `ratings`
- `tags`
- `users`
  
Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="MovieLens_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma AmazonBooks</em>
</p>

---
## **2. Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movies`**: Obsahuje informácie o filmoch ako napríklad: id filmu, názov a rok vydania
- **`dim_genres`**: Obsahuje informácie o žánroch, čiže id žánru a názov
- **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vek, vekové kategórie, pohlavie, PSČ a povolanie.
- **`dim_tags`**: Obsahuje značky (tags), ktoré môžu slúžiť na analýzu nálad alebo na dodatočné triedenie filmov.
- **`dim_time`**: Obsahuje podrobné informácie o tag-och, ako napríklad: tagy, dátum a čas vytvorenia.
- **`dim_date`**: Obsahuje údaje o hodnotiacich dátumoch, ako sú deň, mesiac, rok, názvy mesiacov a dni v týždni.

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="MovieLens_Star.png" alt="MovieLens Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch kľúčových fáz: `extrakcie` (Extract), `transformácie` (Transform) a `načítania` (Load). Tento proces bol implementovaný v Snowflake na prípravu zdrojových dát zo staging vrstvy do viacdimenzionálneho modelu určeného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta (formát `.csv`) najprv nahráme do Snowflake pomocou interného stage úložiska my_stage, odkiaľ sa následne importujú do staging tabuliek.

#### Kód pre vytvorenie stage-u:
```sql
CREATE OR REPLACE STAGE scorpion_stage;
```
