# Photos de démo — S2 Étalonnage (§7.1.5)

Pièces visuelles pour la capture terrain de la section **S2 — Étalonnage & équipements
de mesure**. Contexte : atelier maintenance **Sucy-en-Brie (RATP)**, rames MI09,
clé **CD-12**, récidive de la NC §7.1.5 ouverte en **novembre 2024**.

> ⚠️ Rappel technique : dans l'app, la photo **n'est pas analysée** par l'IA.
> C'est le **texte dicté au micro** (`observation`) qui pilote l'analyse ISO.
> La photo sert de **preuve visuelle** dans le rapport final.
>
> Les images sont **générées** (mock de démo) via `scripts/gen_demo_photos.py` —
> aucun cliché réel, pas de problème de droits.

## Mode opératoire (par point)

Pour chaque point : cliquer l'item à gauche → micro → **dicter le texte** →
couper le micro → « Prendre une photo » → choisir l'image → « Analyser ».

---

### 1 · Clés dynamométriques étalonnées  → NC MAJEURE (récidive)

**À dicter :**
> « Les clés dynamométriques du poste 12 ne sont pas étalonnées. Le certificat
> COFRAC est périmé depuis huit mois, échéance dépassée en octobre 2025. Aucun
> étalonnage de remplacement n'est programmé. »

**Photo :** `01_cle_dynamometrique_tag.png` — clé dyn. CD-12 avec étiquette-volante
d'étalonnage et tampon **PÉRIMÉ**.

---

### 2 · Étiquettes d'étalonnage visibles  → NC MINEURE

**À dicter :**
> « L'étiquette d'étalonnage de la clé CD-12 est présente mais illisible : la date
> de prochaine vérification est effacée et le numéro de laboratoire COFRAC n'est
> plus déchiffrable. Impossible de confirmer la validité sur le terrain. »

**Photo :** `02_etiquette_etalonnage.png` — sticker « Contrôle métrologique »
défraîchi, coin décollé, date de vérif. peu lisible.

---

### 3 · Registre des équipements complet  → NC MAJEURE

**À dicter :**
> « Le registre des équipements de mesure est incomplet. Quatre équipements sur
> sept n'ont pas de date d'étalonnage renseignée, dont le manomètre de frein et le
> banc de freinage. Deux clés dynamométriques sont en échéance dépassée. »

**Photo :** `03_registre_equipements.png` — registre métrologie de l'atelier avec
lignes non renseignées et échéances dépassées en rouge.

---

## Régénérer les images

```bash
cd uc28-inspection
python3 scripts/gen_demo_photos.py
```
