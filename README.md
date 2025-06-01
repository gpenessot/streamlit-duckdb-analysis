# NYC Yellow Taxi Analysis with DuckDB and Streamlit

Ce projet analyse les données NYC Yellow Taxi en utilisant DuckDB pour le traitement de données et Streamlit pour la visualisation.

## Structure du projet

```
newsletter-mai-25/
├── data/                # Données parquet et résultats d'analyse
├── scripts/             # Scripts Python pour télécharger et analyser les données
│   ├── download_data.py # Télécharge les données Yellow Taxi
│   └── analyze_data.py  # Analyse les données avec DuckDB
├── app/                 # Application Streamlit
│   └── app.py           # Interface utilisateur Streamlit
├── pyproject.toml      # Configuration du projet et dépendances
└── post_substack.md     # Newsletter Substack Mai 2025
```

## Installation

1. Installez uv (si ce n'est pas déjà fait) :

```bash
curl -sSf https://astral.sh/uv/install.sh | bash
```

2. Créez un environnement virtuel et installez les dépendances avec uv :

```bash
uv venv
source .venv/bin/activate  # Sur Windows : .venv\Scripts\activate
uv pip install -e .
```

## Utilisation

1. Téléchargez les données NYC Yellow Taxi :

```bash
cd scripts
python download_data.py
```

2. Analysez les données avec DuckDB :

```bash
python analyze_data.py
```

3. Lancez l'application Streamlit :

```bash
cd ../app
streamlit run app.py
```

L'application sera accessible à l'adresse http://localhost:8501

## Fonctionnalités

- Analyse complète des données de taxi avec DuckDB
- Visualisations interactives avec Streamlit et Plotly
- Filtrage des données par date, distance et autres paramètres
- Possibilité d'exécuter des requêtes SQL personnalisées via l'interface

## À propos des données

Ce projet utilise les données NYC Yellow Taxi de janvier 2022, qui incluent :
- Dates et heures de prise en charge et dépose
- Distances de trajet
- Tarifs, pourboires et montants totaux
- Types de paiement
- Coordonnées de localisation

Source des données : NYC Taxi & Limousine Commission