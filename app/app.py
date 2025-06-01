import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# Configuration de la page
st.set_page_config(
    page_title="Analyse des Taxis Jaunes de NYC",
    page_icon="🚕",
    layout="wide"
)

# Fonction pour se connecter à DuckDB et créer des tables
@st.cache_resource
def init_duckdb():
    # Connexion à une nouvelle base de données
    conn = duckdb.connect(database=':memory:')
    
    # Chemins des données
    data_dir = Path('../data')
    main_data = data_dir / "yellow_taxi_2022_01.parquet"
    hourly_data = data_dir / "hourly_trips.parquet" 
    daily_data = data_dir / "daily_trips.parquet"
    payment_data = data_dir / "payment_types.parquet"
    
    # Vérification de l'existence des fichiers
    if not main_data.exists():
        st.error("Fichier de données principal non trouvé. Veuillez exécuter download_data.py d'abord.")
        return None
    
    if not hourly_data.exists() or not daily_data.exists() or not payment_data.exists():
        st.warning("Fichiers d'analyse non trouvés. Veuillez exécuter analyze_data.py d'abord.")
    
    # Création des tables à partir des fichiers Parquet
    if main_data.exists():
        conn.execute(f"CREATE TABLE IF NOT EXISTS yellow_taxi AS SELECT * FROM '{main_data}'")
    
    if hourly_data.exists():
        conn.execute(f"CREATE TABLE IF NOT EXISTS hourly_trips AS SELECT * FROM '{hourly_data}'")
    
    if daily_data.exists():
        conn.execute(f"CREATE TABLE IF NOT EXISTS daily_trips AS SELECT * FROM '{daily_data}'")
    
    if payment_data.exists():
        conn.execute(f"CREATE TABLE IF NOT EXISTS payment_types AS SELECT * FROM '{payment_data}'")
    
    return conn

# Fonction principale
def main():
    st.title("🚕 Analyse des Taxis Jaunes de NYC")
    st.markdown("Un tableau de bord interactif pour explorer les données des courses de taxis jaunes de NYC avec DuckDB et Streamlit")
    
    # Initialisation de la connexion DuckDB
    conn = init_duckdb()
    if conn is None:
        return
    
    # Options de la barre latérale
    st.sidebar.header("Filtres")
    
    # Valeurs par défaut pour les filtres
    min_date, max_date = None, None
    date_range = None
    distance_range = None
    
    # Vérification de l'existence de la table avant d'essayer de l'interroger
    table_exists = False
    try:
        table_check = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'yellow_taxi'").fetchone()[0]
        table_exists = table_check > 0
    except Exception as e:
        st.warning(f"Erreur lors de la vérification de l'existence de la table: {str(e)}")
    
    if table_exists:
        try:
            # Essayer d'obtenir la plage de dates
            min_date, max_date = conn.execute(
                "SELECT MIN(tpep_pickup_datetime)::DATE, MAX(tpep_pickup_datetime)::DATE FROM yellow_taxi"
            ).fetchone()
            
            if min_date is not None and max_date is not None:
                date_range = st.sidebar.date_input(
                    "Période",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date
                )
            
            # Essayer d'obtenir la plage de distance
            try:
                min_distance, max_distance = conn.execute(
                    "SELECT MIN(trip_distance), MAX(trip_distance) FROM yellow_taxi"
                ).fetchone()
                
                if min_distance is not None and max_distance is not None:
                    # Arrondir min à 0 et max au plus proche multiple de 10 miles au-dessus
                    min_distance_rounded = 0.0
                    max_distance_rounded = float((int(max_distance) // 10 + 1) * 10)
                    
                    distance_range = st.sidebar.slider(
                        "Distance de trajet (miles)",
                        min_value=min_distance_rounded,
                        max_value=max_distance_rounded,
                        value=(min_distance_rounded, max_distance_rounded),
                        step=10.0
                    )
            except Exception as e:
                st.sidebar.warning(f"Impossible de charger le filtre de distance: {str(e)}")
                
        except Exception as e:
            st.sidebar.warning(f"Impossible de charger le filtre de date: {str(e)}")
    else:
        st.sidebar.warning("Données non chargées. Veuillez exécuter download_data.py d'abord.")
    
    # Création d'onglets pour différentes analyses
    tab1, tab2, tab3, tab4 = st.tabs(["Aperçu", "Analyse Horaire", "Analyse Journalière", "Analyse des Paiements"])
    
    with tab1:
        st.header("Aperçu")
        
        # Affichage des statistiques sommaires
        if not table_exists:
            st.info("Aucune donnée disponible. Veuillez exécuter download_data.py pour télécharger le jeu de données.")
        else:
            try:
                # Création de la clause WHERE pour les filtres
                where_clauses = []
                
                if date_range is not None and len(date_range) == 2:
                    where_clauses.append(f"tpep_pickup_datetime::DATE BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
                    
                if distance_range is not None and len(distance_range) == 2:
                    where_clauses.append(f"trip_distance BETWEEN {distance_range[0]} AND {distance_range[1]}")
                
                where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                
                # Application des filtres à toutes les requêtes
                row_count = conn.execute(f"SELECT COUNT(*) FROM yellow_taxi{where_clause}").fetchone()[0]
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total des Courses", f"{row_count:,}")
                
                stats = conn.execute(f"""
                    SELECT 
                        AVG(trip_distance) AS avg_distance,
                        AVG(total_amount) AS avg_amount,
                        SUM(total_amount) AS total_revenue
                    FROM yellow_taxi{where_clause}
                """).fetchone()
                
                with col2:
                    st.metric("Distance Moyenne", f"{stats[0]:.2f} miles")
                
                with col3:
                    st.metric("Tarif Moyen", f"${stats[1]:.2f}")
                
                with col4:
                    st.metric("Revenu Total", f"${stats[2]:,.2f}")
                
                # Échantillon de données
                st.subheader("Exemple de Données")
                sample = conn.execute(f"SELECT * FROM yellow_taxi{where_clause} LIMIT 10").df()
                st.dataframe(sample)
                
            except Exception as e:
                st.error(f"Erreur lors de la récupération des données d'aperçu: {e}")
    
    with tab2:
        st.header("Distribution Horaire des Courses")
        
        if not table_exists:
            st.info("Aucune donnée disponible. Veuillez exécuter download_data.py et analyze_data.py pour générer les statistiques horaires.")
        else:
            try:
                # Vérifier si la table hourly_trips existe
                hourly_exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'hourly_trips'").fetchone()[0] > 0
                if not hourly_exists:
                    st.warning("Données d'analyse horaire non disponibles. Veuillez exécuter le script analyze_data.py d'abord.")
                    return
                    
                # Distribution horaire des courses
                hourly_df = conn.execute("""
                    SELECT * FROM hourly_trips
                """).df()
                
                fig = px.bar(hourly_df, x="hour", y="trip_count", 
                            labels={"hour": "Heure de la journée", "trip_count": "Nombre de courses"},
                            title="Courses par heure de la journée")
                fig.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=1))
                st.plotly_chart(fig, use_container_width=True)
                
                # Ajouter quelques informations
                max_hour = hourly_df.loc[hourly_df['trip_count'].idxmax()]['hour']
                st.info(f"L'heure la plus chargée pour les courses de taxi est {int(max_hour)}:00, ce qui correspond probablement à l'heure de pointe du soir.")
                
            except Exception as e:
                st.error(f"Erreur lors de la récupération des données horaires: {e}")
    
    with tab3:
        st.header("Analyse Journalière des Courses")
        
        if not table_exists:
            st.info("Aucune donnée disponible. Veuillez exécuter download_data.py et analyze_data.py pour générer les statistiques journalières.")
        else:
            try:
                # Vérifier si la table daily_trips existe
                daily_exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'daily_trips'").fetchone()[0] > 0
                if not daily_exists:
                    st.warning("Données d'analyse journalière non disponibles. Veuillez exécuter le script analyze_data.py d'abord.")
                    return
                    
                # Données journalières des courses
                daily_df = conn.execute("""
                    SELECT * FROM daily_trips
                """).df()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig1 = px.line(daily_df, x="day", y="trip_count", 
                                labels={"day": "Jour du mois", "trip_count": "Nombre de courses"},
                                title="Courses par jour du mois")
                    fig1.update_layout(xaxis=dict(tickmode='linear', tick0=1, dtick=1))
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col2:
                    fig2 = px.scatter(daily_df, x="day", y="avg_distance", size="trip_count",
                                    labels={"day": "Jour du mois", "avg_distance": "Distance moyenne des courses (miles)"},
                                    title="Distance moyenne des courses par jour")
                    fig2.update_layout(xaxis=dict(tickmode='linear', tick0=1, dtick=1))
                    st.plotly_chart(fig2, use_container_width=True)
                
                # Ajouter quelques informations
                peak_day = daily_df.loc[daily_df['trip_count'].idxmax()]['day']
                st.info(f"Le jour le plus chargé du mois était le jour {int(peak_day)} avec {int(daily_df.loc[daily_df['trip_count'].idxmax()]['trip_count']):,} courses.")
                
            except Exception as e:
                st.error(f"Erreur lors de la récupération des données journalières: {e}")
    
    with tab4:
        st.header("Analyse des Méthodes de Paiement")
        
        if not table_exists:
            st.info("Aucune donnée disponible. Veuillez exécuter download_data.py et analyze_data.py pour générer les statistiques de paiement.")
        else:
            try:
                # Vérifier si la table payment_types existe
                payment_exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'payment_types'").fetchone()[0] > 0
                if not payment_exists:
                    st.warning("Données d'analyse de paiement non disponibles. Veuillez exécuter le script analyze_data.py d'abord.")
                    return
                    
                # Données des types de paiement
                payment_df = conn.execute("""
                    SELECT * FROM payment_types
                """).df()
                
                # Ajouter les descriptions des types de paiement
                payment_types = {
                    1: "Carte de crédit",
                    2: "Espèces",
                    3: "Sans frais",
                    4: "Litige",
                    5: "Inconnu",
                    6: "Course annulée"
                }
                
                payment_df['payment_name'] = payment_df['payment_type'].map(lambda x: payment_types.get(x, f"Type {x}"))
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig1 = px.pie(payment_df, values='count', names='payment_name',
                                title="Répartition des méthodes de paiement")
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col2:
                    fig2 = px.bar(payment_df, x='payment_name', y='avg_amount',
                                labels={"payment_name": "Méthode de paiement", "avg_amount": "Montant moyen du tarif ($)"},
                                title="Tarif moyen par méthode de paiement")
                    st.plotly_chart(fig2, use_container_width=True)
                
                # Ajouter quelques informations
                most_common = payment_df.loc[payment_df['count'].idxmax()]['payment_name']
                highest_avg = payment_df.loc[payment_df['avg_amount'].idxmax()]['payment_name']
                
                st.info(f"La méthode de paiement la plus courante est '{most_common}', tandis que '{highest_avg}' a le tarif moyen le plus élevé.")
                
            except Exception as e:
                st.error(f"Erreur lors de la récupération des données de paiement: {e}")
    
    # Section de requête DuckDB personnalisée
    st.header("Exécuter des requêtes DuckDB personnalisées")
    custom_query = st.text_area("Entrez votre requête SQL", height=150, 
                              value="SELECT COUNT(*) AS nombre_courses, AVG(total_amount) AS tarif_moyen\nFROM yellow_taxi\nWHERE trip_distance > 5")
    
    if st.button("Exécuter la requête"):
        try:
            query_result = conn.execute(custom_query).df()
            st.dataframe(query_result)
        except Exception as e:
            st.error(f"Erreur lors de l'exécution de la requête: {e}")

if __name__ == "__main__":
    main()