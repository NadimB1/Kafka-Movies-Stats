import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


api_url_worst_scores = "http://localhost:8080/stats/ten/worst/score"
api_url_best_scores = "http://localhost:8080/stats/ten/best/score"
api_url_most_viewed = "http://localhost:8080/stats/ten/best/views"
api_url_least_viewed = "http://localhost:8080/stats/ten/worst/views"
api_url_history = "http://localhost:8080/movies/{}/history"


def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch data: {response.status_code}")
        return []


worst_scores = fetch_data(api_url_worst_scores)
best_scores = fetch_data(api_url_best_scores)
most_viewed = fetch_data(api_url_most_viewed)
least_viewed = fetch_data(api_url_least_viewed)


df_worst_scores = pd.DataFrame(worst_scores)
df_best_scores = pd.DataFrame(best_scores)
df_most_viewed = pd.DataFrame(most_viewed)
df_least_viewed = pd.DataFrame(least_viewed)


def plot_bar_chart(df, title, x_col, y_col):
    fig = px.bar(df, x=x_col, y=y_col, title=title)
    st.plotly_chart(fig)

def plot_time_series(df, title, movie_title):
    fig = go.Figure()
    for column in df.columns:
        if column != 'timestamp' and column != 'title':
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df[column], mode='lines', name=column))
    fig.update_layout(title=f"Historique du film {movie_title}", xaxis_title='Timestamp', yaxis_title='Values')
    st.plotly_chart(fig)


st.title("Statistiques des Films")


st.header("Pires scores moyens des films")
col1, col2 = st.columns(2)
with col1:
    st.dataframe(df_worst_scores)
with col2:
    plot_bar_chart(df_worst_scores, "Pires Scores Moyens", "title", "meanScore")

st.header("Meilleurs scores moyens des films")
col3, col4 = st.columns(2)
with col3:
    st.dataframe(df_best_scores)
with col4:
    plot_bar_chart(df_best_scores, "Meilleurs Scores Moyens", "title", "meanScore")

st.header("Films les plus vus")
col5, col6 = st.columns(2)
with col5:
    st.dataframe(df_most_viewed)
with col6:
    plot_bar_chart(df_most_viewed, "Films les Plus Vus", "title", "views")

st.header("Films les moins vus")
col7, col8 = st.columns(2)
with col7:
    st.dataframe(df_least_viewed)
with col8:
    plot_bar_chart(df_least_viewed, "Films les Moins Vus", "title", "views")


st.header("Historique d'un film")
movie_id = st.text_input("Entrez l'ID du film pour voir son historique:")
if movie_id:
    history_url = api_url_history.format(movie_id)
    history_data = fetch_data(history_url)

    st.subheader("Données brutes de l'historique")
    st.json(history_data)

    if history_data:
        try:
            df_history = pd.DataFrame([{
                'timestamp': pd.to_datetime(entry['timestamp'], unit='ms'),
                'title': entry['data']['title'],  # Ajout du titre ici
                'start_only': entry['data']['start_only'],
                'half': entry['data']['half'],
                'full': entry['data']['full']
            } for entry in history_data])

            movie_title = df_history['title'][0] if not df_history.empty else 'Inconnu'
            col9, col10 = st.columns(2)
            with col9:
                st.dataframe(df_history)
            with col10:
                plot_time_series(df_history, f"Historique du film {movie_id}", movie_title)
        except KeyError as e:
            st.error(f"KeyError: {e}")
