import streamlit as st
import plotly.express as px
import pyspark.sql.functions as F
from most_used_cards import create_card_counts_with_names
from PIL import Image
import pandas as pd
import requests

# Metrics Functions
def calculate_total_crowns(filtered_df):
    """Calculate the total number of crowns."""
         
    winner_loser = filtered_df.select("winner_crowns", "loser_crowns")
    winner_loser.cache()

    # Calculate total crowns won by winners and losers individually then sum those values for total overall
    total_winner_crowns = winner_loser.agg(F.sum("winner_crowns")).collect()[0][0]
    total_loser_crowns = winner_loser.agg(F.sum("loser_crowns")).collect()[0][0]
    total_crowns = total_winner_crowns + total_loser_crowns

    winner_loser.unpersist()
    
    # Convert the number into a nice, easily readable string
    total_crowns = convert_readable_number(total_crowns)
    
    return total_crowns


def calculate_number_of_players(filtered_df):
    """Calculate the number of unique players."""
    winner_tags = filtered_df.select("winner_tag")
    loser_tags = filtered_df.select("loser_tag")
    all_tags = winner_tags.union(loser_tags)
    all_tags.cache()
    
    # Rename column with tags to player_tags
    all_tags = all_tags.withColumnRenamed("winner_tag", "player_tags")

    # Count how many unique player_tags
    number_of_players = all_tags.select("player_tags").distinct().count()
    all_tags.unpersist()
    
    # Convert the number into a nice, easily readable string
    number_of_players = convert_readable_number(number_of_players)
    
    return number_of_players

def convert_readable_number(number):
    
    number = str(number)
    
    if len(number) <= 3:
        return number
    elif len(number) == 4:
        return number[0] + "," + number[1:]
    elif len(number) == 5:
        return number[0:2] + "," + number[2:]
    elif len(number) == 6:
        return number[0:3] + "," + number[3:]
    elif len(number) == 7:
        return number[0] + "," + number[1:4] + "," + number[4:]
    elif len(number) == 8:
        return number[0:2] + "," + number[2:5] + "," + number[5:]
    elif len(number) == 9:
        return number[0:3] + "," + number[3:6] + "," + number[6:]
    else:
        return "Some Very Large Number, Yikes"


def calculate_number_of_battles(filtered_df):
    """Calculate the number of battles."""
    id = filtered_df.select("id")
    id.cache()

    # Count how many rows of data as this is equal to the number of battles
    number_of_battles = id.count()
    id.unpersist()
    
    # Convert the number into a nice, easily readable string
    number_of_battles = convert_readable_number(number_of_battles)
    
    return number_of_battles


def display_metrics(filtered_df):
    """Display key metrics in Streamlit."""
    # Calculate metrics
    total_crowns = calculate_total_crowns(filtered_df)
    number_of_players = calculate_number_of_players(filtered_df)
    number_of_battles = calculate_number_of_battles(filtered_df)
    
    col4, col5, col6 = st.columns(3, vertical_alignment="center")
    with col4:
        crown = Image.open("app/images/crown_resized.png")
        crown = crown.resize((70, 70))
        st.image(crown)
        
    with col5:
        king = Image.open("app/images/blue_king.png")
        king = king.resize((72, 80))
        st.image(king)
        
    with col6:
        trophy = Image.open("app/images/trophy.png")
        trophy = trophy.resize((75, 75))
        st.image(trophy)

    
    # Display metrics in columns
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Crowns", value=total_crowns)
    with col2:
        st.metric(label="Number of Players", value=number_of_players)
    with col3:
        st.metric(label="Number of Battles", value=number_of_battles)
        

# Visualisation functions
def create_most_used_cards_bar(df):
    """Create bar chart using pandas DataFrame with the top ten most used cards and their counts"""
    fig = px.bar(df, x='card_name', y='card_count', color = "card_count",
                   labels={"card_name": "Card", "card_count": "Number of Times Used"}, height=400)
    return fig


# Generates a list of image links for cards in descending order of most used cards (top ten only)
def generate_list_image_links(top_ten_df):
    
    card_images_df = pd.read_csv("app/data/raw/card_ids_images.csv", index_col = 0)
    top_ten_images_df = pd.merge(card_images_df, top_ten_df, on = "card_name", how = "inner")
    top_ten_images_df = top_ten_images_df.sort_values("card_count", ascending=False)
    images_in_order = list(top_ten_images_df["image_link"])
    
    return images_in_order


# Takes in the top ten df and uses it to add images to the bar chart
def add_images(top_ten_df):
    # im = Image.open(requests.get(url, stream=True).raw)
    image_list = generate_list_image_links(top_ten_df)
    image_size = (40, 45)
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10 = st.columns(10, vertical_alignment="center")
    with col1:
            card = Image.open(requests.get(image_list[0], stream=True).raw)
            card = card.resize(image_size)
            st.image(card)
            
    with col2:
            card = Image.open(requests.get(image_list[1], stream=True).raw)
            card = card.resize(image_size)
            st.image(card)
            
    with col3:
        card = Image.open(requests.get(image_list[2], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)
        
    with col4:
        card = Image.open(requests.get(image_list[3], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)
        
    with col5:
        card = Image.open(requests.get(image_list[4], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)
        
    with col6:
        card = Image.open(requests.get(image_list[5], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)

    with col7:
        card = Image.open(requests.get(image_list[6], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)
        
    with col8:
        card = Image.open(requests.get(image_list[7], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)
        
    with col9:
        card = Image.open(requests.get(image_list[8], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)
        
    with col10:
        card = Image.open(requests.get(image_list[9], stream=True).raw)
        card = card.resize(image_size)
        st.image(card)


def display_visualisations(filtered_df):
    """Display visualisations in Streamlit."""
    # Top 10 Most Used Cards Bar Chart
    # Calculate top 10 most used cards as a pandas DataFrame
    df = create_card_counts_with_names(filtered_df)
    
    # Create bar chart for top ten most used cards
    fig1 = create_most_used_cards_bar(df)
    
    # Diplay title of chart
    st.header("Top 10 Most Used Cards", divider="blue")
    
    # Display bar chart
    st.plotly_chart(fig1)
    
    # Display images of top ten most used cards
    add_images(df)
