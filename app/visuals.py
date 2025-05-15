import streamlit as st
import plotly.express as px
import pyspark.sql.functions as F
from most_used_cards import create_card_counts_with_names
from PIL import Image

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

# def calculate_most_used_card(filtered_df):
#     """Calculate the average age of passengers."""
#     return filtered_df["Age"].mean()


# def calculate_least_used_card(filtered_df):
#     """Calculate the survival rate by gender."""
#     return filtered_df.groupby("Sex", observed=False)["Survived"].mean()


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
        
# st.write("a logo and text next to eachother")
# col1, mid, col2 = st.beta_columns([1,1,20])
# with col1:
#     st.image('row_2_col_1.jpg', width=60)
# with col2:
#     st.write('A Name')
    
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


def display_visualisations(filtered_df):
    """Display visualisations in Streamlit."""
    # Top 10 Most Used Cards Bar Chart
    # Calculate top 10 most used cards as a pandas DataFrame
    df = create_card_counts_with_names(filtered_df)
    
    # Create bar chart for top ten most used cards
    fig1 = create_most_used_cards_bar(df)
    
    # Diplay ttile of chart
    st.header("Top 10 Most Used Cards", divider="blue")
    
    # Display bar chart
    st.plotly_chart(fig1)