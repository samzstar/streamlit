import streamlit as st
from PIL import Image

def add_league_filter(df):
    """Add a filter for League."""
    return st.sidebar.selectbox(
        "Select League",
        options=['Challenger I', 'Challenger II', 'Challenger III', 'Master I', 'Master II', 'Master III',
                 'Champion', 'Grand Champion', 'Royal Champion', 'Ultimate Champion']
    )

def filter_dataframe(df, league_filter):
    """Filter the DataFrame based on selected filters."""
    return df.filter(df["league"] == league_filter)

def add_league_image(league):
    
    if league == "Challenger I":
        icon = Image.open("app/images/challenger_I.png")
        icon.resize((50, 50))
        
    elif league == "Challenger II":
        icon = Image.open("app/images/challenger_II.png")
        icon.resize((50, 50))
        
    elif league == "Challenger III":
        icon = Image.open("app/images/challenger_III.png")
        icon.resize((50, 50))
    
    elif league == "Master I":
        icon = Image.open("app/images/master_I.png")
        icon.resize((50, 50))
        
    elif league == "Master II":
        icon = Image.open("app/images/master_II.png")
        icon.resize((50, 50))
    
    elif league == "Master III":
        icon = Image.open("app/images/master_III.png")
        icon.resize((50, 50))
    
    elif league == "Champion":
        icon = Image.open("app/images/champion.png")
        icon.resize((50, 50))
        
    elif league == "Grand Champion":
        icon = Image.open("app/images/grand_champion.png")
        icon.resize((50, 50))
        
    elif league == "Royal Champion":
        icon = Image.open("app/images/royal_champion.png")
        icon.resize((50, 50))
        
    elif league == "Ultimate Champion":
        icon = Image.open("app/images/ultimate_champion.png")
        icon.resize((50, 50))
        
    st.sidebar.image(icon)
    
def apply_filter(df):
    """Apply all filters and return the filtered DataFrame."""
    st.sidebar.header("Filters")
    league_filter = add_league_filter(df)
    add_league_image(league_filter)
    
    return filter_dataframe(df, league_filter)