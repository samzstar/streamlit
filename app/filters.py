import streamlit as st
from PIL import Image

def add_league_filter(df):
    """Add a filter for League."""
    return st.sidebar.selectbox(
        "Select League",
        options=['All Leagues', 'Challenger I', 'Challenger II', 'Challenger III', 'Master I', 'Master II', 'Master III',
                 'Champion', 'Grand Champion', 'Royal Champion', 'Ultimate Champion']
    )

def filter_dataframe_league(df, league_filter):
    """Filter the DataFrame based on selected league filter."""
    if league_filter == "All Leagues":
        return df
    else:
        return df.filter(df["league"] == league_filter)

def add_league_image(league):
    
    if league == "All Leagues":
        icon = Image.open("app/images/all_leagues.png")
        icon.resize((50, 50))
    
    elif league == "Challenger I":
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


def distinct_player_tags(filtered_df):
    """Takes in the dataframe filtered by league.
    Returns the distinct player tags in a list."""

    # Creates a new dataframe with all player tags
    all_tags = create_all_tags_df(filtered_df)

    # Collects distinct player tags into a list
    distinct_players = all_tags.select("player_tag").distinct()
    distinct_players_list = distinct_players.select("player_tag").rdd.map(lambda x: x[0]).collect()
    all_tags.unpersist()
    
    return distinct_players_list

# Join loser tags to bottom of winner tags column to create a column with all player tags
def create_all_tags_df(filtered_df):
    """Takes in filtered dataframe and combines winner tags and loser tags in one column.
    Renames the new column to player_tags.
    Returns a 1 column dataframe with all player tags."""
    
    winner_tags = filtered_df.select("winner_tag")
    loser_tags = filtered_df.select("loser_tag")
    all_tags = winner_tags.union(loser_tags)
    all_tags.cache()
    
    # Rename column with tags to player_tag
    all_tags = all_tags.withColumnRenamed("winner_tag", "player_tag")
    return all_tags


def add_player_filter(distinct_players):
    """Add a filter for player tags."""
    return st.sidebar.selectbox(
        "Select Player Tag",
        options= ["All Players"] + distinct_players
    )


def filter_dataframe_player(df, player_filter):
    """Filter the DataFrame based on selected player filter.
    Return filtered DataFrame"""
    if player_filter == "All Players":
        return df
    else:
        return df.filter((df.winner_tag == player_filter) | (df.loser_tag == player_filter))


def apply_filter(df):
    """Apply all filters and return the filtered DataFrame."""
    st.sidebar.header("Filters")
    league_filter = add_league_filter(df)
    add_league_image(league_filter)
    
    filtered_df = filter_dataframe_league(df, league_filter)
    
    distinct_players = distinct_player_tags(filtered_df)
    player_filter = add_player_filter(distinct_players)
    
    return filter_dataframe_player(filtered_df, player_filter)