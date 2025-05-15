import streamlit as st
from PIL import Image
from clean import load_and_clean_data
from filters import apply_filter
from visuals import display_metrics, display_visualisations
import clean
import os

def main():
    """Main function to run the Streamlit app."""
    
    filepath_shield = os.path.join(os.path.dirname(__file__),
        "images/clash_shield_sized.png")
    # Storing images
    clash_shield_logo = Image.open(filepath_shield)
    clash_shield_logo = clash_shield_logo.resize((32, 32))

    filepath_logo = os.path.join(os.path.dirname(__file__),
        "images/clash_logo.png")
    clash_logo = Image.open(filepath_logo)
    
    st.set_page_config(
        page_title="Clash Royale Logmas Dashboard",
        page_icon = clash_shield_logo,
        layout="wide",
        initial_sidebar_state="auto",
    )
    
    # Set the title of the app
    st.title("Clash Royale S18 (Logmas): Ladder Stats")
    
    # Set logo for the app
    st.logo(clash_logo)
    
    filepath_data = os.path.join(os.path.dirname(__file__),
        "data/raw/enriched_data.csv")
    # Load and clean the data
    clash_df = clean.load_and_clean_data(filepath_data)

    # Apply filter
    filtered_df = apply_filter(clash_df)
    
    # Display Metrics
    display_metrics(filtered_df)
    
    # Display Visualisations
    display_visualisations(filtered_df)
    
if __name__ == "__main__":
    main()