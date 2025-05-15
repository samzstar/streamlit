import clean
import os

def create_card_counts_with_names(enriched_df):
    individual_dfs = create_individual_dfs(enriched_df)
    all_cards_df = join_card_id_dfs(individual_dfs)
    
    # GroupBy the card_ids and count how many times each card was used
    count_cards = all_cards_df.groupBy("winner_card1_id").count()

    # Rename columns to "card_id" and "card_count"
    count_cards = count_cards.withColumnRenamed("winner_card1_id", "card_id")
    count_cards = count_cards.withColumnRenamed("count", "card_count")

    # cache the count_cards df into memory
    # count_cards.cache()
    
    card_filepath = os.path.join(os.path.dirname(__file__),
        "data/raw/card_ids.csv")
    # Load in the card_ids csv as a DataFrame - this contains which card_id refers to what card name 
    # it will help us link the IDs to the card names
    card_ids_df = clean.load_data(card_filepath)
    
    # Join card_ids to count_cards on card_id 
    # so have the card IDs, card names, and number of times the card was used in one DataFrame
    counts_cards_names = count_cards.join(
        card_ids_df, on = 'card_id', how = "left").select(
        count_cards.card_id, card_ids_df.card_name, count_cards.card_count)

    # Uncache count_cards
    # count_cards.unpersist()
    
    # DataFrame with the top 10 most used cards by sorting by counts descending and showing only top 10 records
    counts_cards_names_10 = counts_cards_names.orderBy(counts_cards_names.card_count.desc())
    counts_cards_names_10 = counts_cards_names_10.limit(10)
    counts_cards_names_10 = counts_cards_names_10.toPandas()
    
    return counts_cards_names_10

def create_individual_dfs(enriched_df):
    
    # Storing the columns for the different card_ids as individual dataframes
    win_1 = enriched_df.select("winner_card1_id")
    win_2 = enriched_df.select("winner_card2_id")
    win_3 = enriched_df.select("winner_card3_id")
    win_4 = enriched_df.select("winner_card4_id")
    win_5 = enriched_df.select("winner_card5_id")
    win_6 = enriched_df.select("winner_card6_id")
    win_7 = enriched_df.select("winner_card7_id")
    win_8 = enriched_df.select("winner_card8_id")
    los_1 = enriched_df.select("loser_card1_id")
    los_2 = enriched_df.select("loser_card2_id")
    los_3 = enriched_df.select("loser_card3_id")
    los_4 = enriched_df.select("loser_card4_id")
    los_5 = enriched_df.select("loser_card5_id")
    los_6 = enriched_df.select("loser_card6_id")
    los_7 = enriched_df.select("loser_card7_id")
    los_8 = enriched_df.select("loser_card8_id")
    
    return (win_1, win_2, win_3, win_4, win_5, win_6, win_7, win_8, los_1, los_2, los_3, los_4, los_5, los_6, los_7, los_8)

def join_card_id_dfs(dfs):
    
    # Joining all the dataframes for the card_ids one below the other to get all the card_ids
    # Every record of the card_id is a battle where that card was used
    
    all_cards_df = dfs[0].union(dfs[1])
    
    for i in range(2, 16):
        all_cards_df = all_cards_df.union(dfs[i])
    
    return all_cards_df