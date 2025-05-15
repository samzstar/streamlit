from pyspark.sql.types import StringType, DateType, IntegerType, FloatType
from pyspark.sql.functions import round
from spark_setup import setup_spark
import functools

@functools.lru_cache
def load_data(filepath):
    """Load the Clash Royale Dataset from a CSV file."""
    # Setup spark
    spark = setup_spark()
    return spark.read.csv(filepath, header=True, inferSchema=True)

def convert_data_types(df):
    """Convert columns to appropriate data types."""

    df = df.withColumn("id", df.id.cast(IntegerType()))
    df = df.withColumn("gamemode_id", df.gamemode_id.cast(IntegerType()))
    df = df.withColumn("avg_starting_trophies", df.avg_starting_trophies.cast(FloatType()))
    df = df.withColumn("winner_tag", df.winner_tag.cast(StringType()))
    df = df.withColumn("winner_starting_trophies", df.winner_starting_trophies.cast(IntegerType()))
    df = df.withColumn("winner_trophy_change", df.winner_trophy_change.cast(IntegerType()))
    df = df.withColumn("winner_crowns", df.winner_crowns.cast(IntegerType()))
    df = df.withColumn("loser_tag", df.loser_tag.cast(StringType()))
    df = df.withColumn("loser_starting_trophies", df.loser_starting_trophies.cast(IntegerType()))
    df = df.withColumn("loser_trophy_change", df.loser_trophy_change.cast(IntegerType()))
    df = df.withColumn("loser_crowns", df.loser_crowns.cast(IntegerType()))
    df = df.withColumn("winner_card1_id", df.winner_card1_id.cast(IntegerType()))
    df = df.withColumn("winner_card1_level", df.winner_card1_level.cast(IntegerType()))
    df = df.withColumn("winner_card2_id", df.winner_card2_id.cast(IntegerType()))
    df = df.withColumn("winner_card2_level", df.winner_card2_level.cast(IntegerType()))
    df = df.withColumn("winner_card3_id", df.winner_card3_id.cast(IntegerType()))
    df = df.withColumn("winner_card3_level", df.winner_card3_level.cast(IntegerType()))
    df = df.withColumn("winner_card4_id", df.winner_card4_id.cast(IntegerType()))
    df = df.withColumn("winner_card4_level", df.winner_card4_level.cast(IntegerType()))
    df = df.withColumn("winner_card5_id", df.winner_card5_id.cast(IntegerType()))
    df = df.withColumn("winner_card5_level", df.winner_card5_level.cast(IntegerType()))
    df = df.withColumn("winner_card6_id", df.winner_card6_id.cast(IntegerType()))
    df = df.withColumn("winner_card6_level", df.winner_card6_level.cast(IntegerType()))
    df = df.withColumn("winner_card7_id", df.winner_card7_id.cast(IntegerType()))
    df = df.withColumn("winner_card7_level", df.winner_card7_level.cast(IntegerType()))
    df = df.withColumn("winner_card8_id", df.winner_card8_id.cast(IntegerType()))
    df = df.withColumn("winner_card8_level", df.winner_card8_level.cast(IntegerType()))
    df = df.withColumn("winner_total_card_level", df.winner_total_card_level.cast(IntegerType()))
    df = df.withColumn("winner_elixir_average", df.winner_elixir_average.cast(FloatType()))
    df = df.withColumn("winner_elixir_average", round(df.winner_elixir_average, 2))
    df = df.withColumn("loser_card1_id", df.loser_card1_id.cast(IntegerType()))
    df = df.withColumn("loser_card1_level", df.loser_card1_level.cast(IntegerType()))
    df = df.withColumn("loser_card2_id", df.loser_card2_id.cast(IntegerType()))
    df = df.withColumn("loser_card2_level", df.loser_card2_level.cast(IntegerType()))
    df = df.withColumn("loser_card3_id", df.loser_card3_id.cast(IntegerType()))
    df = df.withColumn("loser_card3_level", df.loser_card3_level.cast(IntegerType()))
    df = df.withColumn("loser_card4_id", df.loser_card4_id.cast(IntegerType()))
    df = df.withColumn("loser_card4_level", df.loser_card4_level.cast(IntegerType()))
    df = df.withColumn("loser_card5_id", df.loser_card5_id.cast(IntegerType()))
    df = df.withColumn("loser_card5_level", df.loser_card5_level.cast(IntegerType()))
    df = df.withColumn("loser_card6_id", df.loser_card6_id.cast(IntegerType()))
    df = df.withColumn("loser_card6_level", df.loser_card6_level.cast(IntegerType()))
    df = df.withColumn("loser_card7_id", df.loser_card7_id.cast(IntegerType()))
    df = df.withColumn("loser_card7_level", df.loser_card7_level.cast(IntegerType()))
    df = df.withColumn("loser_card8_id", df.loser_card8_id.cast(IntegerType()))
    df = df.withColumn("loser_card8_level", df.loser_card8_level.cast(IntegerType()))
    df = df.withColumn("loser_total_card_level", df.loser_total_card_level.cast(IntegerType()))
    df = df.withColumn("loser_elixir_average", df.loser_elixir_average.cast(FloatType()))
    df = df.withColumn("loser_elixir_average", round(df.loser_elixir_average, 2))
    df = df.withColumn("battle_date", df.battle_date.cast(DateType()))
    
    return df

def load_and_clean_data(filepath):
    """Load and clean the Clash Royale data."""
    df = load_data(filepath)
    df = convert_data_types(df)
    
    return df
