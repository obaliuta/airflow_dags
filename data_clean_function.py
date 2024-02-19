def pokem_report(file_to_read):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(df, sep=";")
    df_groupped = df.groupby('TYPE').agg({
    'NAME':'nunique',
    'TOTAL': 'mean',
    'HP': 'max',
    'ATTACK': 'min'}).reset_index().rename(columns={'TYPE':'Category', 'NAME': 'No of Pokemons', 'TOTAL': 'Avg Points', 'HP': 'Max HP', 'ATTACK':'Min Attack'})
    df_groupped.to_csv('poke_report_by_category.csv', index=False)