# File: scheduled_analysis.py (updated function)

def generate_recommendations(current_df, previous_df):
    """Compares current signals with previous ones to generate new recommendations."""
    if previous_df.empty:
        current_df['Recommendation'] = 'First run, no prior data to compare.'
        return current_df

    merged_df = pd.merge(
        current_df,
        previous_df[['Instrument', 'Signal']],
        on='Instrument',
        how='left',
        suffixes=('', '_prev')
    ).fillna({'Signal_prev': 'N/A'})

    def get_recommendation(row):
        current = row['Signal']
        previous = row['Signal_prev']
        
        if current == 'Hold for now' and previous == 'Hold for now':
            return "No change."

        direction = 'long' if 'Buy' in current else 'short'

        # ---- THIS IS THE CORRECTED LINE ----
        # It now checks for an exact match instead of a substring.
        if 'Super Strong' in previous and current in ['Strong Buy', 'Strong Sell']:
            return f"📉 Degradation: Consider reducing {direction} positions."

        if 'Strong' in previous and 'Moderate Strong' in current:
            return f"📈 Improvement: Consider re-entering {direction} positions."
        
        if 'Moderate Strong' in previous and 'Super Strong' in current:
            return f"🚀 Alignment: Accumulate {direction} positions."
            
        if 'Strong' in previous and 'Super Strong' in current:
            return f"🔥 Strengthening: Accumulate {direction} positions."

        if 'Hold' in previous or previous == 'N/A':
             return f"New Signal: {current}"
        
        return "Monitor signal change."

    merged_df['Recommendation'] = merged_df.apply(get_recommendation, axis=1)
    
    all_cols = list(merged_df.columns)
    signal_index = all_cols.index('Signal')
    rec_col = merged_df.pop('Recommendation')
    merged_df.insert(signal_index + 1, 'Recommendation', rec_col)
    
    merged_df.drop(columns=['Signal_prev'], inplace=True)
    return merged_df
    if __name__ == "__main__":
    main()
