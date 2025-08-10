import pandas as pd
import streamlit as st
# Import the logic from the new file
from stock_analyzer_logic import run_full_analysis

# --- Styling Function ---
def style_signals(val):
    """Applies CSS styling to the 'Signal' column."""
    if "Super Strong" in val:
        return 'color: blue; font-weight: bold;'
    if "Strong" in val:
        return 'color: green; font-weight: bold;'
    return ''

# --- Streamlit User Interface ---
def run_streamlit_app():
    st.set_page_config(layout="wide")
    st.title("📈 High-Conviction Signal Screener")
    st.write("Displays 'Strong' (daily) and 'Super Strong' (daily + 30min) signals with risk management levels.")
    
    st.subheader("Enter Tickers to Analyze")
    uploaded_file = st.file_uploader("Choose a .csv or .txt file.", type=["csv", "txt"])
    user_input = st.text_area("Or enter tickers manually (comma or space separated)")

    if st.button("Find Setups"):
        tickers_to_analyze = []
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df_from_csv = pd.read_csv(uploaded_file)
                    # Assuming the first column contains tickers
                    tickers_to_analyze = df_from_csv.iloc[:, 0].dropna().unique().tolist()
                else:
                    string_data = uploaded_file.getvalue().decode("utf-8")
                    tickers_to_analyze = list(set(string_data.upper().replace(',', ' ').split()))
                st.info(f"Loaded {len(tickers_to_analyze)} unique tickers from {uploaded_file.name}")
            except Exception as e:
                st.error(f"Error reading file: {e}")

        elif user_input:
            tickers_to_analyze = list(set(user_input.upper().replace(',', ' ').split()))

        if not tickers_to_analyze:
            st.warning("Please enter at least one ticker.")
        else:
            status_text = st.empty()
            # The main analysis loop is now a single function call
            # We pass the status_text.text function as a callback for progress updates
            full_results_df = run_full_analysis(tickers_to_analyze, status_callback=status_text.text)
            
            status_text.success("Analysis Complete!")
            
            if not full_results_df.empty:
                actionable_df = full_results_df[full_results_df['Signal'] != 'Hold for now'].reset_index(drop=True)

                if not actionable_df.empty:
                    public_view, internal_view = st.tabs(["Public View", "Internal View"])

                    with public_view:
                        st.subheader("Public Recommendations")
                        public_df = actionable_df[['Instrument', 'Signal', 'Entry Price', 'Stop Loss']]
                        # Apply styling
                        styled_public = public_df.style.apply(lambda col: col.map(style_signals), subset=['Signal'])
                        st.dataframe(styled_public, use_container_width=True, hide_index=True)

                    with internal_view:
                        st.subheader("Internal Analysis Details")
                        # Apply styling
                        styled_internal = actionable_df.style.apply(lambda col: col.map(style_signals), subset=['Signal'])
                        st.dataframe(styled_internal, use_container_width=True, hide_index=True)
                else:
                    st.info("No 'Strong' or 'Super Strong' signals found among the analyzed tickers.")
            else:
                st.info("No instruments were analyzed or data could be fetched.")

if __name__ == "__main__":
    run_streamlit_app()
