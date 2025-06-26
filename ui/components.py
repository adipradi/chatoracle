import streamlit as st

def render_page_header():
    st.set_page_config(page_title="Chat Metabase + OpenRouter", page_icon="📊")
    st.title("📊 Chatbot Analitik Metabase (OpenRouter)")

def initialize_session_state():
    if "chat_history" not in st.session_state:
        st.session_state.initial_message = st.session_state.get("initial_message", 
            "Halo! Saya siap bantu jawab pertanyaan analitik dari Metabase Anda.")
        st.session_state.chat_history = [
            st.session_state.initial_message,
        ]
    if "metabase_client" not in st.session_state:
        st.session_state.metabase_client = None
    if "selected_database_id" not in st.session_state:
        st.session_state.selected_database_id = None
    if "table_structure_analyzed" not in st.session_state:
        st.session_state.table_structure_analyzed = False