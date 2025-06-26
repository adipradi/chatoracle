import streamlit as st
from langchain_core.messages import AIMessage, HumanMessage
from services.llm_service import get_response

def display_chat_history():
    for message in st.session_state.chat_history:
        if isinstance(message, AIMessage):
            with st.chat_message("assistant"):
                st.write(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message("user"):
                st.write(message.content)

def handle_chat_input():
    if st.session_state.metabase_client and st.session_state.selected_database_id:
        # Chat input
        user_query = st.chat_input("Tanyakan sesuatu tentang data Anda...")
        
        if user_query:
            # Add user message to chat history
            st.session_state.chat_history.append(HumanMessage(content=user_query))
            
            # Display user message
            with st.chat_message("user"):
                st.write(user_query)
            
            # Generate and display assistant response
            with st.chat_message("assistant"):
                with st.spinner("Menganalisis pertanyaan..."):
                    response = get_response(
                        user_query, 
                        st.session_state.metabase_client, 
                        st.session_state.selected_database_id,
                        st.session_state.chat_history
                    )
                    st.write(response)
            
            # Add assistant response to chat history
            st.session_state.chat_history.append(AIMessage(content=response))
    
    else:
        if not st.session_state.metabase_client:
            st.info("👈 Silakan hubungkan ke Metabase terlebih dahulu menggunakan sidebar.")
        elif not st.session_state.selected_database_id:
            st.info("👈 Silakan pilih database yang akan digunakan dari sidebar.")