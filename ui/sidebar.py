import streamlit as st
from clients.metabase_client import MetabaseClient

def render_sidebar():
    with st.sidebar:
        st.subheader("🔌 Koneksi Metabase")
        metabase_url = st.text_input("Metabase URL", value="http://ai.quick.com:3000")
        metabase_username = st.text_input("Username", value="admin@example.com")
        metabase_password = st.text_input("Password", type="password")
        
        if st.button("Connect to Metabase"):
            with st.spinner("Menghubungkan ke Metabase..."):
                try:
                    client = MetabaseClient(metabase_url, metabase_username, metabase_password)
                    if client.authenticate():
                        st.session_state.metabase_client = client
                        st.session_state.table_structure_analyzed = False
                        st.success("✅ Terhubung ke Metabase!")
                    else:
                        st.error("❌ Koneksi gagal!")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
        
        # Database selection
        if st.session_state.metabase_client:
            st.subheader("🗄️ Pilih Database")
            databases = st.session_state.metabase_client.get_databases()
            if databases:
                db_options = {f"{db['name']} ({db['engine']})": db["id"] for db in databases}
                if db_options:
                    selected_db = st.selectbox("Database", options=list(db_options.keys()))
                    if selected_db:
                        new_db_id = db_options[selected_db]
                        if st.session_state.selected_database_id != new_db_id:
                            st.session_state.selected_database_id = new_db_id
                            st.session_state.table_structure_analyzed = False
                        st.info(f"Database aktif: {selected_db}")
                else:
                    st.warning("No valid databases found.")
            else:
                st.warning("No databases available.")

def render_additional_features():
    st.sidebar.markdown("---")
    st.sidebar.subheader("🛠️ Fitur Tambahan")

    if st.sidebar.button("🗑️ Clear Chat History"):
        st.session_state.chat_history = [
            st.session_state.initial_message,
        ]
        st.rerun()

    if st.sidebar.button("🔄 Refresh Connection"):
        if st.session_state.metabase_client:
            try:
                st.session_state.metabase_client.authenticate()
                st.sidebar.success("✅ Koneksi diperbarui!")
            except Exception as e:
                st.sidebar.error(f"❌ Gagal refresh: {e}")

def render_info_panel():
    with st.sidebar.expander("ℹ️ Informasi Aplikasi"):
        st.write("""
        **Fitur Utama:**
        - Chat interaktif dengan data Metabase
        - Query SQL otomatis
        - Analisis data mendalam
        - Rekomendasi bisnis
        
        **Cara Penggunaan:**
        1. Hubungkan ke Metabase
        2. Pilih database
        3. Ajukan pertanyaan dalam bahasa natural
        
        **Contoh Pertanyaan:**
        - "Siapa pelanggan dengan penjualan tertinggi?"
        - "Bagaimana trend penjualan per bulan?"
        - "Produk apa yang paling laris?"
        """)

def render_status_info():
    if st.session_state.metabase_client:
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Status Koneksi:** 🟢 Terhubung")
        if st.session_state.selected_database_id:
            st.sidebar.markdown(f"**Database ID:** {st.session_state.selected_database_id}")
            
            # Show table count
            try:
                tables = st.session_state.metabase_client.get_tables(st.session_state.selected_database_id)
                st.sidebar.markdown(f"**Jumlah Tabel:** {len(tables)}")
            except:
                pass
    else:
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Status Koneksi:** 🔴 Belum terhubung")
