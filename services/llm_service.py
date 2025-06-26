import streamlit as st
import pandas as pd
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from typing import Dict, List
from services.query_classifier import classify_query_type
from services.query_generator import generate_sql_query

def get_response(user_query: str, metabase_client, database_id: int, chat_history: list):
    query_type = classify_query_type(user_query)
    llm = ChatOpenAI(model="deepseek/deepseek-chat:free", temperature=0.4)
    
    if query_type == "data_query":
        # Get detailed table information
        tables = metabase_client.get_tables(database_id)
        
        if not tables:
            return "❌ Tidak dapat mengakses tabel database. Pastikan koneksi database sudah benar."
        
        # Generate SQL query with enhanced logic
        sql_query = generate_sql_query(user_query, tables, chat_history, metabase_client, database_id)
        
        with st.expander("🔍 SQL Query yang Digunakan", expanded=False):
            st.code(sql_query, language="sql")
        
        # Execute query
        with st.spinner("Menjalankan query..."):
            df = metabase_client.execute_query(database_id, sql_query)
        
        if not df.empty:
            st.subheader("📊 Hasil Query")
            st.dataframe(df, use_container_width=True)
            
            # Generate comprehensive insights
            prompt = ChatPromptTemplate.from_template("""
Sebagai analis data ahli, berikan analisis mendalam berdasarkan hasil query berikut:

Pertanyaan User: {question}
SQL Query: {query}
Jumlah Data: {row_count} baris
Kolom: {columns}

Data Sample (5 baris pertama):
{sample_data}

Statistik Ringkas:
{data_summary}

Tugas Anda:
1. Berikan ringkasan eksekutif dari hasil analisis
2. Identifikasi insight utama dan pola menarik
3. Berikan interpretasi bisnis yang actionable
4. Sertakan rekomendasi strategis jika relevan
5. Gunakan bahasa Indonesia yang profesional namun mudah dipahami

Format response dengan struktur yang jelas dan numbering untuk kemudahan pembacaan.
""")
            
            # Prepare data summary
            sample_data = df.head(5).to_string(index=False)
            data_summary = ""
            
            # Generate statistics for numeric columns
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                data_summary += "Statistik Numerik:\n"
                for col in numeric_cols:
                    data_summary += f"- {col}: Min={df[col].min()}, Max={df[col].max()}, Avg={df[col].mean():.2f}\n"
            
            # Generate frequency for categorical columns (top 3 values)
            categorical_cols = df.select_dtypes(include=['object']).columns
            if len(categorical_cols) > 0:
                data_summary += "\nTop Values per Kategori:\n"
                for col in categorical_cols[:3]:  # Limit to first 3 categorical columns
                    top_values = df[col].value_counts().head(3)
                    data_summary += f"- {col}: {', '.join([f'{k}({v})' for k, v in top_values.items()])}\n"
            
            chain = prompt | llm | StrOutputParser()
            analysis = chain.invoke({
                "question": user_query,
                "query": sql_query,
                "row_count": len(df),
                "columns": ", ".join(df.columns.tolist()),
                "sample_data": sample_data,
                "data_summary": data_summary
            })
            
            return analysis
            
        else:
            return "❌ Query tidak mengembalikan hasil. Coba pertanyaan yang lebih spesifik atau periksa ketersediaan data."
    
    elif query_type == "dashboard_info":
        dashboards = metabase_client.get_dashboards()
        if dashboards:
            dashboard_list = "\n".join([f"- {dash['name']}: {dash['description']}" for dash in dashboards[:10]])
            
            prompt = ChatPromptTemplate.from_template("""
User bertanya tentang dashboard: {question}

Daftar Dashboard yang Tersedia:
{dashboard_list}

Berikan informasi yang relevan tentang dashboard yang diminta, serta saran dashboard mana yang paling sesuai untuk kebutuhan user.
""")
            
            chain = prompt | llm | StrOutputParser()
            return chain.invoke({"question": user_query, "dashboard_list": dashboard_list})
        else:
            return "❌ Tidak dapat mengakses daftar dashboard."
    
    elif query_type == "card_info":
        cards = metabase_client.get_cards()
        if cards:
            card_list = "\n".join([f"- {card['name']}: {card['description']}" for card in cards[:10]])
            
            prompt = ChatPromptTemplate.from_template("""
User bertanya tentang cards/questions: {question}

Daftar Cards/Questions yang Tersedia:
{card_list}

Berikan informasi yang relevan tentang cards yang diminta, serta saran cards mana yang paling sesuai untuk kebutuhan user.
""")
            
            chain = prompt | llm | StrOutputParser()
            return chain.invoke({"question": user_query, "card_list": card_list})
        else:
            return "❌ Tidak dapat mengakses daftar cards/questions."
    
    elif query_type == "recommendation":
        # Get some sample data for context
        tables = metabase_client.get_tables(database_id)
        if tables:
            main_table = f"{tables[0]['schema']}.{tables[0]['table']}"
            sample_query = f"SELECT * FROM {main_table} LIMIT 100"
            df = metabase_client.execute_query(database_id, sample_query)
            
            if not df.empty:
                # Prepare data context for recommendations
                data_context = f"Dataset memiliki {len(df)} sampel data dengan kolom: {', '.join(df.columns.tolist())}"
                
                # Add some basic statistics
                if len(df.select_dtypes(include=['number']).columns) > 0:
                    numeric_summary = df.select_dtypes(include=['number']).describe().to_string()
                    data_context += f"\n\nStatistik Dasar:\n{numeric_summary}"
                
                prompt = ChatPromptTemplate.from_template("""
Sebagai konsultan bisnis berpengalaman, berikan rekomendasi strategis berdasarkan pertanyaan berikut:

Pertanyaan User: {question}

Konteks Data:
{data_context}

Berikan rekomendasi yang:
1. Actionable dan praktis
2. Berdasarkan data yang tersedia
3. Mengidentifikasi peluang bisnis
4. Mencakup langkah implementasi
5. Mempertimbangkan risiko dan mitigasi

Format dalam bahasa Indonesia yang profesional dan terstruktur.
""")
                
                chain = prompt | llm | StrOutputParser()
                return chain.invoke({
                    "question": user_query,
                    "data_context": data_context
                })
            else:
                return "❌ Tidak dapat mengakses data untuk memberikan rekomendasi."
        else:
            return "❌ Tidak dapat mengakses tabel untuk analisis rekomendasi."
    
    else:  # general
        prompt = ChatPromptTemplate.from_template("""
Sebagai asisten analitik data yang ramah, jawab pertanyaan umum berikut dengan informatif:

Pertanyaan: {question}

Berikan jawaban yang membantu dan jika relevan, arahkan user untuk mengajukan pertanyaan analitik yang lebih spesifik tentang data mereka.
""")
        
        chain = prompt | llm | StrOutputParser()
        return chain.invoke({"question": user_query})