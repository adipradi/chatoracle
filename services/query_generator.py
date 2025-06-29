import streamlit as st
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import AIMessage, HumanMessage
from langchain_openai import ChatOpenAI
from typing import Dict, List

def generate_sql_query(question: str, tables_info: List[Dict], chat_history: list, metabase_client, database_id: int):
    try:
        # Build comprehensive schema information
        schema_details = ""
        main_table = None
        
        for table in tables_info:
            table_name = f"{table['schema']}.{table['table']}"
            schema_details += f"\nTable: {table_name}\n"
            
            if table.get('fields'):
                schema_details += "Columns:\n"
                for field in table['fields']:
                    field_name = field.get('name', 'Unknown')
                    field_type = field.get('type', 'Unknown')
                    display_name = field.get('display_name', field_name)
                    schema_details += f"  - {field_name} ({field_type}) - {display_name}\n"
                
                # Set main table for sales data
                if 'transaction' in table['table'].lower() or 'sales' in table['table'].lower():
                    main_table = table_name
            
            schema_details += "\n"
        
        # If no main table identified, use the first available table
        if not main_table and tables_info:
            main_table = f"{tables_info[0]['schema']}.{tables_info[0]['table']}"
        
        # Analyze table structure if needed
        if main_table and not st.session_state.table_structure_analyzed:
            with st.spinner("Menganalisis struktur tabel..."):
                structure_info = metabase_client.analyze_table_structure(database_id, main_table)
                if structure_info:
                    st.session_state.table_structure_analyzed = True
                    schema_details += f"\nTable Analysis for {main_table}:\n"
                    schema_details += f"- Total columns: {len(structure_info.get('columns', []))}\n"
                    schema_details += f"- Sample data available: {len(structure_info.get('sample_data', []))} rows\n"
                    if 'total_rows' in structure_info:
                        schema_details += f"- Estimated total rows: {structure_info['total_rows']}\n"
        
        prompt = ChatPromptTemplate.from_template("""
You are an expert SQL analyst. Generate a precise SQL query to answer the user's question based on the provided database schema.

Database Schema Information:
{schema_details}

Main Table: {main_table}

User Question: {question}

Previous Context: {chat_context}

SQL Generation Rules:
1. Generate ONLY the SQL query, no explanations or markdown
2. Use proper table names with schema prefixes
3. For sales/revenue questions, focus on TOTAL_PRICE, QUANTITY, PRICE columns
4. For customer analysis, use CUSTOMER_NAME, CUSTOMER_CITY, CUSTOMER_PROVINCE
5. For product analysis, use ITEM_DESCRIPTION, ITEM_CODE, ITEM_TYPE
6. For time-based analysis, use REQUEST_DATE or CREATION_DATE
7. Use appropriate aggregate functions (SUM, COUNT, AVG, MAX, MIN)
8. Include proper GROUP BY clauses when aggregating
9. Use ORDER BY for ranking/sorting results
10. Add LIMIT clause for top/bottom results
11. Handle NULL values appropriately
12. Use proper date formatting and filtering

Query Generation Strategy:
- Identify key entities in the question (customer, product, time period, metric)
- Determine required aggregations and groupings
- Select appropriate columns for the analysis
- Apply filters based on question context
- Sort results meaningfully

Generate the SQL query:
""")
        
        llm = ChatOpenAI(model="mistralai/mistral-small-3.2-24b-instruct:free", temperature=0)
        chain = prompt | llm | StrOutputParser()
        
        # Prepare chat context
        chat_context = ""
        if len(chat_history) > 2:
            recent_messages = chat_history[-4:]  # Last 2 Q&A pairs
            for msg in recent_messages:
                if isinstance(msg, HumanMessage):
                    chat_context += f"Q: {msg.content}\n"
                elif isinstance(msg, AIMessage):
                    chat_context += f"A: {msg.content[:100]}...\n"
        
        sql_query = chain.invoke({
            "question": question,
            "schema_details": schema_details,
            "main_table": main_table or "mb.khs_customer_transactions",
            "chat_context": chat_context
        })
        
        # Clean the SQL query
        sql_query = sql_query.strip()
        if sql_query.startswith("```sql"):
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query.replace("```", "").strip()
        
        return sql_query
        
    except Exception as e:
        st.warning(f"SQL generation failed: {e}")
        # Intelligent fallback based on question content
        question_lower = question.lower()
        
        if any(word in question_lower for word in ["penjualan", "sales", "revenue", "omzet"]):
            if any(word in question_lower for word in ["tertinggi", "terbesar", "highest", "top"]):
                return "SELECT CUSTOMER_NAME, SUM(TOTAL_PRICE) as total_sales FROM mb.khs_customer_transactions GROUP BY CUSTOMER_NAME ORDER BY total_sales DESC LIMIT 10"
            elif any(word in question_lower for word in ["bulan", "month", "bulanan"]):
                return "SELECT EXTRACT(MONTH FROM REQUEST_DATE) as month, SUM(TOTAL_PRICE) as monthly_sales FROM mb.khs_customer_transactions GROUP BY EXTRACT(MONTH FROM REQUEST_DATE) ORDER BY month"
            else:
                return "SELECT CUSTOMER_NAME, ITEM_DESCRIPTION, TOTAL_PRICE, REQUEST_DATE FROM mb.khs_customer_transactions ORDER BY TOTAL_PRICE DESC LIMIT 20"
        
        elif any(word in question_lower for word in ["customer", "pelanggan", "pembeli"]):
            return "SELECT CUSTOMER_NAME, CUSTOMER_CITY, COUNT(*) as transaction_count, SUM(TOTAL_PRICE) as total_spent FROM mb.khs_customer_transactions GROUP BY CUSTOMER_NAME, CUSTOMER_CITY ORDER BY total_spent DESC LIMIT 15"
        
        elif any(word in question_lower for word in ["produk", "product", "item", "barang"]):
            return "SELECT ITEM_DESCRIPTION, COUNT(*) as sold_count, SUM(QUANTITY) as total_quantity, SUM(TOTAL_PRICE) as total_revenue FROM mb.khs_customer_transactions GROUP BY ITEM_DESCRIPTION ORDER BY total_revenue DESC LIMIT 15"
        
        else:
            return "SELECT * FROM mb.khs_customer_transactions ORDER BY REQUEST_DATE DESC LIMIT 10"