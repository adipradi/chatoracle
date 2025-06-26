import streamlit as st
import pandas as pd
import requests
from typing import Dict, List, Optional

class MetabaseClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session_token = None
        self.username = username
        self.password = password
        self.table_schemas = {}  # Cache for table schemas
        
    def authenticate(self):
        """Authenticate with Metabase and get session token"""
        try:
            response = self.session.post(
                f"{self.base_url}/api/session",
                json={"username": self.username, "password": self.password}
            )
            response.raise_for_status()
            self.session_token = response.json()["id"]
            self.session.headers.update({"X-Metabase-Session": self.session_token})
            return True
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            return False
    
    def get_databases(self) -> List[Dict]:
        """Get list of available databases"""
        try:
            response = self.session.get(f"{self.base_url}/api/database")
            response.raise_for_status()
            data = response.json()
            
            databases = []
            
            # Handle different response formats
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("id") is not None:
                        databases.append({
                            "id": item.get("id"),
                            "name": item.get("name", f"Database {item.get('id')}"),
                            "engine": item.get("engine", "Unknown")
                        })
            
            elif isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    for item in data["data"]:
                        if isinstance(item, dict) and item.get("id") is not None:
                            databases.append({
                                "id": item.get("id"),
                                "name": item.get("name", f"Database {item.get('id')}"),
                                "engine": item.get("engine", "Unknown")
                            })
                
                elif "databases" in data and isinstance(data["databases"], list):
                    for item in data["databases"]:
                        if isinstance(item, dict) and item.get("id") is not None:
                            databases.append({
                                "id": item.get("id"),
                                "name": item.get("name", f"Database {item.get('id')}"),
                                "engine": item.get("engine", "Unknown")
                            })
                
                else:
                    if "id" in data and "name" in data:
                        databases.append({
                            "id": data.get("id"),
                            "name": data.get("name", f"Database {data.get('id')}"),
                            "engine": data.get("engine", "Unknown")
                        })
                    else:
                        for key, value in data.items():
                            if isinstance(value, dict) and "id" in value:
                                databases.append({
                                    "id": value.get("id"),
                                    "name": value.get("name", key),
                                    "engine": value.get("engine", "Unknown")
                                })
                            elif isinstance(value, list):
                                for item in value:
                                    if isinstance(item, dict) and item.get("id") is not None:
                                        databases.append({
                                            "id": item.get("id"),
                                            "name": item.get("name", f"Database {item.get('id')}"),
                                            "engine": item.get("engine", "Unknown")
                                        })
            
            return databases
        except Exception as e:
            st.error(f"Failed to get databases: {e}")
            return []
    
    def get_tables(self, database_id: int) -> List[Dict]:
        """Get tables for a specific database"""
        try:
            response = self.session.get(f"{self.base_url}/api/database/{database_id}/metadata")
            response.raise_for_status()
            data = response.json()
            
            tables = []
            
            if isinstance(data, dict) and "tables" in data:
                tables_data = data["tables"]
                if isinstance(tables_data, list):
                    for table in tables_data:
                        if isinstance(table, dict):
                            schema_name = table.get("schema", "public")
                            table_name = table.get("name", table.get("display_name"))
                            if table_name:
                                table_info = {
                                    "schema": schema_name, 
                                    "table": table_name,
                                    "id": table.get("id"),
                                    "fields": []
                                }
                                
                                # Get field information
                                if "fields" in table and isinstance(table["fields"], list):
                                    for field in table["fields"]:
                                        field_info = {
                                            "name": field.get("name"),
                                            "type": field.get("base_type", "Unknown"),
                                            "display_name": field.get("display_name")
                                        }
                                        table_info["fields"].append(field_info)
                                
                                tables.append(table_info)
                                
                                # Cache the schema
                                full_table_name = f"{schema_name}.{table_name}"
                                self.table_schemas[full_table_name] = table_info["fields"]
            
            # Fallback if no tables found
            if not tables:
                fallback_tables = [
                    {
                        "schema": "mb", 
                        "table": "khs_customer_transactions", 
                        "id": None,
                        "fields": [
                            {"name": "REQUEST_ID", "type": "VARCHAR", "display_name": "Request ID"},
                            {"name": "REQUEST_DATE", "type": "DATE", "display_name": "Request Date"},
                            {"name": "CREATION_DATE", "type": "TIMESTAMP", "display_name": "Creation Date"},
                            {"name": "CUSTOMER_NAME", "type": "VARCHAR", "display_name": "Customer Name"},
                            {"name": "CUSTOMER_CITY", "type": "VARCHAR", "display_name": "Customer City"},
                            {"name": "CUSTOMER_PROVINCE", "type": "VARCHAR", "display_name": "Customer Province"},
                            {"name": "ITEM_CODE", "type": "VARCHAR", "display_name": "Item Code"},
                            {"name": "ITEM_DESCRIPTION", "type": "VARCHAR", "display_name": "Item Description"},
                            {"name": "ITEM_TYPE", "type": "VARCHAR", "display_name": "Item Type"},
                            {"name": "QUANTITY", "type": "INTEGER", "display_name": "Quantity"},
                            {"name": "PRICE", "type": "DECIMAL", "display_name": "Unit Price"},
                            {"name": "TOTAL_PRICE", "type": "DECIMAL", "display_name": "Total Price"},
                            {"name": "ORG_NAME", "type": "VARCHAR", "display_name": "Organization"},
                            {"name": "INVOICE_NUMBER", "type": "VARCHAR", "display_name": "Invoice Number"},
                            {"name": "SO_NUMBER", "type": "VARCHAR", "display_name": "SO Number"}
                        ]
                    }
                ]
                self.table_schemas["mb.khs_customer_transactions"] = fallback_tables[0]["fields"]
                return fallback_tables
            
            return tables
            
        except Exception as e:
            st.error(f"Failed to get tables: {e}")
            # Return fallback with schema
            fallback_schema = [
                {"name": "REQUEST_ID", "type": "VARCHAR", "display_name": "Request ID"},
                {"name": "REQUEST_DATE", "type": "DATE", "display_name": "Request Date"},
                {"name": "CREATION_DATE", "type": "TIMESTAMP", "display_name": "Creation Date"},
                {"name": "CUSTOMER_NAME", "type": "VARCHAR", "display_name": "Customer Name"},
                {"name": "CUSTOMER_CITY", "type": "VARCHAR", "display_name": "Customer City"},
                {"name": "CUSTOMER_PROVINCE", "type": "VARCHAR", "display_name": "Customer Province"},
                {"name": "ITEM_CODE", "type": "VARCHAR", "display_name": "Item Code"},
                {"name": "ITEM_DESCRIPTION", "type": "VARCHAR", "display_name": "Item Description"},
                {"name": "ITEM_TYPE", "type": "VARCHAR", "display_name": "Item Type"},
                {"name": "QUANTITY", "type": "INTEGER", "display_name": "Quantity"},
                {"name": "PRICE", "type": "DECIMAL", "display_name": "Unit Price"},
                {"name": "TOTAL_PRICE", "type": "DECIMAL", "display_name": "Total Price"},
                {"name": "ORG_NAME", "type": "VARCHAR", "display_name": "Organization"},
                {"name": "INVOICE_NUMBER", "type": "VARCHAR", "display_name": "Invoice Number"},
                {"name": "SO_NUMBER", "type": "VARCHAR", "display_name": "SO Number"}
            ]
            self.table_schemas["mb.khs_customer_transactions"] = fallback_schema
            return [{"schema": "mb", "table": "khs_customer_transactions", "id": None, "fields": fallback_schema}]
    
    def analyze_table_structure(self, database_id: int, table_name: str) -> Dict:
        """Analyze table structure by running sample queries"""
        try:
            # Try to get basic info about the table
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            df = self.execute_query(database_id, sample_query)
            
            if not df.empty:
                structure_info = {
                    "columns": list(df.columns),
                    "sample_data": df.head(3).to_dict('records'),
                    "row_count_estimate": len(df),
                    "data_types": {col: str(df[col].dtype) for col in df.columns}
                }
                
                # Try to get actual row count
                try:
                    count_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
                    count_df = self.execute_query(database_id, count_query)
                    if not count_df.empty:
                        structure_info["total_rows"] = count_df.iloc[0]["total_rows"]
                except:
                    pass
                
                return structure_info
            
        except Exception as e:
            st.warning(f"Could not analyze table structure: {e}")
        
        return {}
    
    def execute_query(self, database_id: int, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            payload = {
                "type": "native",
                "native": {"query": query},
                "database": database_id
            }
            response = self.session.post(f"{self.base_url}/api/dataset", json=payload)
            response.raise_for_status()
            
            result = response.json()
            if "data" in result and "cols" in result["data"] and "rows" in result["data"]:
                columns = [col["name"] for col in result["data"]["cols"]]
                rows = result["data"]["rows"]
                return pd.DataFrame(rows, columns=columns)
            else:
                st.error(f"Unexpected response format: {result}")
                return pd.DataFrame()
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def get_dashboards(self) -> List[Dict]:
        """Get list of available dashboards"""
        try:
            response = self.session.get(f"{self.base_url}/api/dashboard")
            response.raise_for_status()
            data = response.json()
            
            dashboards = []
            
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("id") is not None:
                        dashboards.append({
                            "id": item.get("id"),
                            "name": item.get("name", f"Dashboard {item.get('id')}"),
                            "description": item.get("description", "No description")
                        })
            
            elif isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    for item in data["data"]:
                        if isinstance(item, dict) and item.get("id") is not None:
                            dashboards.append({
                                "id": item.get("id"),
                                "name": item.get("name", f"Dashboard {item.get('id')}"),
                                "description": item.get("description", "No description")
                            })
                
                elif "dashboards" in data and isinstance(data["dashboards"], list):
                    for item in data["dashboards"]:
                        if isinstance(item, dict) and item.get("id") is not None:
                            dashboards.append({
                                "id": item.get("id"),
                                "name": item.get("name", f"Dashboard {item.get('id')}"),
                                "description": item.get("description", "No description")
                            })
                
                else:
                    if "id" in data and "name" in data:
                        dashboards.append({
                            "id": data.get("id"),
                            "name": data.get("name", f"Dashboard {data.get('id')}"),
                            "description": data.get("description", "No description")
                        })
                    else:
                        for key, value in data.items():
                            if isinstance(value, dict) and "id" in value:
                                dashboards.append({
                                    "id": value.get("id"),
                                    "name": value.get("name", key),
                                    "description": value.get("description", "No description")
                                })
                            elif isinstance(value, list):
                                for item in value:
                                    if isinstance(item, dict) and item.get("id") is not None:
                                        dashboards.append({
                                            "id": item.get("id"),
                                            "name": item.get("name", f"Dashboard {item.get('id')}"),
                                            "description": item.get("description", "No description")
                                        })
            
            return dashboards
        except Exception as e:
            st.error(f"Failed to get dashboards: {e}")
            return []
    
    def get_cards(self) -> List[Dict]:
        """Get list of available cards/questions"""
        try:
            response = self.session.get(f"{self.base_url}/api/card")
            response.raise_for_status()
            data = response.json()
            
            cards = []
            
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("id") is not None:
                        cards.append({
                            "id": item.get("id"),
                            "name": item.get("name", f"Question {item.get('id')}"),
                            "description": item.get("description", "No description")
                        })
            
            elif isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    for item in data["data"]:
                        if isinstance(item, dict) and item.get("id") is not None:
                            cards.append({
                                "id": item.get("id"),
                                "name": item.get("name", f"Question {item.get('id')}"),
                                "description": item.get("description", "No description")
                            })
                
                elif "cards" in data and isinstance(data["cards"], list):
                    for item in data["cards"]:
                        if isinstance(item, dict) and item.get("id") is not None:
                            cards.append({
                                "id": item.get("id"),
                                "name": item.get("name", f"Question {item.get('id')}"),
                                "description": item.get("description", "No description")
                            })
                
                else:
                    if "id" in data and "name" in data:
                        cards.append({
                            "id": data.get("id"),
                            "name": data.get("name", f"Question {data.get('id')}"),
                            "description": data.get("description", "No description")
                        })
                    else:
                        for key, value in data.items():
                            if isinstance(value, dict) and "id" in value:
                                cards.append({
                                    "id": value.get("id"),
                                    "name": value.get("name", key),
                                    "description": value.get("description", "No description")
                                })
                            elif isinstance(value, list):
                                for item in value:
                                    if isinstance(item, dict) and item.get("id") is not None:
                                        cards.append({
                                            "id": item.get("id"),
                                            "name": item.get("name", f"Question {item.get('id')}"),
                                            "description": item.get("description", "No description")
                                        })
            
            return cards
        except Exception as e:
            st.error(f"Failed to get cards: {e}")
            return []