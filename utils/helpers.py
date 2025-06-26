import os
from typing import Dict, List, Optional, Any

def load_environment_variables() -> Dict[str, str]:
    """Load and validate environment variables"""
    api_key = os.getenv("OPENROUTER_API_KEY")
    base_url = os.getenv("OPENROUTER_BASE_URL")
    
    if not api_key or not base_url:
        raise ValueError("❌ OPENROUTER_API_KEY dan OPENROUTER_BASE_URL belum diset!")
    
    # Set up for LangChain
    os.environ["OPENAI_API_KEY"] = api_key
    os.environ["OPENAI_BASE_URL"] = base_url
    
    return {
        "api_key": api_key,
        "base_url": base_url
    }

def clean_sql_query(sql_query: str) -> str:
    """Clean and format SQL query string"""
    sql_query = sql_query.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    elif sql_query.startswith("```"):
        sql_query = sql_query.replace("```", "").strip()
    
    return sql_query

def format_database_info(databases: List[Dict]) -> Dict[str, int]:
    """Format database information for UI display"""
    if not databases:
        return {}
    
    return {f"{db['name']} ({db['engine']})": db["id"] for db in databases}

def safe_get_nested_value(data: Dict, keys: List[str], default: Any = None) -> Any:
    """Safely get nested dictionary value"""
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def parse_api_response(data: Any, expected_structure: str = "list") -> List[Dict]:
    """Parse API response with different possible structures"""
    results = []
    
    if expected_structure == "list" and isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("id") is not None:
                results.append(item)
    
    elif isinstance(data, dict):
        # Try different nested structures
        for key in ["data", "databases", "dashboards", "cards"]:
            if key in data and isinstance(data[key], list):
                for item in data[key]:
                    if isinstance(item, dict) and item.get("id") is not None:
                        results.append(item)
                break
        
        # If no nested structure found, check if data itself is a valid item
        if not results and "id" in data:
            results.append(data)
        
        # Last resort: iterate through all values
        if not results:
            for key, value in data.items():
                if isinstance(value, dict) and "id" in value:
                    results.append(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict) and item.get("id") is not None:
                            results.append(item)
    
    return results

def generate_fallback_query(question: str) -> str:
    """Generate fallback SQL query based on question content"""
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