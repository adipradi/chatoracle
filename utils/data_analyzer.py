import pandas as pd
from typing import Dict, List, Optional

def analyze_dataframe(df: pd.DataFrame) -> Dict:
    """Analyze DataFrame and return comprehensive statistics"""
    if df.empty:
        return {}
    
    analysis = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "data_types": {col: str(df[col].dtype) for col in df.columns},
        "null_counts": df.isnull().sum().to_dict(),
        "memory_usage": df.memory_usage(deep=True).sum()
    }
    
    # Numeric columns analysis
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        analysis["numeric_summary"] = {}
        for col in numeric_cols:
            analysis["numeric_summary"][col] = {
                "min": df[col].min(),
                "max": df[col].max(),
                "mean": df[col].mean(),
                "median": df[col].median(),
                "std": df[col].std()
            }
    
    # Categorical columns analysis
    categorical_cols = df.select_dtypes(include=['object']).columns
    if len(categorical_cols) > 0:
        analysis["categorical_summary"] = {}
        for col in categorical_cols[:5]:  # Limit to first 5 categorical columns
            value_counts = df[col].value_counts().head(10)
            analysis["categorical_summary"][col] = {
                "unique_count": df[col].nunique(),
                "top_values": value_counts.to_dict()
            }
    
    return analysis

def prepare_data_summary_text(analysis: Dict) -> str:
    """Convert analysis dictionary to readable text summary"""
    if not analysis:
        return "No data available for analysis."
    
    summary_parts = []
    
    # Basic info
    summary_parts.append(f"Dataset contains {analysis['row_count']} rows and {analysis['column_count']} columns.")
    
    # Numeric summary
    if "numeric_summary" in analysis:
        summary_parts.append("\nNumeric Statistics:")
        for col, stats in analysis["numeric_summary"].items():
            summary_parts.append(f"- {col}: Min={stats['min']:.2f}, Max={stats['max']:.2f}, Avg={stats['mean']:.2f}")
    
    # Categorical summary
    if "categorical_summary" in analysis:
        summary_parts.append("\nTop Values per Category:")
        for col, info in analysis["categorical_summary"].items():
            top_values = info["top_values"]
            top_str = ", ".join([f"{k}({v})" for k, v in list(top_values.items())[:3]])
            summary_parts.append(f"- {col}: {top_str}")
    
    return "\n".join(summary_parts)

def detect_data_patterns(df: pd.DataFrame) -> List[str]:
    """Detect interesting patterns in the data"""
    patterns = []
    
    if df.empty:
        return patterns
    
    # Check for date columns
    date_cols = df.select_dtypes(include=['datetime']).columns
    if len(date_cols) > 0:
        patterns.append(f"Time series data detected with {len(date_cols)} date column(s)")
    
    # Check for high cardinality columns
    for col in df.select_dtypes(include=['object']).columns:
        unique_ratio = df[col].nunique() / len(df)
        if unique_ratio > 0.8:
            patterns.append(f"High cardinality detected in '{col}' column")
    
    # Check for potential duplicates
    if df.duplicated().sum() > 0:
        patterns.append(f"Found {df.duplicated().sum()} duplicate rows")
    
    # Check for missing data patterns
    missing_cols = df.columns[df.isnull().any()].tolist()
    if missing_cols:
        patterns.append(f"Missing data found in {len(missing_cols)} columns")
    
    return patterns