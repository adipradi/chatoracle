import os

def setup_environment():
    """Setup environment variables for OpenRouter/OpenAI compatibility"""
    api_key = os.getenv("OPENROUTER_API_KEY")
    base_url = os.getenv("OPENROUTER_BASE_URL")

    if not api_key or not base_url:
        raise ValueError("❌ OPENROUTER_API_KEY dan OPENROUTER_BASE_URL belum diset!")

    # Konfigurasi ke LangChain
    os.environ["OPENAI_API_KEY"] = api_key
    os.environ["OPENAI_BASE_URL"] = base_url