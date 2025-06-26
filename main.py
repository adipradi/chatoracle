import os
from dotenv import load_dotenv

def setup_environment():
    """Setup environment variables for OpenRouter/OpenAI compatibility"""
    api_key = os.getenv("OPENROUTER_API_KEY")
    base_url = os.getenv("OPENROUTER_BASE_URL")

    if not api_key or not base_url:
        raise ValueError("❌ OPENROUTER_API_KEY dan OPENROUTER_BASE_URL belum diset!")

    # Konfigurasi ke LangChain
    os.environ["OPENAI_API_KEY"] = api_key
    os.environ["OPENAI_BASE_URL"] = base_url
    
    return api_key, base_url

# Load environment first
load_dotenv()

# Setup environment
api_key, base_url = setup_environment()

# Import UI components (setelah environment setup)
from ui.components import render_page_header, initialize_session_state
from ui.sidebar import render_sidebar, render_additional_features, render_info_panel, render_status_info
from ui.chat_interface import display_chat_history, handle_chat_input

# Initialize UI
render_page_header()
initialize_session_state()

# Render sidebar components
render_sidebar()
render_additional_features()
render_info_panel()
render_status_info()

# Main chat interface
display_chat_history()
handle_chat_input()