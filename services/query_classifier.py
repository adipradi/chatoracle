from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

def classify_query_type(question: str) -> str:
    try:
        prompt = ChatPromptTemplate.from_messages([
            ("system", """
Classify the user question into one of these categories:
- data_query: Questions about specific data, metrics, or analysis
- dashboard_info: Questions about existing dashboards
- card_info: Questions about existing cards/questions
- recommendation: Business recommendations based on data
- general: General questions not related to data analysis

Respond ONLY with the category label."""),
            ("human", "{question}")
        ])
        llm = ChatOpenAI(model="deepseek/deepseek-chat:free", temperature=0.2)
        chain = prompt | llm | StrOutputParser()
        return chain.invoke({"question": question}).strip().lower()
    except Exception:
        return "data_query"