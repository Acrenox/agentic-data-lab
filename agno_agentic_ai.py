import pandas as pd
from langchain_google_genai.chat_models import ChatGoogleGenerativeAI
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents.agent_types import AgentType
from IPython.display import display, Markdown

def create_interactive_agent(llm_model="gemini-2.5-flash", verbose=True):
    # Load datasets
    results_df = pd.read_csv("C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/results_clean.csv")
    constructors_df = pd.read_csv("C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/constructors_clean.csv")
    races_df = pd.read_csv("C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/races_clean.csv")
    drivers_df = pd.read_csv("C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/drivers_clean.csv")

    # Standardize column names
    rename_mapping = {
        "raceid": "raceId",
        "driverid": "driverId",
        "constructorid": "constructorId"
    }
    for df in [results_df, races_df, drivers_df, constructors_df]:
        df.rename(columns={k: v for k, v in rename_mapping.items() if k in df.columns}, inplace=True)

    # Merge all DataFrames
    merged_df = results_df.merge(races_df, on="raceId", how="left") \
                          .merge(drivers_df, on="driverId", how="left") \
                          .merge(constructors_df, on="constructorId", how="left")

    # Initialize LLM
    llm = ChatGoogleGenerativeAI(
        model=llm_model,
        temperature=0,
        convert_system_message_to_human=True
    )

    # Create agent
    agent = create_pandas_dataframe_agent(
        llm=llm,
        df=merged_df,
        verbose=verbose,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        allow_dangerous_code=True
    )

    return agent


# -------------------------
# Interactive loop
# -------------------------
agent = create_interactive_agent()

print("Ask me questions about the datasets (type 'exit' to quit):\n")
while True:
    query = input("Please enter your query: ")
    if query.lower() in ["exit","quit", "bye"]:
        print("Goodbye!")
        break
    try:
        response = agent.invoke(query)
        display(Markdown(f"### Query:\n{query}\n\n### Answer:\n{response['output']}"))
    except Exception as e:
        print("Error:", e)
