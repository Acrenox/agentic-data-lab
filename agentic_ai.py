import pandas as pd
import json
from langchain_google_genai.chat_models import ChatGoogleGenerativeAI
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents.agent_types import AgentType
from dotenv import load_dotenv
import os
import time

load_dotenv()

#initialize gemini model
llm = ChatGoogleGenerativeAI(
    model = "gemini-2.5-flash",
    temperature = 0,
    convert_system_message_to_human = True
)

#load dataset
results_file_path = "C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/results_clean.csv"
results_df = pd.read_csv(results_file_path)

constructors_file_path = "C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/constructors_clean.csv"
constructors_df = pd.read_csv(constructors_file_path)

races_file_path = "C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/races_clean.csv"
races_df = pd.read_csv(races_file_path)

drivers_file_path = "C:/Users/Praha/OneDrive/Documents/Python/formula1_project/processed/drivers_clean.csv"
drivers_df = pd.read_csv(drivers_file_path)

results_df.rename(columns={'raceid':'raceId', 'driverid':'driverId', 'constructorid':'constructorId'}, inplace=True)
constructors_df.rename(columns={'constructorid':'constructorId'}, inplace=True)
races_df.rename(columns={'raceid':'raceId'}, inplace=True)
drivers_df.rename(columns={'driverid':'driverId'}, inplace=True)

merged_df = (
    results_df
    .merge(races_df, on="raceId", how="left")
    .merge(drivers_df, on="driverId", how="left")
    .merge(constructors_df, on="constructorId", how="left")
)

#create the agent
agent = create_pandas_dataframe_agent(
    llm=llm,
    df=results_df,
    # df={
    #     "results":results_df, 
    #     "constructors":constructors_df, 
    #     "races":races_df, 
    #     "drivers":drivers_df
    # },
    verbose=True,
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    allow_dangerous_code=True
)


def safe_invoke(agent, user_query, retries=3, delay=5):
    for i in range(retries):
        try:
            return agent.invoke(user_query)
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}")
            time.sleep(delay)
    return ("Sorry, the model is overloaded. Please try again later.")


#--make it interactive--
print("Ask me questions about the datasets (type 'exit' to quit):\n")
# print(results_df.columns)
# print(races_df.columns)
# print(drivers_df.columns)
# print(constructors_df.columns)


while True:
    user_query = input("Please enter your query: ")
    if user_query.lower() in ["exit", "quit"]:
        print("Goodbye!")
        break

    response = safe_invoke(agent, user_query)
    print("\nResponse:\n", response)


# #example query
# response = agent.run("Show me the top 5 races with the highest points")
# print(response)

