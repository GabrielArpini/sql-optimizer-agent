import psycopg2
from transformers import AutoTokenizer, AutoModelForCausalLM
from utils.db_utils import *
import torch



def profile_query():
    """
    Create the profile of a query, the performance it have in the database.
    """


def set_model():
    tokenizer = AutoTokenizer.from_pretrained("google/gemma-3-270m")
    model = AutoModelForCausalLM.from_pretrained("google/gemma-3-270m") 
    messages = [
        {"role": "system", "content": "You are a chatbot spcialized in sql queries.",},
        {"role": "user", "content": "I want to use you to train you to optimize sql queries, how good you are with sql queries?"},
        {"role": "assistant", "content": "I know everything about sql, even though i'm small i can handle sql query optimization if you fine tune me"}

    ]
    inputs = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        tokenize=True, 
        return_tensors="pt"    
    )
    print("DECODED SEQUENCE")
    print(tokenizer.decode(inputs[0]))
    print("ENCODED SEQUENCE")
    print(inputs)
    print("MODEL OUTPUT")
    output = model.generate(inputs)
    print(output)
    print("MODEL RESPONSE")
    response = tokenizer.decode(output[0])
    print(response)

if __name__ == '__main__':
    conn = get_db_connection()
    if conn:
        print(conn)
        print("Success!")

        print("Trying to load model...")
        set_model()
    else:
        print("Not success!")
