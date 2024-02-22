import uuid

import dotenv

from r2r.codesearch import Indexer
from r2r.client import SciPhiR2RClient

from r2r.main import load_config
from r2r.llms import OpenAIConfig, OpenAILLM
from r2r.core import GenerationConfig

# Initialize the client with the base URL of your API
base_url = "http://localhost:8000"  # Change this to your actual API base URL
client = SciPhiR2RClient(base_url)
dotenv.load_dotenv()

DESCRIPTION_PROMPT = "Summarize the following code snippet in two to three sentences: \n\n{extraction}"

if __name__ == "__main__":
    (
        api_config,
        logging_config,
        embedding_config,
        database_config,
        language_model_config,
        text_splitter_config,
    ) = load_config()


    llm = OpenAILLM(OpenAIConfig())
    generation_config = GenerationConfig(
        model_name=language_model_config["model_name"],
        temperature=language_model_config["temperature"],
        top_p=language_model_config["top_p"],
        top_k=language_model_config["top_k"],
        max_tokens_to_sample=language_model_config["max_tokens_to_sample"],
        do_stream=language_model_config["do_stream"],
    )


    # for i, (symbol, extraction) in enumerate(Indexer().extractor()):
    #     document_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, symbol))
    #     symbol_extraction = f"Symbol: {symbol}\nExtraction:\n\n{extraction}"
    #     summary = llm.get_chat_completion(
    #         [
    #             {"role": "system", "content": "You are a helpful assistant."},
    #             {"role": "user", "content": DESCRIPTION_PROMPT.format(extraction=symbol_extraction)},
             
    #          ],
    #         generation_config
    #     )
    #     description = summary.choices[0].message.content

    #     entry_response = client.upsert_entries(
    #         [
    #             {
    #                 "document_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, symbol+"-desc-only")),
    #                 "blobs": {"txt": description},
    #                 "metadata": {"symbol": symbol, 'type': 'desc-only'},
    #             },
    #             {
    #                 "document_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, symbol+"-ext-only")),
    #                 "blobs": {"txt": extraction},
    #                 "metadata": {"symbol": symbol, 'type': 'ext-only'},
    #             },
    #             {
    #                 "document_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, symbol+"-symbol-ext-only")),
    #                 "blobs": {"txt": symbol_extraction},
    #                 "metadata": {"symbol": symbol, 'type': 'symbol-ext-only'},
    #             },
    #             {
    #                 "document_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, symbol+"-desc-plus-symbol-ext")),
    #                 "blobs": {"txt": f"Description:\n{description}\n{symbol_extraction}"},
    #                 "metadata": {"symbol": symbol, 'type': 'desc-plus-symbol-ext'},
    #             }
    #         ],
    #         {"embedding_settings": {"do_chunking": "false"}}
    #     )

    # query = "How is logging performed?"
    # query = "How do we launch a server?"
    # query = "How do we search?"
    # query = "How do we embed a document?"
    # query = "How do we ingest a file?"
    # query = "How do we create an LLM?"
    # query = "How do we connect to a vector database?"
    query = "What are the core pipelines?"

    filters = {"type": "desc-only"}
    print("-"*25)
    print(f"Searching remote db with query = {query} and filters={filters}...")
    search_response = client.search(query, 10, filters=filters)
    for i, response in enumerate(search_response):
        metadata = response['metadata']
        print(f"{i+1}. {metadata['symbol']}: {response['score']:.3f}")

    filters = {"type": "ext-only"}
    print("-"*25)
    print(f"Searching remote db with query = {query} and filters={filters}...")
    search_response = client.search(query, 10, filters=filters)
    for i, response in enumerate(search_response):
        metadata = response['metadata']
        print(f"{i+1}. {metadata['symbol']}: {response['score']:.3f}")

    filters = {"type": "symbol-ext-only"}
    print("-"*25)
    print(f"Searching remote db with query = {query} and filters={filters}...")
    search_response = client.search(query, 10, filters=filters)
    for i, response in enumerate(search_response):
        metadata = response['metadata']
        print(f"{i+1}. {metadata['symbol']}: {response['score']:.3f}")

    filters = {"type": "desc-plus-symbol-ext"}
    print("-"*25)
    print(f"Searching remote db with query = {query} and filters={filters}...")
    search_response = client.search(query, 10, filters=filters)
    for i, response in enumerate(search_response):
        metadata = response['metadata']
        print(f"{i+1}. {metadata['symbol']}: {response['score']:.3f}")

    print("-"*25)

    # print(f"Searching remote db with query = {query} and filters={filters}...")
    # search_response = client.search(query, 2, filters=filters)
    # print(f"Leading 2 responses:\n{search_response}\n\n")    