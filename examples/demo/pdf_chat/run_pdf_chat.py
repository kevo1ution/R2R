import glob
import uuid

import fire

from r2r.client import R2RClient


class PDFChat:
    def __init__(self, base_url="http://localhost:8000", user_id=None):
        self.client = R2RClient(base_url)
        if not user_id:
            self.user_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, "user_id"))
        self.titles = {
            "examples/demo/pdf_chat/meditations.pdf": "Title: Meditations - Marcus Aurelius",
            "examples/demo/pdf_chat/the_republic.pdf": "Title: The Republic - Plato",
        }

    def ingest(self):
        for file_path in glob.glob("examples/demo/pdf_chat/*.pdf"):
            print("Uploading file: ", file_path)
            document_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, file_path))
            metadata = {
                "user_id": self.user_id,
                "chunk_prefix": self.titles[file_path],
            }
            settings = {}
            upload_response = self.client.upload_and_process_file(
                document_id, file_path, metadata, settings
            )
            print("Upload response = ", upload_response)

    def search(self, query):
        search_response = self.client.search(
            query,
            5,
            filters={"user_id": self.user_id},
        )

        print("search_response = ", search_response)
        for i, response in enumerate(search_response):
            text = response["metadata"]["text"]
            title, body = text.split("\n", 1)
            print(f"Result {i + 1}: {title}")
            print(body[:500])
            print("\n")

    def rag_completion(self, query):
        rag_response = self.client.rag_completion(
            query,
            5,
            filters={"user_id": self.user_id},
        )
        print("rag_response = ", rag_response)


if __name__ == "__main__":
    fire.Fire(PDFChat)
