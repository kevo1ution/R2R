# Seeds an r2r pipeline w/ scrapped web data
import asyncio
import os
import uuid
from gcloud.aio.storage import Storage
import tempfile
import asyncio
from supabase import create_client
from prettytable import PrettyTable

from r2r.client import R2RClient

# Initialize the client with the base URL of your API
base_url = "http://localhost:8000"
client = R2RClient(base_url)

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)
service_account_info = os.environ["SERVICE_ACCOUNT_JSON"]

with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    service_account_info = os.environ["SERVICE_ACCOUNT_JSON"]
    temp_file.write(service_account_info.encode('utf-8'))
    temp_file_path = temp_file.name

async def seed():
    data = supabase.table('yc_company_data') \
        .select('id, name, employee_count, company_pages(blob_id)') \
        .not_.is_('employee_count', 'null') \
        .order('employee_count', desc=True) \
        .limit(10) \
        .execute()
    
    headers = [key for key in data.data[0].keys() if key != 'company_pages']
    table = PrettyTable(headers)
    table.align = "l"
    for row in data.data:
        table.add_row([value for key, value in row.items() if key != 'company_pages'])
    print(table)
    async with Storage(service_file=temp_file_path) as storage_client:
        blob_ids = [company_page.get('blob_id')
                    for company in data.data
                    for company_page in company.get('company_pages', [])
                    if company_page.get('blob_id')]
        blobs = await asyncio.gather(*(
            storage_client.download(bucket="scraped_webpages", object_name=blob_id)
            for blob_id in blob_ids if blob_id))

        for blob_id, blob in zip(blob_ids, blobs):
            if blob:
                print(f"Processing blob with ID: {blob_id}...")
                # Upload and process the blob data
                metadata = {"company_id": "temp"}
                upload_pdf_response = client.upload_and_process_file(
                    blob_id, blob, metadata, None
                )
                print(upload_pdf_response)

asyncio.run(seed())
