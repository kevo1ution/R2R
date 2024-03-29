from r2r.main import E2EPipelineFactory, R2RConfig
from r2r.pipelines import BasicIngestionPipeline
from unstructured.partition.html import partition_html
from unstructured.chunking.title import chunk_by_title

class UnstructuredIngestionPipeline(BasicIngestionPipeline):
  def _parse_html(self, data: str) -> str:
    elements = partition_html(text=data)
    chunks = chunk_by_title(elements)
    return '\n'.join([str(chunk) for chunk in chunks]);


# Creates a pipeline with default configuration
# This is the main entry point for the application
# The pipeline is built using the `config.json` file
app = E2EPipelineFactory.create_pipeline(config=R2RConfig.load_config(), ingestion_pipeline_impl=UnstructuredIngestionPipeline)
