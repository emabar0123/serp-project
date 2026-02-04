import os
import pathlib
import json
from abc import ABC, abstractmethod
from phoenix.microservice_interface import Microservice
from pho_elk.elk_handler import ElasticSearchHandler
from pho_json.json_handler import JsonHandler
from pho_csv.csv_handler import CsvHandler


class ThreatIntelIndexer(Microservice, ABC):

    def __init__(self, logger, config, global_config, io_handler):
        super().__init__(logger, config, global_config, io_handler)
        self.ti_config = self.global_config.get("ThreatIntel")
        self.max_json_size = self.ti_config.get("max_json_size")
        self.max_files_for_bulk = self.ti_config.get("max_files_for_bulk")
        self.chunk_size = self.ti_config.get("chunk_size")
        self.delete_duplicate_documents = self.config.get("delete_duplicate_documents")
        self.provide_id = self.config.get("provide_id")
        self.filter_duplicate_records = self.config.get("filter_duplicate_json_records")
        self.filter_key = self.config.get("filter_key")
        self.index_name = self.config.get("index_name")
        self.ioc_type = self.config.get("ioc_type")
        self.chunk_size = self.config.get("chunk_size")
        self.update = self.config.get("elastic_update")
        self.filter_duplicate_records = self.config.get(
            "filter_duplicate_json_records")
        self.filter_key = self.config.get("filter_key")
        self.ioc_type = self.config.get("ioc_type")
        self.record_id = self.config.get("record_id_key")
        self.delete_duplicate_documents = self.config.get("delete_duplicate_documents")
        self.provide_id = self.config.get("provide_id")
        self.bulk = []
        self.file_path = None
        self.elastic_config = self.global_config["ElasticSearch"]
        self.elk_handler = ElasticSearchHandler(self.logger, self.elastic_config)

    def execute(self, **input_message):
        input_data = json.loads(input_message.get('data', {}))
        self.file_path = input_data.get('file_path')
        if pathlib.Path(self.file_path).suffix == ".json":
            records_to_index = JsonHandler(self.logger, self.chunk_size, self.max_json_size).get_records_to_index(
                self.file_path,
                self.filter_duplicate_records,
                self.filter_key)
            self.load_bulk(records_to_index)
        elif pathlib.Path(self.file_path).suffix == ".csv":
            records_to_index = CsvHandler(self.logger).csv_to_list_of_dicts(self.file_path)
            self.load_bulk(records_to_index)
        else:
            self.logger.info("The file type is unsupported.")
            pass

        return None

    def load_bulk(self, records_to_index):
        """
        Load bulk of records from JSON file.
        :param records_to_index: list, records to be submitted into "Elasticsearch".
        update only the first document and delete the others with the same "id" from "Elasticsearch".
        """
        for record in records_to_index:
            ioc_id = record[self.record_id]
            # Search if there are documents with the same ioc value, that already indexed in "Elasticsearch"
            indexed_documents = self.elk_handler.search_document_by_ioc(self.index_name, self.ioc_type, ioc_id)
            try:
                # If there are no such documents, append the current document record to bulk list
                if indexed_documents is None:
                    self.bulk.append(record)
                # If the are indexed documents and the 'update' flag is set to True. The documents will be updated.
                elif self.update:
                    if self.delete_duplicate_documents and indexed_documents.__len__() > 1:
                        self.update_documents_without_duplicates(indexed_documents, ioc_id, record)
                    else:
                        self.update_documents(indexed_documents, ioc_id, record)
                    continue
                else:
                    self.bulk.append(record)
            except Exception as e:
                self.logger.error("Failed to index the record due to error: ", e)
                self.handle_load_failure(indexed_documents)
            # Check if you've reached the bulk size limit or if it's the last item in JSON file
            if len(self.bulk) >= self.max_files_for_bulk or record == records_to_index[-1]:
                is_inserted = self.elk_handler.index_bulk(self.index_name, self.bulk, self.record_id, self.provide_id)
                if is_inserted:
                    self.logger.info(f"{len(self.bulk)} documents were indexed to {self.index_name}")
                self.bulk = []
        self.logger.info(f"Handling process for {records_to_index} finished successfully")

    def update_documents(self, indexed_documents, ioc_id, document):
        for doc_id in indexed_documents:
            doc_index_name = indexed_documents[doc_id]
            # doc_id value represents an '_id' that uniquely identifies each document in 'Elasticsearch'.
            if doc_id == ioc_id:
                self.elk_handler.update_document(doc_index_name, ioc_id, document)
            else:
                self.elk_handler.reindex_document(doc_index_name, ioc_id, doc_id, document)

    def update_documents_without_duplicates(self, indexed_documents, ioc_id, document):
        # Get the first value from the indexed_documents dictionary
        first_doc_id, first_doc_index_name = next(iter(indexed_documents.items()))
        if first_doc_id:
            first_document = {first_doc_id: first_doc_index_name}
            # Update the document with the first item
            self.update_documents(first_document, ioc_id, document)
            # Delete the remaining documents
            remaining_docs = list(indexed_documents.keys())[1:]
            for doc_id in remaining_docs:
                doc_index_name = indexed_documents[doc_id]
                self.elk_handler.delete_document(doc_index_name, doc_id)

    def handle_load_failure(self, failed_record):
        pass

    @abstractmethod
    def parse(self):
        pass

    def after_success(self):
        os.remove(self.file_path)
        pass

    def stop(self):
        return True
