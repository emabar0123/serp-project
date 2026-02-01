from tzworks_downloader import TZWorksDownloader
from file_handler import FileHandler


class ScannerFactory:
    DOWNLOADERS = {
        "TZWorks": TZWorksDownloader
    }

    def __init__(self, *, rabbit, sqlite_db, config, logger):
        self.rabbit = rabbit
        self.db = sqlite_db
        self.config = config
        self.logger = logger

    def execute(self, **input_msg):
        # SCAN MESSAGE
        if "url" in input_msg:
            self._scan(**input_msg)
            return

        # DOWNLOAD MESSAGE
        if "download_url" in input_msg:
            self._download(**input_msg)
            return

        raise ValueError(f"Unknown message: {input_msg}")

    def _scan(self, *, site, url):
        cls = self.DOWNLOADERS[site]
        scanner = cls(url=url, site=site, logger=self.logger)
        files = scanner.list_files()

        for f in files:
            if self.db.file_exists(f.name, f.version, f.site):
                continue

            self.rabbit.publish(
                "downloader_verify_queue",
                {
                    "site": f.site,
                    "name": f.name,
                    "version": f.version,
                    "download_url": f.download_url,
                },
            )

    def _download(self, *, site, name, version, download_url):
        if self.db.file_exists(name, version, site):
            return

        self.db.insert(name, version, site)

        fh = FileHandler(
            chunk_size=self.config["chunk_size"],
            staging_dir=self.config["staging_dir"],
            logger=self.logger,
        )

        staged = fh.download_file(download_url)
        fh.move_file_to_output_dir(staged, self.config["output_dir"])

        self.db.mark_sent(name, version, site)



#OLD
# from typing import Dict, Type
#
# from base_downloader import BaseDownloader
# from tzworks_downloader import TZWorksDownloader
# from file_handler import FileHandler
#
#
# class ScannerFactory:
#     """
#     ScannerFactory is the orchestration layer of the system.
#
#     - It exposes a single execute(**input_msg) entry point
#     - It uses the Factory pattern to select site-specific scanners
#     - It does NOT consume queues itself
#     - It does NOT know how workers are run
#     """
#
#     # FACTORY REGISTRY
#     DOWNLOADERS: Dict[str, Type[BaseDownloader]] = {
#         "TZWorks": TZWorksDownloader,
#         # future: "EricZimmerman": EricZimmermanDownloader
#     }
#
#     def __init__(self, *, rabbit, sqlite_db, config, logger):
#         """
#         :param rabbit: RabbitMQ client
#         :param sqlite_db: SQLite state store
#         :param config: Global config dict
#         :param logger: Logger instance
#         """
#         self.rabbit = rabbit
#         self.db = sqlite_db
#         self.config = config
#         self.logger = logger
#
#     # SINGLE ENTRY POINT
#     def execute(self, **input_msg):
#         """
#         Single execution entry point.
#
#         input_msg comes directly from the consumed queue message.
#         Message type is inferred from its structure.
#         """
#
#         # Scanner queue message
#         if "url" in input_msg:
#             self._handle_scan(**input_msg)
#             return
#
#         # Downloader queue message
#         if "download_url" in input_msg:
#             self._handle_download(**input_msg)
#             return
#
#         raise ValueError(f"Unknown message format: {input_msg}")
#
#     # SCANNER STAGE
#     def _handle_scan(self, *, site: str, url: str):
#         """
#         Handle messages from scanner_queue:
#         - Instantiate the correct site scanner (Factory)
#         - Discover files
#         - Push new files to downloader_verify_queue
#         """
#
#         downloader_cls = self.DOWNLOADERS.get(site)
#         if not downloader_cls:
#             raise ValueError(f"Unsupported site: {site}")
#
#         self.logger.info(f"Scanning site: {site}")
#
#         scanner = downloader_cls(
#             url=url,
#             site=site,
#             logger=self.logger,
#         )
#
#         files = scanner.list_files()
#
#         for f in files:
#             # Skip already-known files
#             if self.db.file_exists(f.name, f.version, f.site):
#                 continue
#
#             self.rabbit.publish(
#                 queue=self.config["queues"]["downloader_verify"],
#                 message={
#                     "site": f.site,
#                     "name": f.name,
#                     "version": f.version,
#                     "download_url": f.download_url,
#                 },
#             )
#
#     # DOWNLOADER + VERIFY STAGE
#     def _handle_download(
#         self,
#         *,
#         site: str,
#         name: str,
#         version: str | None,
#         download_url: str,
#     ):
#         """
#         Handle messages from downloader_verify_queue:
#         - Download file
#         - (Optional) verify hashes
#         - Move to output directory
#         - Update SQLite state
#         """
#
#         # Double-check state (idempotency)
#         if self.db.file_exists(name, version, site):
#             return
#
#         self.logger.info(f"Downloading {name} ({version}) from {site}")
#
#         self.db.upsert_status(
#             name=name,
#             version=version,
#             site=site,
#             status="DOWNLOADING",
#         )
#
#         file_handler = FileHandler(
#             chunk_size=self.config["chunk_size"],
#             staging_dir=self.config["staging_dir"],
#             logger=self.logger,
#         )
#
#         staged_file = file_handler.download_file(download_url)
#
#         # Hash verification hook (optional / site-specific)
#         # if expected_hashes:
#         #     calculated = file_handler.calculate_hashes(...)
#         #     if not file_handler.validate_hashes(...):
#         #         raise ValueError("Hash mismatch")
#
#         file_handler.move_file_to_output_dir(
#             staged_file,
#             self.config["output_dir"],
#         )
#
#         self.db.upsert_status(
#             name=name,
#             version=version,
#             site=site,
#             status="SENT_TO_OUTPUT",
#         )


# OLD

# from pathlib import Path
# from typing import Dict, Type
#
# from microservice import Microservice
# from base_downloader import BaseDownloader
# from tzworks_downloader import TZWorksDownloader
# from file_handler import FileHandler
#
#
# class Scanner(Microservice):
#     """
#     Single entry point microservice.
#     execute(**input_msg) is called by workers consuming different queues.
#     """
#
#     DOWNLOADERS: Dict[str, Type[BaseDownloader]] = {
#         "TZWorks": TZWorksDownloader,
#         # future: "EricZimmerman": EricZimmermanDownloader
#     }
#
#     def execute(self, **input_msg):
#         """
#         Called with a message from either:
#         - scanner_queue
#         - downloader_verify_queue
#         """
#
#         # SCANNER QUEUE MESSAGE
#         if "url" in input_msg:
#             self._handle_scan(**input_msg)
#             return
#
#         # DOWNLOADER QUEUE MESSAGE
#         if "download_url" in input_msg:
#             self._handle_download(**input_msg)
#             return
#
#         raise ValueError(f"Unknown message format: {input_msg}")
#
#
#     def _handle_scan(self, *, site: str, url: str):
#         downloader_cls = self.DOWNLOADERS.get(site)
#         if not downloader_cls:
#             raise ValueError(f"Unsupported site: {site}")
#
#         scanner = downloader_cls(url=url, site=site, logger=self.logger)
#         files = scanner.list_files()
#
#         for f in files:
#             if self.state_store.file_exists(f.name, f.version, f.site):
#                 continue
#
#             self.rabbit.publish(
#                 queue=self.config["queues"]["downloader_verify"],
#                 message={
#                     "site": f.site,
#                     "name": f.name,
#                     "version": f.version,
#                     "download_url": f.download_url,
#                 },
#             )
#
#
#     def _handle_download(self, *, site: str, name: str, version: str | None, download_url: str):
#         if self.state_store.file_exists(name, version, site):
#             return
#
#         self.state_store.upsert_status(
#             name=name,
#             version=version,
#             site=site,
#             status="DOWNLOADING",
#         )
#
#         file_handler = FileHandler(
#             chunk_size=self.config["chunk_size"],
#             staging_dir=Path(self.config["staging_dir"]),
#             logger=self.logger,
#         )
#
#         staged_file = file_handler.download_file(download_url)
#
#         file_handler.move_file_to_output_dir(
#             staged_file,
#             Path(self.config["output_dir"]),
#         )
#
#         self.state_store.upsert_status(
#             name=name,
#             version=version,
#             site=site,
#             status="SENT_TO_OUTPUT",
#         )
