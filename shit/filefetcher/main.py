import json
import logging
from pathlib import Path

from pymongo import MongoClient

from scanner_factory import ScannerFactory
from file_handler import FileHandler
from rabbitmq import RabbitMQ
from sqlite_db import SQLiteDBHandler


# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("file-fetcher-test")


# Load config from Mongo (TEST VERSION)
def load_config_from_mongo():
    """
    Minimal config loader from Mongo for testing.
    Assumes:
      DB: applications
      Collection: base_config
    """
    client = MongoClient("mongodb://<MONGO_IP>:27017", serverSelectionTimeoutMS=3000)
    db = client["applications"]
    cfg = db["base_config"].find_one({"name": "file_fetcher"})

    if not cfg:
        raise RuntimeError("Config not found in Mongo")

    return cfg


# MAIN
def main():
    logger.info("Starting test flow")

    # ---- load config
    config = load_config_from_mongo()

    # ---- RabbitMQ
    rabbit = Rabbit(config["connections"]["rabbitmq"]["url"])

    # ---- SQLite (state store)
    sqlite_db = SQLiteDBHandler(
        db_path=config["connections"]["sqlite"]["path"],
        logger=logger,
    )

    # ---- Scanner factory (core logic)
    scanner = ScannerFactory(
        rabbit=rabbit,
        sqlite_db=sqlite_db,
        config=config,
        logger=logger,
    )

    # Scheduler simulation (push scan message)
    logger.info("Pushing scan message to scanner_queue")

    rabbit.publish(
        "scanner_queue",
        {
            "site": "TZWorks",
            "url": "https://tzworks.com/download_links.php",
        },
    )

    # Scanner worker (consume scanner_queue)
    logger.info("Running scanner worker")

    scan_msg = rabbit.consume_one("scanner_queue")
    if scan_msg:
        scanner.execute(**scan_msg)

    # Downloader workers (consume downloader queue)
    logger.info("Running downloader workers")

    while True:
        msg = rabbit.consume_one("downloader_verify_queue")
        if not msg:
            break

        scanner.execute(**msg)

    logger.info("Flow finished")


# -------------------------------------------------
if __name__ == "__main__":
    main()



# OLD
# import json
# from pathlib import Path
#
# from db_handler import MongoDBHandler
# from rabbitmq import RabbitMQ
# from sqlitedbhandler import SQLiteDBHandler, Status
# from tzworks_downloader import TZWorksDownloader
# from logger import Logger
#
#
# DOWNLOADER_REGISTRY = {
#     "TZWorksDownloader": TZWorksDownloader,
# }
#
#
# def main() -> None:
#     logger = Logger()
#
#     # load config
#     cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
#     global_cfg = json.loads(Path())
#
#     # mongo
#     mongo = MongoDBHandler(
#         mongo_uri=cfg["mongo"]["uri"],
#         logger=logger,
#     )
#     rabbit_cfg = mongo.load_rabbitmq_config()
#
#     # rabbit
#     rabbit = RabbitMQ(
#         host=rabbit_cfg["host"],
#         port=rabbit_cfg["port"],
#         username=rabbit_cfg["username"],
#         password=rabbit_cfg["password"],
#         queue=rabbit_cfg["queue_scan_to_download"],
#         logger=logger,
#     )
#
#     # sqlite
#     sqlite_cfg = cfg["sqlite"]
#     db = SQLiteDBHandler(
#         db_path=sqlite_cfg["db_path"],
#         logger=logger,
#     )
#     max_retries = sqlite_cfg.get("max_retries", 3)
#
#     # sources
#     for src in cfg["sources"]:
#         cls_name = src["class"]
#         downloader_cls = DOWNLOADER_REGISTRY[cls_name]
#
#         downloader = downloader_cls(
#             url=src["url"],
#             logger=logger,
#             tool_filters=src.get("tool_filters"),
#             site=src["site"],
#         )
#
#         scan_msgs = downloader.execute(
#             output_dir=cfg["output_dir"]
#         )
#
#         for msg in scan_msgs:
#             name = msg["name"]
#             version = msg["version"]
#             site = msg["site"]
#
#             # SQLite
#             record = db.get_record(name, version, site)
#
#             if record:
#                 if db.is_final_status(record["status"]):
#                     logger.info(f"Skipping final: {name}@{version}")
#                     continue
#                 if not db.should_retry(
#                     record["status"],
#                     record["retry_count"],
#                     max_retries,
#                 ):
#                     logger.info(f"No retry allowed: {name}@{version}")
#                     continue
#
#             # Reserve / mark pending
#             db.upsert_status(
#                 name=name,
#                 version=version,
#                 site=site,
#                 status=Status.PENDING,
#             )
#
#             rabbit.publish(msg)
#             logger.info(f"Queued for download: {name}@{version}")
#
#     rabbit.close()
#     mongo.close()
#
#     logger.info("Scan phase completed")
#
#
# if __name__ == "__main__":
#     main()
