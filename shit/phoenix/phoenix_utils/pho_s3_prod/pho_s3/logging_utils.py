#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import socket
import logging

from logging import handlers

__LOGS_DIR__ = "logs"


# Todo check if logger already exists
def get_logger(logger_name, file_name, log_file=True, log_stdout=True, log_syslog=True, log_level=logging.DEBUG):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    stdout_formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
                                         datefmt="%Y-%m-%d %H:%M:%S",
                                         style="%")
    normal_formatter = logging.Formatter(
        fmt="%(asctime)s %(process)d %(thread)d %(name)s %(levelname)s %(filename)s %(funcName)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        style="%")

    if log_file:
        if not os.path.isdir(__LOGS_DIR__):
            os.makedirs(__LOGS_DIR__)
        file_name = os.path.join(__LOGS_DIR__, file_name)
        file_handler = handlers.TimedRotatingFileHandler(filename=file_name,
                                                         when="midnight",
                                                         backupCount=7)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(normal_formatter)
        logger.addHandler(file_handler)

    if log_stdout:
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.setLevel(log_level)
        stdout_handler.setFormatter(stdout_formatter)
        logger.addHandler(stdout_handler)

    if log_syslog:
        try:
            syslog_handler = handlers.SysLogHandler(address=('logstash-p-01', 5000), socktype=socket.SOCK_DGRAM)
        except:
            syslog_handler = handlers.SysLogHandler(address=('logstash-t-01', 5000), socktype=socket.SOCK_DGRAM)
        syslog_handler.setLevel(log_level)
        syslog_handler.setFormatter(normal_formatter)
        logger.addHandler(syslog_handler)

    return logger