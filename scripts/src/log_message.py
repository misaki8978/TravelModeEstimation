#! /usr/bin/env python3
import os
import sys

def log_message(message, path):
    with open(path, "a") as log_file:
        log_file.write(message + "\n")