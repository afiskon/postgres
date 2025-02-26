#!/usr/bin/enb python3

# coverage-analyze.py
# (c) Aleksander Alekseev 2025
# https://eax.me/

import re
import sys

functions = set()

with open("src/include/catalog/pg_proc.dat") as proc_f:
	proc_data = proc_f.read()
	re_str = """proname\s+=>\s+['"](.*?)['"]"""
	for m in re.finditer(re_str, proc_data):
		func_name = m.group(1)
		functions.add(func_name)

print("Functions found: {}".format(len(functions)))
