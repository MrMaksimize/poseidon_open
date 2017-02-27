#!/usr/bin/python

import sys
import subprocess
import string
import time

args = sys.argv

command = args[1]


if command == 'encrypt_local':
    c_str = "gpg --encrypt --recipient n@city.gov --recipient g@city.gov local.env"

    print "encrypting ./local.env"
    subprocess.call(c_str, shell=True)

if command == 'decrypt_local':
    c_str = "gpg local.env.gpg"

    print "decrypting ./local.env"
    subprocess.call(c_str, shell=True)
