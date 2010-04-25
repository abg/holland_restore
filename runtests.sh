#!/bin/sh
PYTHON=$(env which python)

${PYTHON} setup.py nosetests
