#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_state_bug import SourceStateBug

if __name__ == "__main__":
    source = SourceStateBug()
    launch(source, sys.argv[1:])
