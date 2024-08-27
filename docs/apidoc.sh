#!/bin/bash

OUTPUT_PATH="source/api"
MODULE_PATH="../pygcp"

#sphinx-apidoc --separate -d 3 -f -o source/api ../pygcp  

sphinx-apidoc \
  -d 3 \
  --force \
  --separate \
  --module-first \
  -o ${OUTPUT_PATH} \
  ${MODULE_PATH}

echo "Sphinx api-doc files written to: ${OUTPUT_PATH}"