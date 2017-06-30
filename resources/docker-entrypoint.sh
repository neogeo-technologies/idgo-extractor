#!/bin/bash
DIR=/docker-entrypoint.d

if [[ -d "$DIR" ]]
then
  /bin/run-parts --verbose "$DIR"
fi

# execute CMD
exec "$@"