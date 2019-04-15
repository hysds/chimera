#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export SMAP_HOME=$(dirname "${BASE_PATH}")
export COMMONS_HOME=$SMAP_HOME/smap_sciflo/commons
export SMAP_SCIFLO_HOME= $SMAP_HOME/smap_sciflo
export PYTHONPATH=$BASE_PATH:$SMAP_HOME:$PYTHONPATH:$SMAP_SCIFLO_HOME
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")

# source environment
source $HOME/verdi/bin/activate

WF_NAME="$1"

echo "##########################################" 1>&2
echo -n "Running $PGE run_sciflo.py with params $BASE_PATH/$WF_NAME.sf.xml and _context.json: " 1>&2
date 1>&2
python $BASE_PATH/run_sciflo.py $BASE_PATH/wf_xml/$WF_NAME.sf.xml _context.json output > run_sciflo_$WF_NAME.log 2>&1
STATUS=$?
echo -n "Finished running $PGE run_sciflo.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run $PGE run_sciflo.py" 1>&2
  cat run_sciflo_$WF_NAME.log 1>&2
  echo "{}"
  exit $STATUS
fi
