DEFAULT_PATH="../gida-landing/"
STATIC_XLSX_REL_PATH="${1:-$DEFAULT_PATH}"
BASEDIR=$(dirname $0)
ORIGDIR=${PWD}
# uvicorn main:app --port 8000 &
# pid=$!
# echo PID is $pid
# sleep 15;
cd $BASEDIR && \
cd ../../ && \
cd $STATIC_XLSX_REL_PATH && \
bash ./shell/update-amp.sh && \
bash ./shell/update-amp-summary.sh && \
npm run deploy;
# kill $pid && \
cd $ORIGDIR && \
exit 0;