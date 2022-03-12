web: gunicorn -w 4 -k uvicorn.workers.UvicornWorker --bind '127.0.0.1:8000' --timeout 90 --log-level debug main:app
