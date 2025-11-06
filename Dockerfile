FROM public.ecr.aws/docker/library/python:3.11

RUN useradd -ms /bin/bash appuser
WORKDIR /home/appuser

# RUN apt-get update && apt-get install libsm6 libxext6  -y && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

USER appuser
EXPOSE 8000
COPY --chown=appuser:appuser . .

CMD ["uvicorn", "app.main:app", "--port", "8000", "--host", "0.0.0.0", "--no-access-log", "--timeout-keep-alive", "10000", "--timeout-graceful-shutdown", "30", "--limit-concurrency", "100", "--limit-max-requests", "1000"]