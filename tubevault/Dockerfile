FROM public.ecr.aws/lambda/python:3.8

COPY /tubevault/src .

ENV SECRET_NAME prod/hanni
ENV REGION_NAME us-east-1
ENV KEY_BUCKET_NAME yta-bucket-name
ENV KEY_PROJECT_FOLDER_NAME yta-folder-name
ENV KEY_YTA_CREDS_PATH yta-creds-path
ENV KEY_FERNET yta-fernet-encryption-key


RUN pip3 install -r requirements.txt

CMD [ "app.handler" ]