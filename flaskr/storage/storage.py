import logging
import boto3
from botocore.exceptions import ClientError
import os
from pydub import AudioSegment
from pydub.utils import which
from models import update_processed

sso_region = os.getenv('SSO_REGION')
queue_url = os.getenv('SQS_QUEUE_URL')
sso_bucket_s3 = os.getenv('SSO_BUCKET_S3')
origin_folder = os.getenv('ORIGIN_FOLDER')

AudioSegment.converter = which("ffmpeg")


def upload_file(file_name, bucket, object_name, region):
    s3_client = boto3.client('s3', region_name=region)
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def downloading_files(file_name, bucket, object_name, region):
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.download_file(bucket, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_object_name(file_name):
    return os.path.basename(file_name)


def remove_file(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)
    else:
        return 'No se pudo remover el archivo', 500


def find_object(bucket, region, object_name):
    s3 = boto3.resource('s3', region_name=region)
    bucket = s3.Bucket(bucket)
    a = [x for x in bucket.objects.all() if x.key == object_name]
    if len(a) > 0:
        return True
    else:
        return False


def delete_object(bucket, region, object_name):
    s3 = boto3.resource('s3', region_name=region)
    try:
        s3.Object(bucket, object_name).delete()
    except Exception as e:
        logging.error(e)
        return False
    return True


def receive_and_delete_messages_queue():
    sqs = boto3.client('sqs', region_name=sso_region)
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    body = message['Body']
    title = message['MessageAttributes']['Title']['StringValue']
    author = message['MessageAttributes']['Author']['StringValue']
    origen = body.split(",")[0]
    destino = body.split(",")[1]

    try:
        if find_object(sso_bucket_s3, sso_region, "origin-{}-{}.{}".format(author, title, origen)):
            downloading_files(
                'originales/{}'.format("origin-{}-{}.{}".format(author, title, origen)),
                sso_bucket_s3,
                "origin-{}-{}.{}".format(author, title, origen),
                sso_region
            )
            archivo = AudioSegment.from_file(
                "originales/origin-{}-{}.{}".format(author, title, origen),
                origen
            )
            archivo.export(
                "originales/destino-{}-{}.{}".format(author, title, destino),
                format=destino
            )
            remove_file("originales/origin-{}-{}.{}".format(author, title, origen))
            upload_file(
                "originales/destino-{}-{}.{}".format(author, title, destino),
                sso_bucket_s3,
                "destino-{}-{}.{}".format(author, title, destino),
                sso_region
            )
            remove_file("originales/destino-{}-{}.{}".format(author, title, destino))
            update_processed(title)
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        else:
            logging.error("Archivo no encontrado en S3")
    except Exception as e:
        logging.error('error convirtiendo')
        logging.error(e)


def conversion_background(conversion):
    try:
        if find_object(sso_bucket_s3, sso_region,
                       "origin-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.origen)):
            downloading_files(
                'originales/{}'.format(
                    "origin-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.origen)),
                sso_bucket_s3,
                "origin-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.origen),
                sso_region
            )
            archivo = AudioSegment.from_file(
                "originales/origin-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.origen),
                conversion.origen
            )
            archivo.export(
                "originales/destino-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.destino),
                format=conversion.destino
            )
            remove_file("originales/origin-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.origen))
            upload_file(
                "originales/destino-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.destino),
                sso_bucket_s3,
                "destino-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.destino),
                sso_region
            )
            remove_file("originales/destino-{}-{}.{}".format(conversion.usuario_id, conversion.id, conversion.destino))
            update_processed(str(conversion.id))
        else:
            logging.error("Archivo no encontrado en S3")
    except Exception as e:
        logging.error('error convirtiendo')
        logging.error(e)
