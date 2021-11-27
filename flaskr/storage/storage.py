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

def create_bucket(bucket_name, region):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3',
                                     aws_access_key_id='AKIAVI7PUQMWA7CHFW7Q',
                                     aws_secret_access_key='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')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3',
                                     region_name=region,
                                     aws_access_key_id='AKIAVI7PUQMWA7CHFW7Q',
                                     aws_secret_access_key='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')
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def list_buckets():
    # Retrieve the list of existing buckets
    s3 = boto3.client('s3')
    response = s3.list_buckets()

    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')


def upload_file(file_name, bucket, object_name, region):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # Upload the file
    s3_client = boto3.client('s3',
                             region_name=region,
                             aws_access_key_id='AKIAVI7PUQMWA7CHFW7Q',
                             aws_secret_access_key='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')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print('upload_file is good')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def downloading_files(file_name, bucket, object_name, region):
    s3 = boto3.client('s3',
                      region_name=region,
                      aws_access_key_id='AKIAVI7PUQMWA7CHFW7Q',
                      aws_secret_access_key='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')
    try:
        s3.download_file(bucket, object_name, file_name)
        print('downloading_files is good')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_object_name(file_name):
    return os.path.basename(file_name)


def remove_file(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)
        print('Archivo removido con exito ', file_name)
    else:
        print('No se pudo remover el archivo')
        return 'No se pudo remover el archivo', 500


def find_object(bucket, region, object_name):
    s3 = boto3.resource('s3',
                        region_name=region,
                        aws_access_key_id='AKIAVI7PUQMWA7CHFW7Q',
                        aws_secret_access_key='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')
    bucket = s3.Bucket(bucket)
    print("in find_object i need to find......... ", object_name)
    a = [x for x in bucket.objects.all() if x.key == object_name]
    if len(a) > 0:
        print("in find_object..... True")
        return True
    else:
        print("in find_object..... False")
        return False


def delete_object(bucket, region, object_name):
    s3 = boto3.resource('s3',
                        region_name=region,
                        aws_access_key_id='AKIAVI7PUQMWA7CHFW7Q',
                        aws_secret_access_key='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')
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
    print("body......... ", body)

    title = message['MessageAttributes']['Title']['StringValue']
    print("title......... ", title)

    author = message['MessageAttributes']['Author']['StringValue']
    print("author......... ", author)

    try:
        if find_object(sso_bucket_s3, sso_region,
                       "origin-{}-{}.{}".format(author, title, body.split(",")[0])):
            downloading_files(
                origin_folder+'{}'.format("origin-{}-{}.{}".format(author, title, body.split(",")[0])),
                sso_bucket_s3,
                "origin-{}-{}.{}".format(author, title, body.split(",")[1]),
                sso_region
            )
            archivo = AudioSegment.from_file(
                origin_folder+"origin-{}-{}.{}".format(author, title, body.split(",")[0]),
                body.split(",")[0])
            archivo.export(
                origin_folder+"destino-{}-{}.{}".format(author, title, body.split(",")[1]),
                format=body.split(",")[1])
            print('convertido satisfactoriamente',
                  "destino-{}-{}.{}".format(author, title, body.split(",")[1]))
            upload_file(origin_folder+"destino-{}-{}.{}".format(author, title, body.split(",")[1]),
                        sso_bucket_s3,
                        "destino-{}-{}.{}".format(author, title, body.split(",")[1]),
                        sso_region)
            remove_file(origin_folder+"destino-{}-{}.{}".format(author, title, body.split(",")[1]))
            update_processed(title)
        else:
            print("Archivo no encontrado en S3")
    except Exception as err:
        print('error convirtiendo')
        print(err)
        print(err.args)

    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
