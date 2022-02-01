# Lambda function to sftp the file from s3 to sftp location.
import boto3
import os
import paramiko
import logging

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

SSH_USERNAME = os.environ['SSH_USERNAME']
SSH_HOST = os.environ['SSH_HOST']
SSH_DIR = os.environ['SSH_DIR']
SSH_PORT = int(os.environ.get('SSH_PORT', 22))
SSH_PASSWORD = os.environ.get('SSH_PASSWORD')

SOURCE_S3_BUCKET = <Source S3 Bucket>
S3_KEY_PATH = <Complete S3 Key Path>
SFTP_DEST_FILE_NAME = <SFTP Destination File Name>


def connect_to_sftp(hostname, port, username, password):
    """
    Function to connect to sftp
    """
    try:
        transport = paramiko.Transport((hostname, port))
        transport.connect(
            username=username,
            password=password
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        LOG.info("Successfully connected to SFTP")
        return sftp, transport

    except Exception as err:
        LOG.error("Error connecting to SFTP")
        raise Exception('Unknown Exception - {}'.format(str(err)))


SFTP, TPT = connect_to_sftp(
    hostname=SSH_HOST,
    port=SSH_PORT,
    username=SSH_USERNAME,
    password=SSH_PASSWORD)

S3 = boto3.client('s3')


def transfer_file(s3_client, bucket, key, sftp_client, sftp_dest):
    """
    Function to download S3 file and uploads to SFTP location
    """
    with sftp_client.file(sftp_dest, 'w') as sftp_file:
        s3_client.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=sftp_file)


def lambda_handler():
    """ Lambda entry point"""

    with TPT:
        try:
            SFTP.chdir(SSH_DIR)
            transfer_file(
                s3_client=S3,
                bucket=SOURCE_S3_BUCKET,
                key=S3_KEY_PATH,
                sftp_client=SFTP,
                sftp_dest=SFTP_DEST_FILE_NAME)

            LOG.info('S3 file uploaded to SFTP successfully')
        except Exception as err:
            LOG.error("Could not upload file to SFTP")
            raise Exception('Unknown Exception - {}'.format(str(err)))
