#!/usr/bin/python3
import logging
import os
import boto3
from botocore.exceptions import ClientError
import threading
import sys
import glob
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
import getpass
import logging

def upload_file(args):
	"""Upload a file to an S3 bucket

	:param file_name: File to upload
	:param bucket: Bucket to upload to
	:param s3_filename: S3 object name. If not specified then file_name is used
	:return: True if file was uploaded, else False
	"""
	file_name = args['file_name']
	s3_client = args['s3_client']
	
	bucket = 'bucket.bodagraj'
	S3_FOLDER_NAME = 'source_data'
	
	
	s3_filename = f"{S3_FOLDER_NAME}/{os.path.basename(file_name)}"
	# If S3 s3_filename was not specified, use file_name
	if s3_filename is None:
		s3_filename = os.path.basename(file_name)

	# Upload the file
	#s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	try:
		response = s3_client.upload_file(
			file_name, bucket, s3_filename,
			Callback=ProgressPercentage(file_name)
		)
	except ClientError as e:
		logging.error(e)
		return False
	return True

class ProgressPercentage(object):

	def __init__(self, filename):
		self._filename = filename
		self._size = float(os.path.getsize(filename))
		self._seen_so_far = 0
		self._lock = threading.Lock()

	def __call__(self, bytes_amount):
		# To simplify, assume this is hooked up to a single filename
		with self._lock:
			self._seen_so_far += bytes_amount
			percentage = (self._seen_so_far / self._size) * 100
			sys.stdout.write(
				"\r%s %5s %s / %s  (%.2f%%) " % (
					self._filename,"\t:", self._seen_so_far, self._size,
					percentage))
			sys.stdout.flush()
			if percentage == 100:
				print(" - [DONE]")
			
			
def upload_multiprocess():
	
	DATA_FILES_LOCATION   = '/tmp/migration/*.out.gz'
	# The list of files we're uploading to S3 
	filenames =  glob.glob(DATA_FILES_LOCATION) 
	upload_args = []
	
	aws_access_key_id = None
	aws_secret_access_key = None
	s3_client = None
	
	try:
		print("Enter aws_access_key_id:")
		aws_access_key_id = input()
	except Exception as error:
		logging.critical('ERROR', error)
		sys.exit(-1)
	else:
		print('aws_access_key_id entered:', aws_access_key_id)
	
	
	try:
		print("Enter aws_secret_access_key")
		aws_secret_access_key = getpass.getpass()
	except Exception as error:
		logging.critical('ERROR', error)
		sys.exit(-1)
	
		
	try:
		s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	except Exception as e:
		logging.critical("Please check S3 client credentials (aws_access_key_id & aws_secret_access_key)")
		sys.exit(-1)
	
	for file in filenames:
		info = {
			'file_name' : file,
			's3_client' : s3_client
		}
		upload_args.append(info) 
	
	#total_process = mp.cpu_count()
	total_process = 2
	print("Total parallel process="+str(total_process))
	pool = ThreadPool(processes=total_process) 
	pool.map(upload_file, upload_args) 
	
	print("All Data files uploaded to S3 Ok")
	
def main():
	upload_multiprocess()


if __name__ == '__main__':
	main()
