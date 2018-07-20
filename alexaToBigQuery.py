# this script is designed to be run on a schedule every day
# designed to record information about npr's amazon alexa donations in a google big query database  
# for use by a npr member station
# Code by Jake Gluck Summer 2018


import os
from os import listdir
from os.path import isfile, join
import csv, s
import npr
from collections import namedtuple
import json, requests
from datetime import date, timedelta
import numpy as np
import mws
import pandas as pd
from google.cloud import bigquery

#set up credentials in notebook, you will need the key for the db here
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=""
#helpfull links

# mws python library used
# https://github.com/python-amazon-mws/python-amazon-mws

# amazon's guide detailing how to progratically access amazon information
# https://amazonpayments.s3.amazonaws.com/documents/Settlement_Report_Documentation_US.pdf

# some helpfull code I looked at
# https://github.com/tmoy61/amazon-mws-report-example/blob/master/pub_get_order_rpts.py

#amazon api reference
# https://pay.amazon.com/us/developer/documentation/apireference/201751630


#pull from amazon mws api a report from yesterday, if there is a report upload its contents. If not no donations took place
def getYesterdaysData():


	mydb = s.s1()
	
	# Consumer keys and access tokens, used for OAuth, get these from our amazon account
	accessKey = mydb.access_key
	secretKey = mydb.secret_key
	merchantId = 'A2AU8QYORH0MX'


	#create client for amazons mws api

	#http://docs.developer.amazonservices.com/en_US/dev_guide/index.html

	reportClient = mws.Reports(
		access_key=accessKey,
		secret_key=secretKey,
		account_id=merchantId,
	)

	#find id of the report you need
	#amazon auto generates a capture report everyday (if there is data) so we will use that

	#get report list, defaults to last 90 days
	reportList = reportClient.get_report_list(
	)

	notFound = True
	reportId = 0;

	#go through the list of reports and find the id of the capture report from yesterday
	while (notFound == True) and (reportList.parsed['HasNext']['value'] == 'true'):

		for x in reportList.parsed['ReportInfo']:
			if x['ReportType']['value'] == '_GET_FLAT_FILE_OFFAMAZONPAYMENTS_CAPTURE_DATA_':
				# make sure the report is from yesterday
				yesterday = date.today() - timedelta(1)
				if (str(yesterday) == x['AvailableDate']['value'][0:10]):
					#capture report from yesterday found, record its id and stop looking
					reportId = x['ReportId']['value'] 
					notFound = False
					
		#get next report 
		reportList = reportClient.get_report_list(
			next_token=reportList.parsed['NextToken']['value']
		)
		
	#request report and parse report into dataframe

	#if a report was found
	if (reportId != 0):
		
		#request report
		report = reportClient.get_report(
		report_id = reportId
		)

		#parse report to the dataframe rp
		streport = str(report.parsed, 'utf-8')
		text_file = open("t.txt", "w")
		text_file.write(streport)
		text_file.close()

		lnames = ["AmazonCaptureId","CaptureReferenceId","AmazonAuthorizationId","AmazonOrderReferenceId","CaptureAmount","CurrencyCode","SellerCaptureNote","CaptureStatus","LastUpdateTimestamp","ReasonCode","CreationTimestamp","BuyerName","BuyerEmailAddress","BillingAddressLine1","BillingAddressLine2","BillingAddressLine3","BillingAddressCity","BillingAddressDistrictOrCounty","BillingAddressStateOrRegion","BillingAddressPostalCode","BillingAddressCountryCode"]
		rp = pd.read_csv("t.txt", sep=',', skiprows=(1),names=lnames)

		#add suggested station based on zipcode from our endpoint
		potStation = []
		for z in rp['BillingAddressPostalCode']:
			station = npr.Stations(z[0:5])
			potStation.append(station.name)

		#add stations to dataframe
		rp['PotentialStation'] = potStation

		# https://pay.amazon.com/us/developer/documentation/apireference/201751630

		#make sure types are all correct

		rp.AmazonCaptureId = rp.AmazonCaptureId.astype(str)
		rp.CaptureReferenceId = rp.CaptureReferenceId.astype(str)
		rp.AmazonAuthorizationId = rp.AmazonAuthorizationId.astype(str)
		rp.AmazonOrderReferenceId = rp.AmazonOrderReferenceId.astype(str)
		rp.CaptureAmount = rp.CaptureAmount.astype(float)
		rp.CurrencyCode = rp.CurrencyCode.astype(str)
		rp.SellerCaptureNote = rp.SellerCaptureNote.astype(str)
		rp.CaptureStatus = rp.CaptureStatus.astype(str)
		rp.LastUpdateTimestamp = pd.to_datetime(rp['LastUpdateTimestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
		rp.ReasonCode = rp.ReasonCode.astype(str)
		rp.CreationTimestamp = pd.to_datetime(rp['CreationTimestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
		rp.SellerCaptureNote = rp.SellerCaptureNote.astype(str)
		rp.BuyerName = rp.BuyerName.astype(str)
		rp.BuyerEmailAddress = rp.BuyerEmailAddress.astype(str)
		rp.BillingAddressLine1 = rp.BillingAddressLine1.astype(str)
		rp.BillingAddressLine2 = rp.BillingAddressLine2.astype(str)
		rp.BillingAddressLine3 = rp.BillingAddressLine3.astype(str)
		rp.BillingAddressCity = rp.BillingAddressCity.astype(str)
		rp.BillingAddressDistrictOrCounty = rp.BillingAddressDistrictOrCounty.astype(str)
		rp.BillingAddressStateOrRegion = rp.BillingAddressStateOrRegion.astype(str)
		rp.BillingAddressPostalCode = rp.BillingAddressPostalCode.astype(str)
		rp.BillingAddressCountryCode = rp.BillingAddressCountryCode.astype(str)
		rp.PotentialStation = rp.PotentialStation.astype(str)

		#push update to bigquery
		projectId = "reportsserviceproduction"
		datasetId = "PodcastActions"
		tableId = "alexa_donations"

		# Instantiates a client
		bigquery_client = bigquery.client.Client(projectId)
		dataset_ref = bigquery_client.dataset(datasetId)
		table_ref = dataset_ref.table(tableId)

		#create big query job configuration
		job_config = bigquery.LoadJobConfig()
		job_config.source_format = 'Parquet'
		job_config.schema = [
								bigquery.SchemaField('AmazonAuthorizationId', 'STRING', description="Unique id created during this donations authorization event in the amazon api"),
								bigquery.SchemaField('AmazonCaptureId', 'STRING', description="Unique id created during this donations capture event in the amazon api"),
								bigquery.SchemaField('AmazonOrderReferenceId', 'STRING', description="Unique id for this order (A donation is still an order in Amazons system)"),
								bigquery.SchemaField('BillingAddressCity', 'STRING', description="City for this donations billing address"),
								bigquery.SchemaField('BillingAddressCountryCode', 'STRING', description="Country code for this donations billing address"),
								bigquery.SchemaField('BillingAddressDistrictOrCounty', 'STRING', description="District or county for this donations billing address (normally nan)"),
								bigquery.SchemaField('BillingAddressLine1', 'STRING', description="Address line 1 for this donations billing address"),
								bigquery.SchemaField('BillingAddressLine2', 'STRING', description="Address line 2 for this donations billing address (normally nan)"),
								bigquery.SchemaField('BillingAddressLine3', 'STRING', description="Address line 3 for this donations billing address (normally nan)"),
								bigquery.SchemaField('BillingAddressPostalCode', 'STRING', description="Postal code for this donations billing address"),
								bigquery.SchemaField('BuyerName', 'STRING', description="Name of the user whos account donated"),
								bigquery.SchemaField('CaptureAmount', 'FLOAT', description="Amount donated"),
								bigquery.SchemaField('CaptureReferenceId', 'STRING', description="The id for this capture transaction"),
								bigquery.SchemaField('CaptureStatus', 'STRING', description="Type of case (almost always an email)"),
								bigquery.SchemaField('CreationTimestamp', 'TIMESTAMP', description="Timestamp of when donation was made"),
								bigquery.SchemaField('CurrencyCode', 'STRING', description="Currency donation was made in"),
								bigquery.SchemaField('LastUpdateTimestamp', 'TIMESTAMP', description="Timestamp of the last time the event was updated"),
								bigquery.SchemaField('ReasonCode', 'STRING', description="Reason for donation (will probably always be nan)"),
								bigquery.SchemaField('SellerCaptureNote', 'STRING', description="Note left by seller during capture event (we do not currently have one, will probably always be nan)"),
								bigquery.SchemaField('PotentialStation', 'STRING', description="Potential station of donation generated using the npr api with the postal code")
							]

		

		#create bigquery job
		job = bigquery_client.load_table_from_dataframe(rp,table_ref,job_config=job_config)



		# Waits for job to complete
		while True:
			try:
				job.result()
			finally:
				if (job.errors != 'None'):
					print(job.errors)
				if (job.error_result != 'None'):
					print(job.error_result)
			if job.state == 'DONE':
				if job.error_result:
					raise RuntimeError(job.errors)

				print('Loaded {} rows into {}:{}.'.format(
				job.output_rows, datasetId, tableId))
				break
			time.sleep(1)
	else:
		#no capture report was found for yesterday meaning np donations were made
		print("No donations yesterday")



#run code
getYesterdaysData()