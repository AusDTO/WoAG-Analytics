#!/usr/bin/python
'''
GA-BQ Input Script
'''

import sys,os,time

SPLUNK_HOME='/opt/splunk'
#dynamically load in any eggs in /etc/apps/GoogleAnalyticsBQ/bin
APP_HOME = SPLUNK_HOME + "/etc/apps/GoogleAnalyticsBQ/"
EGG_DIR = APP_HOME + "bin/"

for filename in os.listdir(EGG_DIR):
    if filename.endswith(".egg"):
        sys.path.append(EGG_DIR + filename) 


import requests, json, time, calendar, urllib
#import sqlite
from requests_oauthlib import OAuth2Session, TokenUpdated
from oauthlib.oauth2 import WebApplicationClient 
from splunklib.client import connect
from splunklib.client import Service
from splunklib.results import *
from splunklib.modularinput import *

class GABQInput(Script):
	# OAuth endpoints given in the Google API documentation
	_google_oauth2_authorization_url = "https://accounts.google.com/o/oauth2/auth"
	_google_oauth2_token_url = "https://accounts.google.com/o/oauth2/token"
	_google_bq_base_url = "https://www.googleapis.com/bigquery/v2"
	_google_bq_ro_scope = [ "https://www.googleapis.com/auth/bigquery.readonly" ]
	_tokenUpdated = False

	def get_scheme(self):
		scheme = Scheme("Google Analytics - BigQuery")
		scheme.description = "Input method for Google Analytics data using Google's BQ REST API"
		scheme.use_external_validation = True
		scheme.use_single_instance = False
		scheme.add_argument(Argument(name="bigquery_project", description="BigQuery Project - GA data in this context", required_on_create=True))
		scheme.add_argument(Argument(name="bigquery_query_project", description="BigQuery Query Project - queries run in this context", required_on_create=True))
		scheme.add_argument(Argument(name="bigquery_dataset", description="BigQuery Dataset(s); separate by comma for multiple sets, or * for all datasets in a project", required_on_create=True))
		scheme.add_argument(Argument(name="oauth2_access_token", description="OAuth2 Access Token", required_on_create=True))
		scheme.add_argument(Argument(name="oauth2_refresh_token", description="OAuth2 Refresh Token", required_on_create=True))
		scheme.add_argument(Argument(name="oauth2_client_id", description="OAuth2 Client ID", required_on_create=True))
		scheme.add_argument(Argument(name="oauth2_client_secret", description="OAuth2 Client Secret", required_on_create=True))
		scheme.add_argument(Argument(name="https_proxy", description="HTTPS proxy (blank if none)"))
		scheme.add_argument(Argument(name="index_errors", description="Index any errors : true | false"))
		scheme.add_argument(Argument(name="backoff_time", description="How long to wait after an error (seconds)"))
		return scheme

	def validate_input(self, inputs):
		pass 

	def stream_events(self, inputs, ew):
		def extractFields(schemaDict, dataDict):
			def unpackSchema(schemaDict):
				fields = []
				for f in schemaDict:
					if f['type'] == 'RECORD':
						fields.append([f['name'], unpackSchema(f['fields'])])
					else:
						fields.append([f['name'], None])
				return fields

			def unpackData(schemaDict, dataDict):
				rowdata = {}
				i = 0
				for val in dataDict:
					if schemaDict[i][1] is None:
						rowdata[schemaDict[i][0]] = val['v']
					else:
						if type(val['v']) is dict:
							for key in val['v']:
								rowdata[schemaDict[i][0]] = unpackData(schemaDict[i][1], val['v'][key])
						elif type(val['v']) is list:
							rowdata[schemaDict[i][0]] = []
							for row in val['v']:
								rowdata[schemaDict[i][0]].append(unpackData(schemaDict[i][1], row['v']['f']))
					i += 1
				return rowdata

			results = []
			fields = unpackSchema(schemaDict['fields'])
			for f in dataDict['rows']:
				results.append(unpackData(fields, f['f']))
			return results

		def buildStruct(dataRow, fieldList):
			returnVal = {}
			for field in fieldList:
				if '.' in field:
					record, recfield = field.split(".", 1)
					returnVal[field] = buildStruct(dataRow[record], [ recfield ] )
				else:
					if type(dataRow) is list:
						if len(dataRow) == 1:
							returnVal[field] = dataRow[0][field]
						else:
							returnVal[field] = []
							for x in dataRow:
								returnVal[field].append(x[field])
					else:
						returnVal[field] = dataRow[field]
			return returnVal

		def fetchData(session, url, params = {}):
			try:
				response = session.get(url, params=params)
			except TokenUpdated as e:
				self._tokenUpdated = True
				self._token = e.token
				response = session.get(url, params=params)
			if response.status_code != 200:
				self._backing_off = True
				self._ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
			return response

		def pagingFetchData(session, url, pageToken, item):
			done = False
			nextPageToken = ''
			result = []
			while not done:
				if nextPageToken == '': 
					data = fetchData(session, url)
				else: 
					data = fetchData(session, url, params={'pageToken': nextPageToken})
				if data.status_code != 200: 
					return []
				response = json.loads(data.text)
				if pageToken in response.keys():
					nextPageToken = response[pageToken]
				else: done = True
				if item in response.keys():
					for i in response[item]:
						result.append(i)
				else: self._ew(EventWriter.ERROR, "Query error: did not return expected field %s" % item)
			return result
		
		self._ew = ew
		self._backing_off = False
		try:
			args = {'host':'localhost','port':inputs.metadata['server_uri'][18:],'token':inputs.metadata['session_key']}
			service = Service(**args)
			for input_name, input_item in inputs.inputs.iteritems():
				token = { u'access_token': input_item["oauth2_access_token"], 
							 u'token_type': 'Bearer', 
							 u'expires_in': '1745', 
							 u'refresh_token': input_item['oauth2_refresh_token'] }
				token_refresh_params = {'client_id': input_item["oauth2_client_id"], 
												'client_secret': input_item["oauth2_client_secret"]}
				google_bq_sess = OAuth2Session(token_refresh_params['client_id'], scope=self._google_bq_ro_scope, 
														 token=token, auto_refresh_kwargs=token_refresh_params, 
														 auto_refresh_url=self._google_oauth2_token_url)
				while True:
					if self._backing_off: 
						time.sleep(input_item['backoff_time'])
						self._backing_off = False
					# List out the datasets in the project
					bq_base_url = self._google_bq_base_url + "/projects/" + urllib.quote(input_item['bigquery_project']) + "/datasets" 
					dsdata = pagingFetchData(google_bq_sess, bq_base_url, 'nextPageToken', 'datasets')
					if len(dsdata) == 0: 
						ew.log(EventWriter.ERROR, "Error: No datasets in project %s " % input_item['bigquery_project'] )
						continue
					# Match it up with the list provided in config
					found_datasets = []
					for ds in dsdata:
						found_datasets.append(ds['datasetReference']['datasetId'])
					if input_item['bigquery_dataset'] == '*':
						datasets = found_datasets
					else:
						datasets = []
						for s in input_item['bigquery_dataset'].split(','):
							if s.strip() in found_datasets:
								datasets.append(s.strip())
					
					# Process each dataset
					for dataset in datasets:
						# Fetch a list of tables in the dataset
						bq_ds_url = bq_base_url + "/" + urllib.quote(dataset) + "/tables"
						tables = pagingFetchData(google_bq_sess, bq_ds_url, 'nextPageToken', 'tables')
						if len(tables) == 0:
							ew.log(EventWriter.INFO, "No tables in dataset: %s " % dataset )
							continue
						query_mode = {'exec_mode': 'blocking'}
						ingestCount = 0
						gaTables = 0
						for table in tables:
							if '.ga_sessions_' in table['id']:
								gaTables += 1
								# Pass over intraday tables 
								if 'intraday' not in table['id']:
									splunk_jobs = service.jobs
									splunk_job = splunk_jobs.create('| metadata type=sources index=* | where totalCount>0 AND source="%s"' % table['id'], **query_mode)
									completedTables = []
									for result in ResultsReader(splunk_job.results()):
										completedTables.append(result['source'])
									# Pass over completed tables
									if len(completedTables) != 0:
										continue
									session_count = 0
									hit_count = 0
									chunk_count = 0
									ingestCount += 1
									ew.log(EventWriter.INFO, "Start table ingest=%s " % table['id'] )
									
									# We only want new tables. This is currently not intraday export compatible.
									# Collect the schema 
									bq_tables_url = bq_ds_url + "/" + table['tableReference']['tableId']
									response = fetchData(google_bq_sess, bq_tables_url)
									if response.status_code != 200:
										ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
										break
									schemaDict = json.loads(response.text)

									# Collect and process the data
									bq_tables_url = bq_ds_url + "/" + table['tableReference']['tableId'] + "/data"
									done = False
									pageToken = ''
									while not done:
										# Collect a page of data - around 20Mb chunk
										if pageToken == '': 
											response = fetchData(google_bq_sess, bq_tables_url)
										else: 
											response = fetchData(google_bq_sess, bq_tables_url, params={'pageToken': pageToken})
										if response.status_code != 200:
											ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
											break
										chunk_count += 1
										dataDict = json.loads(response.text)
										if 'pageToken' not in dataDict.keys():
											done = True
										else: 
											pageToken = dataDict['pageToken']

										# Join the data chunk with the table schema
										results = extractFields(schemaDict['schema'], dataDict)
										sessions = []
										hits = []
										
										# Process each row into its base session and hit elements
										for r in results:
											hit_ret = buildStruct(r, ['visitId', 'fullVisitorId', 'date', 'hits'])
											for hit in hit_ret['hits']:
												hit['fullVisitorId'] = hit_ret['fullVisitorId']
												hit['visitId'] = hit_ret['visitId']
												hit['date'] = hit_ret['date']
												hits.append(hit)
											session_ret = buildStruct(r, ['fullVisitorId', 'visitId', 'visitStartTime', 'trafficSource', 'geoNetwork', 'device'])
											session_ret['page_hostname'] = hit_ret['hits'][0]['page']['hostname']
											session_ret['hitCount'] = len(hit_ret['hits'])
											sessions.append(session_ret)

										# Harvest sessions
										session_count += len(sessions)
										for session in sessions:
											ew.write_event(Event(data=json.dumps(session),
																sourcetype='ga_sessions',
																source=table['id'],
																host=dataset,
																stanza=input_name,
																time=float(session['visitStartTime'])))

										# Harvest hits
										hit_count += len(hits)
										for hit in hits:
											ew.write_event(Event(data=json.dumps(hit),
																sourcetype='ga_hits',
																source=table['id'],
																host=dataset,
																stanza=input_name,
																time=calendar.timegm(time.strptime(hit['date'] + hit['hour'] + hit['minute'], '%Y%m%d%H%M'))))

									# Write out ingest stats
									ew.log(EventWriter.INFO, "Finished table ingest=%s chunkcount=%s hitcount=%s sessioncount=%s" % (table['id'], chunk_count, hit_count, session_count))
						
						ew.log(EventWriter.INFO, "Finished ingest pass, tablecount=%s ingestcount=%s" % (gaTables, ingestCount))

						# If the token was updated during the import process save it back to the input config.
						if self._tokenUpdated:
							ew.log(EventWriter.INFO, "Updating the %s oauth2 token" % input_name)
							try:
								item = service.inputs.__getitem__(input_name[7:])
								item.update(oauth2_access_token=self._token["access_token"],oauth2_refresh_token=self._token['refresh_token'])
								self._tokenUpdated = False
							except RuntimeError,e:
								ew.log(EventWriter.ERROR, "Input: %s ; Error updating the oauth2 token: %s" % ( input_name, str(e) ))

						# Sleep for 30 minutes; the token should be refreshed at that point, and this instance will likely be killed and restarted.
						for i in range(29):
							# This is a space for later - will use core reporting to pull out additional information
							time.sleep(60)
		except Exception, e:
			ew.log(EventWriter.ERROR, "Unhandled exception: %s" % type(e) )
			

if __name__ == "__main__":
	sys.exit(GABQInput().run(sys.argv))
