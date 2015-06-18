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
from splunklib.modularinput import *

class GABQInput(Script):
	# OAuth endpoints given in the Google API documentation
	_google_oauth2_authorization_url = "https://accounts.google.com/o/oauth2/auth"
	_google_oauth2_token_url = "https://accounts.google.com/o/oauth2/token"
	_google_bq_base_url = "https://www.googleapis.com/bigquery/v2"
	_google_bq_ro_scope = [ "https://www.googleapis.com/auth/bigquery.readonly" ]

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

	def token_update(self, token, srv_uri, srv_port, sess_tok, name, ew):
		ew.log(EventWriter.INFO, "Updating the %s oauth2 token" % name)
		try:
			args = {'host':'localhost','port':srv_port,'token':sess_tok}
			service = Service(**args)   
			item = service.inputs.__getitem__(name[7:])
			item.update(oauth2_access_token=token["access_token"],oauth2_refresh_token=token['refresh_token'])
		except RuntimeError,e:
			ew.log(EventWriter.ERROR, "Input: %s ; Error updating the oauth2 token: %s" % ( name, str(e) ))


	def stream_events(self, inputs, ew):
		def getCompletedTables(checkpointDir):
			#checkpointDir = "/tmp"
			r = {}
			try: 
				f = open(checkpointDir + "/tables_completed", "r")
				l = f.readlines()
				f.close()
			except IOError:
				return r
			for e in l:
				(tablename, inputname) = e.split(" ", 1)
				inputname = inputname.rstrip('\n')
				if inputname in r.keys():
					r[inputname].append(tablename)
				else: r[inputname] = [ tablename ]
			return r

		def addCompletedTable(checkpointDir, tablename, inputname):
			try:
				f = open(checkpointDir + "/tables_completed", "a+")
				f.write("%s %s\n" % (tablename, inputname))
				f.close()
			except IOError:
				pass

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

		self._eventwriter = ew
		token = {}
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
				bq_base_url = self._google_bq_base_url + "/projects/" + urllib.quote(input_item['bigquery_project']) + "/datasets" 
				try:
					response = google_bq_sess.get(bq_base_url)
				except TokenUpdated as e:
					self.token_update(e.token, inputs.metadata['server_uri'], inputs.metadata['server_uri'][18:], inputs.metadata['session_key'], input_name, ew)
					response = google_bq_sess.get(bq_base_url)
				if response.status_code != 200:
					ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
					continue
				if 'datasets' in json.loads(response.text).keys():
					dsdata = json.loads(response.text)['datasets']
				else:
					ew.log(EventWriter.ERROR, "Error: No datasets in project %s " % input_item['bigquery_project'] )
					continue
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
				for dataset in datasets:
					bq_ds_url = bq_base_url + "/" + urllib.quote(dataset) + "/tables"
					try:
						response = google_bq_sess.get(bq_ds_url)
					except TokenUpdated as e:
						self.token_update(e.token, inputs.metadata['server_uri'], inputs.metadata['server_uri'][18:], inputs.metadata['session_key'], input_name, ew)
						response = google_bq_sess.get(bq_ds_url)
					if response.status_code != 200:
						ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
						continue
					tables = json.loads(response.text)['tables']
					completedTables = getCompletedTables(inputs.metadata['checkpoint_dir'])
					ingestCount = 0
					gaTables = 0
					for table in tables:
						if input_name not in completedTables.keys(): completedTables[input_name] = []
						if '.ga_sessions_' in table['id']:
							gaTables += 1
							if table['id'] not in completedTables[input_name] and 'intraday' not in table['id']:
								ingestCount += 1
								ew.log(EventWriter.INFO, "Table ingest: %s " % table['id'] )
								
								# We only want new tables. This is currently not intraday export compatible.
								# Collect the schema and data
								bq_tables_url = bq_ds_url + "/" + table['tableReference']['tableId']
								try:
									response = google_bq_sess.get(bq_tables_url)
								except TokenUpdated as e:
									self.token_update(e.token, inputs.metadata['server_uri'], inputs.metadata['server_uri'][18:], inputs.metadata['session_key'], input_name, ew)
									response = google_bq_sess.get(bq_tables_url)
								if response.status_code != 200:
									ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
									break
								schemaDict = json.loads(response.text)
								bq_tables_url = bq_ds_url + "/" + table['tableReference']['tableId'] + "/data"
								done = False
								pageToken = ''
								while not done:
									try:
										if pageToken == '': response = google_bq_sess.get(bq_tables_url)
										else: response = google_bq_sess.get(bq_tables_url, params={'pageToken': pageToken})
									except TokenUpdated as e:
										self.token_update(e.token, inputs.metadata['server_uri'], inputs.metadata['server_uri'][18:], inputs.metadata['session_key'], input_name, ew)
										response = google_bq_sess.get(bq_tables_url)
									if response.status_code != 200:
										ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
										break
									dataDict = json.loads(response.text)
									if 'pageToken' not in dataDict.keys():
										done = True
									else: pageToken = dataDict['pageToken']

									results = extractFields(schemaDict['schema'], dataDict)
									sessions = []
									visitors = []
									hits = []
									for r in results:
										sessions.append(buildStruct(r, ['fullVisitorId', 'visitId', 'visitStartTime', 'trafficSource', 'geoNetwork']))
										visitors.append(buildStruct(r, ['fullVisitorId', 'device']))
										hit_ret = buildStruct(r, ['visitId', 'date', 'hits'])
										for hit in hit_ret['hits']:
											hit['visitId'] = hit_ret['visitId']
											hit['date'] = hit_ret['date']
											hits.append(hit)

									# Harvest sessions
									ew.log(EventWriter.INFO, "Ingesting %s ga_sessions records" % len(sessions) )
									for session in sessions:
										ew.write_event(Event(data=json.dumps(session),
															sourcetype='ga_sessions',
															stanza=input_name,
															time=float(session['visitStartTime'])))

									# Harvest visitors
									ew.log(EventWriter.INFO, "Ingesting %s ga_visitor records" % len(visitors) )
									for visitor in visitors:
										ew.write_event(Event(data=json.dumps(visitor),
															sourcetype='ga_visitors',
															stanza=input_name))

									# Harvest hits
									ew.log(EventWriter.INFO, "Ingesting %s ga_hits records" % len(hits) )
									for hit in hits:
										ew.write_event(Event(data=json.dumps(hit),
															sourcetype='ga_hits',
															stanza=input_name,
															time=calendar.timegm(time.strptime(hit['date'] + hit['hour'] + hit['minute'], '%Y%m%d%H%M'))))
								addCompletedTable(inputs.metadata['checkpoint_dir'], table['id'], input_name)
					ew.log(EventWriter.INFO, "Saw %s tables and ingested %s on this pass, sleeping" % (gaTables, ingestCount))
					
					# Sleep for 30 minutes; the token should be refreshed at that point, and this instance will likely be killed and restarted.
					for i in range(29):
						# This is a space for later - will use core reporting to pull out additional information
						time.sleep(60)

if __name__ == "__main__":
	sys.exit(GABQInput().run(sys.argv))
