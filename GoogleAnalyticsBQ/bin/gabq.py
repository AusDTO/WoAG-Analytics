#!/usr/bin/python
'''
GA-BQ Input Script
'''

import sys,os,time, traceback

SPLUNK_HOME='/opt/splunk'
#dynamically load in any eggs in /etc/apps/GoogleAnalyticsBQ/bin
APP_HOME = SPLUNK_HOME + "/etc/apps/GoogleAnalyticsBQ/"
EGG_DIR = APP_HOME + "bin/"

for filename in os.listdir(EGG_DIR):
    if filename.endswith(".egg"):
        sys.path.append(EGG_DIR + filename)


import requests, json, time, calendar, urllib, multiprocessing
from Queue import Empty
from requests_oauthlib import OAuth2Session, TokenUpdated
from oauthlib.oauth2 import WebApplicationClient, TokenExpiredError
from splunklib.client import connect
from splunklib.client import Service
from splunklib.results import *
from splunklib.modularinput import *

class GABQInput(Script):
	# OAuth endpoints given in the Google API documentation
	_google_oauth2_authorization_url = "https://accounts.google.com/o/oauth2/auth"
	_google_oauth2_token_url = "https://accounts.google.com/o/oauth2/token"
	_google_bq_base_url = "https://www.googleapis.com/bigquery/v2"
	_google_ga_base_url = "https://www.googleapis.com/analytics/v3/management/accounts"
	_google_bq_ro_scope = [ "https://www.googleapis.com/auth/bigquery.readonly", "https://www.googleapis.com/auth/analytics.readonly" ]
	_tokenUpdated = False

	def get_scheme(self):
		scheme = Scheme("Google Analytics - BigQuery")
		scheme.description = "Input method for Google Analytics data using Google's BQ REST API"
		scheme.use_external_validation = True
		scheme.use_single_instance = False
		scheme.add_argument(Argument(name="bigquery_project", description="BigQuery Project - GA data in this context", required_on_create=True))
		scheme.add_argument(Argument(name="bigquery_query_project", description="BigQuery Query Project - queries run in this context", required_on_create=True))
		scheme.add_argument(Argument(name="bigquery_dataset", description="BigQuery Dataset(s); separate by comma for multiple sets, or * for all datasets in a project", required_on_create=True))
		scheme.add_argument(Argument(name="default_timezone", description="Default timezone for data. Used when the account cannot see the View through the GA API. Format 'Australia/Sydney', 'GMT', etc (similar to user prefs in Splunk)", required_on_create=True))
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

		def fetchData(ew, state, tokenLock, url, params = {}):
			# build session
			session = OAuth2Session(state['token_refresh']['client_id'], scope=self._google_bq_ro_scope, token=state['token'])
			try:
				response = session.get(url, params=params)
				if response.status_code == 401:
					# If we get a 401, then try and cycle our token and see if that sorts it out.
					raise TokenExpiredError
			except TokenExpiredError as e:
				with tokenLock:
					ew.log(EventWriter.ERROR, "Token has expired, refreshing")
					state['token'] = session.refresh_token(self._google_oauth2_token_url, **state['token_refresh'])
				response = session.get(url, params=params)
			except Exception, e:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				ew.log(EventWriter.ERROR, "Unhandled exception: %s, fetching %s" % (type(e), url) )
				for msg in traceback.format_exception(exc_type, exc_value, exc_traceback):
					ew.log(EventWriter.ERROR, msg)
			if response.status_code != 200:
				# Error message, die
				ew.log(EventWriter.ERROR, "Query error: URL %s, params %s, response code %s, body %s" % ( url, params, response.status_code, response.text ) )
				sys.exit(1)
			return response

		def pagingFetchData(ew, state, tokenLock, url, pageToken, item):
			done = False
			nextPageToken = ''
			result = []
			while not done:
				if nextPageToken == '':
					data = fetchData(ew, state, tokenLock, url)
				else:
					data = fetchData(ew, state, tokenLock, url, params={'pageToken': nextPageToken})
				if data.status_code != 200:
					return []
				response = json.loads(data.text)
				if pageToken in response.keys():
					nextPageToken = response[pageToken]
				else: done = True
				if item in response.keys():
					for i in response[item]:
						result.append(i)
				else:
					ew.log(EventWriter.ERROR, "Query error: %s did not return expected field %s, saw %s" % (url, item, str(response.keys())) )
			return result

		def downloader(state, tokenLock, ew, job):
			# job = { url dataset table startRow rowCount dsLength schema input }
			pid = os.getpid()
			ew.log(EventWriter.INFO, "P: %s Start chunk ingest=%s @ %s" % (pid, job['table'], job['startRow']))
			params = {'maxResults': job['rowCount'], 'startIndex': job['startRow']}
			response = fetchData(ew, state, tokenLock, job['url'], params=params)
			if response.status_code != 200:
				ew.log(EventWriter.ERROR, "P: %s Query error: URL %s, startPoint %s, response code %s, body %s" %
													( pid, job['url'], params, job['startRow'], response.status_code, response.text ) )
			dataDict = json.loads(response.text)
			# Join the data chunk with the table schema
			results = extractFields(job['schema'], dataDict)
			sessions = []
			hits = []

			# If the retreval was not a full set of records then push a new job back into the queue for the remaining records
			if len(results) != int(job['rowCount']):
				ew.log(EventWriter.INFO, "P: %s Reinsert chunk ingest=%s @ startpoint %s, rowcount %s, prevresults %s, dsLen %s" % (pid, job['table'], job['startRow']+len(results), job['rowCount'] - len(results), len(results), job['dsLength']))
				downloadQueue.put({'url': job['url'], 'dataset': job['dataset'], 'table': job['table'],
									'startRow': job['startRow'] + len(results), 'rowCount': job['rowCount'] - len(results),
									'dsLength': job['dsLength'], 'schema': job['schema'], 'input': job['input']})

			# Process each row into its base session and hit elements
			for r in results:
				session_ret = buildStruct(r, ['fullVisitorId', 'visitId', 'visitStartTime', 'trafficSource', 'geoNetwork', 'device'])
				hit_ret = buildStruct(r, ['visitId', 'fullVisitorId', 'date', 'hits'])
				session_ret['hitCount'] = len(hit_ret['hits'])
				session_ret['humanTime'] = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(float(session_ret['visitStartTime'])))
				for hit in hit_ret['hits']:
					hit['fullVisitorId'] = hit_ret['fullVisitorId']
					hit['visitId'] = hit_ret['visitId']
					hit['date'] = hit_ret['date']
					hit['hitTime'] = float(session_ret['visitStartTime']) + (float(hit['time'])/1000)
					hit['humanTime'] = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(hit['hitTime']))
					hits.append(hit)
				session_ret['page_hostname'] = hit_ret['hits'][0]['page']['hostname']
				sessions.append(session_ret)

			# Harvest sessions
			for session in sessions:
				ew.write_event(Event(data=json.dumps(session),
									sourcetype='ga_sessions',
									source=job['table'],
									host=job['dataset'],
									stanza=job['input'],
									time=float(session['visitStartTime'])))

			# Harvest hits
			for hit in hits:
				ew.write_event(Event(data=json.dumps(hit),
									sourcetype='ga_hits',
									source=job['table'],
									host=job['dataset'],
									stanza=job['input'],
									time=hit['hitTime']))

			# update counters
			ew.log(EventWriter.INFO, "P: %s End chunk ingest=%s @ %s, %s" % (pid, job['table'], job['startRow'], len(sessions)))

		def downloadManager(downloadQueue, state, processingState, tokenLock, ew):
			max_procs = 10
			ew.log(EventWriter.INFO, "DM started %s" % os.getpid())
			# Keep spawning consumers until either the parent says there are no more job to be added to the queue
				# (by unsetting state['processing']) or the queue is empty and until there are no running children.
			while (processingState.is_set()) or (not downloadQueue.empty()) or (len(multiprocessing.active_children()) > 0):
				if len(multiprocessing.active_children()) < max_procs:
					try:
						job = downloadQueue.get(True, 5)
						x = multiprocessing.Process(target=downloader, args=(state, tokenLock, ew, job))
						x.start()
						downloadQueue.task_done()
					except Empty:
						time.sleep(1)
			ew.log(EventWriter.INFO, "DM finished")
			return

		downloadQueue = multiprocessing.JoinableQueue()
		dataManager = multiprocessing.Manager()
		try:
			processingState = dataManager.Event()
			processingState.set()
			state = dataManager.dict()
			tokenLock = multiprocessing.Lock()
			args = {'host':'localhost','port':inputs.metadata['server_uri'][18:],'token':inputs.metadata['session_key']}
			service = Service(**args)
			for input_name, input_item in inputs.inputs.iteritems():
				state['token'] = { u'access_token': input_item["oauth2_access_token"],
										 u'token_type': 'Bearer',
										 u'expires_in': '60',
										 u'refresh_token': input_item['oauth2_refresh_token'] }
				state['token_refresh'] = {'client_id': input_item["oauth2_client_id"],
												  'client_secret': input_item["oauth2_client_secret"]}
				while True:
					ew.log(EventWriter.ERROR, "Processing run started pid %s" % os.getpid())
					google_bq_sess = OAuth2Session(state['token_refresh']['client_id'], scope=self._google_bq_ro_scope, token=state['token'])
					views = {}
					acctdata = pagingFetchData(ew, state, tokenLock, self._google_ga_base_url, 'nextPageToken', 'items')
					for acct in acctdata:
						propdata = pagingFetchData(ew, state, tokenLock, acct['childLink']['href'], 'nextPageToken', 'items')
						for prop in propdata:
							viewdata = pagingFetchData(ew, state, tokenLock, prop['childLink']['href'], 'nextPageToken', 'items')
							for view in viewdata:
								views[view['id']] = view
					# List out the datasets in the project
					bq_base_url = self._google_bq_base_url + "/projects/" + urllib.quote(input_item['bigquery_project']) + "/datasets"
					dsdata = pagingFetchData(ew, state, tokenLock, bq_base_url, 'nextPageToken', 'datasets')
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

                    # Get the list of completed tables
					query_mode = {'count': 0}
                    # Table seen to be completed when it has results in it - not a 'hard' verification against source data.
					splunk_job = service.jobs.oneshot('| metadata type=sources index=* | where totalCount>0', **query_mode)
					completedTables = []
					for result in ResultsReader(splunk_job):
						completedTables.append(result['source'])
					ew.log(EventWriter.INFO, "Info: %s completedTables, first is: %s" % (len(completedTables), completedTables[0]))

					# Process each dataset
					gaTables = 0
					ingestCount = 0
					downloadManagerProcess = multiprocessing.Process(target=downloadManager, args=(downloadQueue, state, processingState, tokenLock, ew))
					for dataset in datasets:
						# Set processing timezone
						try:
							os.environ['TZ'] = views[dataset]['timezone']
						except KeyError:
							os.environ['TZ'] = input_item['default_timezone']
						time.tzset()
						# Fetch a list of tables in the dataset
						ew.log(EventWriter.ERROR, "Processing dataset %s" % dataset )
						bq_ds_url = bq_base_url + "/" + urllib.quote(dataset) + "/tables"
						tables = pagingFetchData(ew, state, tokenLock, bq_ds_url, 'nextPageToken', 'tables')
						if len(tables) == 0:
							ew.log(EventWriter.INFO, "No tables in dataset: %s " % dataset )
							continue
						for table in tables:
							if '.ga_sessions_' in table['id']:
								gaTables += 1
								# Pass over completed and intraday tables
								if ('intraday' not in table['id']) and (table['id'] not in completedTables):
									ingestCount += 1

									# We only want new tables. This is currently not intraday export compatible.
									# Collect the schema
									bq_tables_url = bq_ds_url + "/" + table['tableReference']['tableId']
									response = fetchData(ew, state, tokenLock, bq_tables_url)
									if response.status_code != 200:
										ew.log(EventWriter.ERROR, "Query error: Response code %s, body %s" % ( response.status_code, response.text ) )
										ew.log(EventWriter.ERROR, "Query error: URL %s, params {}, response code %s, body %s" % ( bq_tables_url, response.status_code, response.text ) )
										break
									schemaDict = json.loads(response.text)

									bq_tables_url = bq_ds_url + "/" + table['tableReference']['tableId'] + "/data"
									# Inject the dataset ID, page checkpoints and page counts into the download queue
									# Rowcounter to be 1000
									sr = 0
									while sr < int(schemaDict['numRows']):
										# job = { url dataset startRow rowCount dsLength schema input }
										if (int(schemaDict['numRows']) - sr) < 1000:
											downloadQueue.put({'url': bq_tables_url, 'dataset': dataset, 'table': table['id'],
																	 'startRow': sr, 'rowCount': int(schemaDict['numRows']) - sr, 'dsLength': schemaDict['numRows'], 'schema': schemaDict['schema'],
																	 'input': input_name})
										else:
											downloadQueue.put({'url': bq_tables_url, 'dataset': dataset, 'table': table['id'],
																	 'startRow': sr, 'rowCount': 1000, 'dsLength': schemaDict['numRows'], 'schema': schemaDict['schema'],
																	 'input': input_name})
										sr += 1000
									if ingestCount == 1:
										# if we have not already started the DM then start it now
										job = downloadQueue.get(True)
										downloader(state, tokenLock, ew, job)
										downloadQueue.task_done()
										downloadManagerProcess.start()

					# Tell the downloadManager there are no more jobs to be added.
					processingState.clear()

					# Wait for the queue to be empty and join the downloadManager
					downloadQueue.join()
					if downloadManagerProcess.is_alive():
						downloadManagerProcess.join()
					ew.log(EventWriter.INFO, "Finished ingest pass, tablecount=%s ingestcount=%s" % (gaTables, ingestCount))
					dataManager.shutdown()
					time.sleep(1800)

		except Exception, e:
			exc_type, exc_value, exc_traceback = sys.exc_info()
			for msg in traceback.format_exception(exc_type, exc_value, exc_traceback):
				ew.log(EventWriter.ERROR, msg )
			ew.log(EventWriter.ERROR, "Quitting")

if __name__ == "__main__":
	sys.exit(GABQInput().run(sys.argv))
