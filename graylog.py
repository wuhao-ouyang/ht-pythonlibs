import ConfigParser
import urllib, urllib2, httplib
import base64, re
# from csvsort import csvsort
from dateutil import tz
from datetime import tzinfo, timedelta, datetime

# query=session_id%3D74139&range=14400&fields=message%2Csource

BASEURL = 'https://graylog.internal.production.us-west-1.healthtap.com/api/search/universal'
ANDROID_FILTER = 'message:"os=Android" AND message:"browser=App" AND source:staging_analytics_v2'

config = ConfigParser.RawConfigParser()
config.read('credentials.ini')
defaultusername = config.get('GraylogCredentials', 'username')
defaultpassword = config.get('GraylogCredentials', 'password')

class GraylogSearcher:
	def __init__(self, username=defaultusername, password=defaultpassword):
		self.username = username
		self.password = password

	def relative_search(self, duration, search_string):
		params = {'fields':'source,message', 'range':str(duration), 'query':search_string}
		events = self.__internalsearch(BASEURL + '/relative/export?' + urllib.urlencode(params))
		events = sorted(events, key=lambda k : k['timestamp'], reverse=True)
		return events

	def absolute_search(self, from_ts, to_ts, search_string):
		date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
		# from_ts = from_ts.astimezone(tz.tzutc())
		# to_ts = to_ts.astimezone(tz.tzutc())
		params = {'fields':'source,message', 'from':from_ts.strftime(date_format), 'to':to_ts.strftime(date_format), 'query':search_string}
		events = self.__internalsearch(BASEURL + '/absolute/export?' + urllib.urlencode(params))
		events = sorted(events, key=lambda k : k['timestamp'], reverse=False)
		return events

	def get_csv_results(self, search_string, file_name='graylog_search.tmp', duration=24*60*60):
		params = {'rangetype':'relative', 'relative':str(duration), 'q':search_string}
		response = self.__graylogsearch(BASEURL + 'relative?' + urllib.urlencode(params))
		csv_file = open(file_name, 'w')
		csv_file.write(response.read())
		# csvsort(file_name, [0])
		return file_name

	def get_person_events(self, from_ts, to_ts, person_id, reverse=False):
		if from_ts > to_ts:
			return
		search_string = 'message:"person_id={person_id}" AND '.format(person_id=person_id) + ANDROID_FILTER
		events_list = self.absolute_search(from_ts, to_ts, search_string)
		return events_list

	def __graylogsearch(self, request_url):
		request = urllib2.Request(request_url)
		request.add_header('Authorization', 'Basic '+base64.b64encode('{username}:{password}'.format(username=self.username, password=self.password)))
		return urllib2.urlopen(request)

	def __internalsearch(self, request_url):
		print request_url
		response = self.__graylogsearch(request_url)
		events_list = list()
		while True:
			line = response.readline()
			if not line:
				break
			try:
				events = parse_event(line)
			except ValueError, e:
				continue
			if events:
				events_list.append(events)
		return events_list

def parse_event(line):
	colums = line.split(',')
	if len(colums) < 3:
		return
	elif len(colums) > 3:
		colums[2] = ','.join(colums[2::])
	events = dict()
	# for m in re.finditer('([a-z_]+)=([^=]*?)\s|$', colums[2].replace('"', '')):
	# 	key = m.group(1)
	# 	value = m.group(2)
	# 	if key and '_id' in key:
	# 		value = re.sub('[^0-9]', '', value)
	# 	events[key] = value
	if 'HTCloud_analytics_v2_1' in colums[1]:
		values = colums[2].replace('[', ', ').replace(']', '').split(', ')
	else:
		values = colums[2].replace('"', '').split(' ')
	if 'unified_logging' in colums[1]:
		pairs = list()
		tmp = list()
		for pair in values:
			tmp.append(pair)
			if '=' in pair:
				pairs.append(' '.join(tmp))
				tmp = list()
	else:
		pairs = values

	for pair in pairs:
		try:
			pair = pair.replace('"', '')
			index = pair.index('=')
			key = pair[:index]
			value = pair[index+1:].replace('\n', '')
			if key and key in ('person_id', 'session_id'):
				value = re.sub('[^0-9]', '', value)
			events[key] = value
		except ValueError, e:
			continue
	dt = datetime.strptime(colums[0].replace('"', ''), '%Y-%m-%dT%H:%M:%S.%fZ')
	events['timestamp'] = dt
	events['raw_message'] = colums[2]
	return events