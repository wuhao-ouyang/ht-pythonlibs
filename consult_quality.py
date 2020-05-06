import graylog
from datetime import datetime, date, time, timedelta

def get_recent_sessions(hours=1, session_id=0):
	api = graylog.GraylogSearcher()
	if session_id == 0:
		sessions = api.relative_search(hours*3600, 'message:"end_session" AND (source:staging_chat)')
	else:
		sessions = api.relative_search(hours*3600, 'message:"end_session" AND message:"session_id={session_id}" AND (source:staging_chat)'.format(session_id=session_id))
	sessions = sorted(sessions, key=lambda k : k['timestamp'])
	sessions_set = set()
	session_ts = list()
	for session in sessions:
		if not session.has_key('session_id') or session['session_id'] in sessions_set or not session.has_key('interaction_id'):
			continue
		persons = session['interaction_id'].split('_')
		patien_id = persons[1]
		doctor_id = persons[0]
		session_id = session['session_id']
		timestamp = session['timestamp']
		session_ts.append((timestamp, session_id, patien_id, doctor_id))
		sessions_set.add(session_id)
	return session_ts

def get_session(from_ts, to_ts, session_id=0):
	api = graylog.GraylogSearcher()
	session_filter = ''
	if (session_id != 0):
		session_filter = 'AND message:"session_id={session_id}"'.format(session_id=session_id)
	sessions = api.absolute_search(from_ts, to_ts, 'message:"end_session" {session_filter} AND (source:staging_chat)'.format(session_filter=session_filter))
	sessions = sorted(sessions, key=lambda k : k['timestamp'])
	sessions_set = set()
	session_ts = list()
	for session in sessions:
		if not session.has_key('session_id') or session['session_id'] in sessions_set or not session.has_key('interaction_id'):
			continue
		persons = session['interaction_id'].split('_')
		patien_id = persons[1]
		doctor_id = persons[0]
		session_id = session['session_id']
		timestamp = session['timestamp']
		session_ts.append((timestamp, session_id, patien_id, doctor_id))
		sessions_set.add(session_id)
	return session_ts
	# session_ts_new = list()
	# for start_session in session_ts:
	# 	if not latest.has_key(start_session[1]) or start_session[0] - latest[start_session[1]] > timedelta(seconds=30):
	# 		session_ts_new.append(start_session)
	# 		latest[start_session[1]] = start_session[0]
	# return session_ts_new

def analyze_session(end_ts, session_id, patient_id, doctor_id, print_log=False):
	api = graylog.GraylogSearcher()
	start_ts = end_ts - timedelta(minutes=30)
	end_ts = end_ts + timedelta(minutes=5)
	search_string = 'message:"session_id={session_id}" OR message:"person_id={patient_id}" OR message:"person_id={doctor_id}"'\
								.format(session_id=session_id, patient_id=patient_id, doctor_id=doctor_id)
	events = api.absolute_search(start_ts,end_ts, search_string)
	events = sorted(events, key=lambda k : k['timestamp'])
	session_info = dict()
	session_info['patient_id'] = patient_id
	session_info['doctor_id'] = doctor_id
	session_info['start_time'] = start_ts
	session_info['session_id'] = session_id
	# session_info['SOURCE'] = set()
	total_send_bw_video = 0
	total_recv_bw_video = 0
	total_send_bw_audio = 0
	total_recv_bw_audio = 0
	if print_log:
		log_file = open(session_id + '_' + patient_id + '_' + doctor_id + '.tmp', 'w+')
	for event in events:
		if print_log:
			log_file.write("\n")
			log_file.write(print_mapping(event))
		if event.has_key('room_id') and event['room_id']:
			session_info['room_id'] = event['room_id']
		if event.has_key('SOURCE') and event['SOURCE']:
			# session_info['SOURCE'].add(event['SOURCE'])
			if event.has_key('person_id') and event['person_id'] == patient_id and 'server' != event['SOURCE']:
				session_info['customer_platform'] = event['SOURCE']
			elif event.has_key('person_id') and event['person_id'] == doctor_id and 'server' != event['SOURCE']:
				session_info['doctor_platform'] = event['SOURCE']
		if event.has_key('message_type') and 'end_session' == event['message_type']:
			session_info['end_time'] = event['timestamp']
			if event.has_key('end_session_reason'):
				session_info['end_session_reason'] = event['end_session_reason']
		if event.has_key('message_type') and 'start_session' == event['message_type']:
			session_info['start_time'] = event['timestamp']

		if event.has_key('action') and 'video_bandwidth_info' in event['action']:
			bandwidth_info = parse_bandwidth_info(event['message'])
			if bandwidth_info.has_key('ActualSendBwVideo'):
				total_send_bw_video += int(bandwidth_info['ActualSendBwVideo'])
			if bandwidth_info.has_key('ActualRecvBwVideo'):
				total_recv_bw_video += int(bandwidth_info['ActualRecvBwVideo'])
			if bandwidth_info.has_key('ActualSendBwAudio'):
				total_send_bw_audio += int(bandwidth_info['ActualSendBwAudio'])
			if bandwidth_info.has_key('ActualRecvBwAudio'):
				total_recv_bw_audio += int(bandwidth_info['ActualRecvBwAudio'])

	if session_info.has_key('end_time') and session_info.has_key('start_time'):
		session_info['duration'] = session_info['end_time'] - session_info['start_time']
	# if not session_info.has_key('customer_platform') or 'android_tablet' != session_info['customer_platform']:
	# 	return
	print 'Bandwidth info:', total_send_bw_video, total_recv_bw_video, total_send_bw_audio, total_recv_bw_audio
	print print_mapping(session_info)
	print ''

def parse_bandwidth_info(message):
	info = dict()
	pairs = message.split(',')
	for pair in pairs:
		if '=' not in pair:
			continue
		index = pair.index('=')
		key = pair[:index]
		value = pair[index+1:].replace('\n', '')
		info[key] = value
	return info

def print_mapping(params):
	tmp = list()
	for key in params:
		value = params[key]
		if isinstance(value,datetime):
			value_str = value.strftime('%Y-%m-%d %H:%M:%S')
		elif isinstance(value, set):
			value_str = ','.join(value)
		else:
			value_str = str(value)
		tmp.append('{key}={value}'.format(key=key, value=value_str))
	return ', '.join(tmp)

def get_long_notify_sessions(days=7):
	api = graylog.GraylogSearcher()
	sessions = api.relative_search(days*24*3600, '"event=notify_long_wait_patients" AND "source=handle_start_video"')
	sessions = sorted(sessions, key=lambda k : k['timestamp'])
	sessions_set = set()
	for session in sessions:
		sessions_set.add(session['session_id'])
	return sessions_set

def get_session_duration(session_id):
	api = graylog.GraylogSearcher()
	events = api.relative_search(30*24*3600, '"session_id={session_id}"'.format(session_id=session_id))
	events = sorted(events, key=lambda k : k['timestamp'])
	result = dict()
	result['participants'] = set()
	for event in events:
		if 'message_type=start_video' in event.get('raw_message'):
			result['start_time'] = event['timestamp']
		if 'message_type=end_session' in event.get('raw_message'):
			result['end_time'] = event['timestamp']
		if event.has_key('person_id'):
			person_id = event.get('person_id')
			if person_id and person_id != '0':
				result['participants'].add(event.get('person_id'))
	return result

print 'Please enter session_id:'
session_id = input()
session_info = get_session_duration(session_id)
persons_query = list()
print "Start time:"
print session_info['start_time'].strftime("%Y/%m/%d %H:%M:%S")
print "End time:"
print session_info['end_time'].strftime("%Y/%m/%d %H:%M:%S")
for person in session_info['participants']:
	persons_query.append('"person_id={id}"'.format(id=person))

api = graylog.GraylogSearcher()
events = api.absolute_search(session_info['start_time'], session_info['end_time'], '(' + " OR ".join(persons_query) + ') AND NOT "message_received"')
for event in events:
	if 'event_category=chat' in event['raw_message'] or 'event_category=mqtt' in event['raw_message'] or 'event_category=general' in event['raw_message']:
		print event['raw_message'].replace('"', '')
		# print "person_id={person}, event_name={event_name}".format(person=event['person_id'],event_name=event['event_name'])


# sessions = get_session(datetime(2016, 2, 11), datetime(2016, 2, 13))
# for session in sessions:
# 	# Specify session id to filter results
# 	# if session[1] == '62409':
# 	# if session[2] != '36409856' and session[2] != '36409856':
# 	if session[1] != '75161':
# 		continue
# 	print session[1]
# 	analyze_session(session[0], session[1], session[2], session[3], True)

#