#%%
import requests
import os
import urllib
import urllib.request
import json
import re
import numpy as np
import pandas as pd
import time
import random
from datetime import datetime
import matplotlib.pyplot as plt
#https://www.ted.com/talks/quick-list?page=132
root_dir = os.path.dirname(os.path.abspath(__file__))+'/'
post_responses = [] #responses from LBRY network when publishing.
DEFAULT_TAGS = ['TED', 'Presentation', 'Talk', 'TEDTalk']

def html_decode(s):
    """
    Returns the ASCII decoded version of the given HTML string. This does
    NOT remove normal HTML tags like <p>.
    """
    htmlCodes = (
            ("'", '&#39;'),
            ('"', '&quot;'),
            ('>', '&gt;'),
            ('<', '&lt;'),
            ('&', '&amp;')
        )
    for code in htmlCodes:
        s = s.replace(code[1], code[0])
    return s



def get_online_items():
	return requests.post("http://localhost:5279", json={"method": "claim_list", "params": {"channel_id": ['3ec9f6e1e4b077b8f4cc060b844f69827635b424'], "page_size":9999}}).json()

def neat_nameify(complex_name):
	return re.sub('[^0-9a-zA-Z]+', '_', complex_name      ).lower().replace('__','_')

#%% Upload new stuff to LBRY
df = pd.read_csv(root_dir + 'df.csv').set_index('authortitle', verify_integrity=True, drop=False)
online_items = get_online_items()['result']['items']
online_titles = [oi['value']['title'] for oi in online_items]

author_titles = df.index.tolist()
random.shuffle(author_titles)
for author_title in author_titles: #for author_title in df.index:#for i in range(100): #
	#author_title = random.choice(df.index)
	#row_no = j#random.randint(0,len(df)-1)
	row = df.loc[author_title]
	
	filename = neat_nameify(row['authortitle']) + '.mp4'

	if df.loc[author_title].isna().sum() == 0 and author_title not in online_titles and df.loc[author_title,'title'] not in online_titles:
		print(author_title + ' not among published videos. Preparing to publish it...')
		advanced_description = row['description'] + '\n\nOriginally published in ' + str(datetime.fromtimestamp(row['published_date']).strftime("%B %Y")) + ' from ' + row['event'] + '.\n\nAuthor/Speaker: ' + row['author']
		json_payload = {
			"method": "publish",
			"params": {
				"name": row['neat_name'],
				"title": row['title'],
				"bid": "0.01",
				"file_path": root_dir + 'videos/' + filename,
				"thumbnail_url": row['thumb_url'],
				"description": advanced_description,#row['description'],
				#"release_time": int(row['published_date']),
				#"validate_file": False,
				#"optimize_file": False,
				"tags": row['tags'][2:-2].split("', '") + DEFAULT_TAGS,
				#"languages": [],
				#"locations": [],
				"license": 'Creative Commons',
				"license_url": 'https://www.ted.com/about/our-organization/our-policies-terms/ted-talks-usage-policy',
				"channel_id": '3ec9f6e1e4b077b8f4cc060b844f69827635b424',
				#"preview": False,
				"blocking": True
				}
			}

		if filename not in [f for f in os.listdir(root_dir + '/videos')]:
			op = 'y'
			#op = input(print(filename + " not among downloaded videos. Shuold download it now and then publish? (y (yes)/c (cancel)/ (no)])"))
			if op == 'c':
				1/0
			if op != 'y': 
				continue

			print("File not found in download/upload folder. Downloading it now...")
			r = requests.get(row['url'], allow_redirects=True)

			if len(r.content) < 100:
				print("Video didn't download properly. (len(r.content) was less than 100. Should be about 10^7. Skipping this.")
				continue
			open(root_dir + '/videos/' + filename, 'wb').write(r.content)
		else:
			time.sleep(10) #So it doesn't go too fast and break due to LBRY's limits.
		print('Publishing the following to LBRY:')
		for k in ['title', 'tags', 'description', 'thumbnail_url']:
			print("\t%s: %s" % (k.capitalize(), json_payload['params'][k]))
		#if input('Should this be published (y)?') == 'y':
		post_response = requests.post(
			"http://localhost:5279",
			json=json_payload).json()
		if 'result' not in post_response:
			print("There was an error with the upload. Here is the response:")
			print(post_response)
			1/0
		print('Upload finished. Probably successfully')
		post_responses.append(post_response)
		#print('Upload finished. Check out the response:')
		#print(post_response)
		online_titles.append(filename)
		time.sleep(3600)


print('Finished!')

#%% Upping the claim for channel: @TEDTalks
#requests.post("http://localhost:5279", json={"method": "channel_update", "params": {"claim_id": "3ec9f6e1e4b077b8f4cc060b844f69827635b424", 'bid':'0.15'}}).json()


#%% Adding data to local df
df = pd.read_csv(root_dir + 'df.csv')#.set_index('authortitle', verify_integrity=True, drop=False)
print('Length of df that was loaded:', len(df))


df_kaggle = pd.read_csv(root_dir + 'kaggle.csv').set_index('name', verify_integrity=True, drop=False)
df_sheets = pd.read_csv(root_dir + 'sheets.csv')
df_sheets['name'] = df_sheets.apply(lambda x: str(x['speaker_name']) + ': ' + str(x['headline']), axis=1)
df_sheets = df_sheets.set_index('name', verify_integrity=True, drop=False)

for i in range(3):
	df = df.set_index('authortitle', verify_integrity=True, drop=False) #Needs to be authortitle-index for this step
	time.sleep(6)
	page_no = i#+198#random.randint(0,155)#
	print(page_no)
	try:
		c = urllib.request.urlopen('https://www.ted.com/talks/quick-list?page=' + str(page_no)).read()
	except Exception as e:
		print('Breaking because of the following exception:')
		print(e)
		break
	
	d = html_decode(c.decode("utf8"))
	start_term = 'row quick-list__row'
	e = re.split(start_term, d)[1:]
	split_term = 'quick-list__container-row'
	f = [re.split(split_term, i)[0] for i in e]
	if len(f) > 0:
		for html_row_no, html_row in enumerate(f[:-2]):
			dirty_rows = re.split('<',html_row)
			date = dirty_rows[2][19:27]
			authortitle = re.split('>',dirty_rows[7])[1]
			if authortitle in df.index:
				#print(authortitle, 'already in df. Not adding. Continuing.')
				#continue
				print(authortitle, 'already in df. Updating.')
				df = df.drop(authortitle)
			author = authortitle.split(': ')[0]
			title = ': '.join(authortitle.split(': ')[1:])
			print(len(dirty_rows))
			if len(dirty_rows) < 32: #ones that have dl links are 40, ones without are 28, it seems.
				continue
			url = dirty_rows[32][8:-6]
			event = re.split('>',dirty_rows[13])[1]
			#from_page_no = page_no
			neat_name = neat_nameify(authortitle)
			if authortitle in df_kaggle.index:
				description = df_kaggle.loc[authortitle,'description']
				published_date = df_kaggle.loc[authortitle,'published_date']
				tags = df_kaggle.loc[authortitle,'tags'][2:-2].split("', '")
			elif authortitle in df_sheets.index:
				description = df_sheets.loc[authortitle,'description']
				published_date = int(datetime.strptime(df_sheets.loc[authortitle,'published'], '%m/%d/%y').timestamp())
				tags = df_sheets.loc[authortitle,'tags'].split(",")
			else:
				description = np.nan
				published_date = np.nan
				tags = np.nan
			thumb_url = np.nan
			row = [date, authortitle, url, event, author, title, thumb_url, description, published_date, tags, neat_name]
			df = pd.concat([df, pd.DataFrame([row],columns=df.columns)])


	#df = pd.read_csv(root_dir + 'df.csv')
	df = df.set_index('title', verify_integrity=True, drop=False) #Needs to be title-index for this step.

	#Doing some thumbnail addings
	print('Trying to add some thumbnail urls. (%s / %s urls in)...' % (str(df['thumb_url'].count()),str(len(df['thumb_url']))))
	#for i in range(10):
	#page_no = random.randint(0,200)
	try:
		c = urllib.request.urlopen('https://www.ted.com/talks?page=' + str(page_no)).read()
	except Exception as e:
		print('Breaking because of the following exception:')
		print(e)
		break
	
	d = html_decode(c.decode("utf8"))
	if len(d) < 75000:
		continue
	split_term = '<div class=\'talk-link\'>\n'
	e = re.split(split_term, d)[1:]

	for f in e:
		title = f.split('talk-link__speaker')[1].split("'>\n")[2].split('\n<')[0]
		thumb_url = f.split('src=\"')[1].split('?quality=')[0]
		if title in df.index:# and df[df['title'] == title] == np.nan:
			df.loc[title,'thumb_url'] = thumb_url
				
	df.to_csv(root_dir + 'df.csv', index=False, encoding='utf8')
	print('...Finished. (%s / %s thumbnail urls out.)' % (str(df['thumb_url'].count()),str(len(df['thumb_url']))))

print("Finished filling in data to df")
#%% Show an overview of the data available
df = pd.read_csv(root_dir + 'df.csv')#.set_index('authortitle', verify_integrity=True, drop=False)
df_kaggle = pd.read_csv(root_dir + 'kaggle.csv').set_index('name', verify_integrity=True, drop=False)
df_sheets = pd.read_csv(root_dir + 'sheets.csv')

plt.figure(figsize=[12,7])
dataset = {'df': {'source':df}, 'kaggle': {'source':df_kaggle}, 'sheets': {'source':df_sheets}}

all_cols = []
for key in dataset:
	all_cols += [i for i in dataset[key]['source'].columns.tolist() if i not in all_cols]

x = np.arange(len(all_cols))
w = 0.25
for i, key in enumerate(dataset):
	o = dataset[key]
	o['label'] = []
	o['y'] = []
	for c in all_cols:
		o['label'].append(c)
		if c in o['source'].columns:
			o['y'].append(o['source'][c].count())
		else:
			o['y'].append(0)
	plt.bar(x - w*len(dataset)/2 + w*i, o['y'],w, label=key)

plt.ylabel('Number of rows') #Not with barh.
plt.xticks(x, all_cols)
plt.xticks(rotation=90)

plt.legend()

#%% Updating previously uploaded videos
r = get_online_items()
online_items = r['result']['items']
random.shuffle(online_items)

df = pd.read_csv(root_dir + 'df.csv').set_index('authortitle', verify_integrity=True, drop=False)

update_reqs = []
abandon_reqs = []
for i, oi in enumerate(online_items):
	#Abandoning duplicates online (and tossing the downloaded files locallt)
	for j, oi2 in enumerate(online_items):
		if i != j and oi['value']['title'] == oi2['value']['title']:
			if len(oi['value']['source']['name']) < len(oi2['value']['source']['name']):
				print("Found duplicates of %s. Abandoning the one who's (file)name is shorter." % oi['value']['title'])
				print(oi['value']['source']['name'])
				print(oi2['value']['source']['name'])
				abandon_reqs.append(requests.post("http://localhost:5279", json={"method": "stream_abandon", "params": {"claim_id": oi['claim_id']}}).json())
				try:
					os.remove(root_dir + '/videos/' + oi['value']['source']['name'])
					print('Deleted file')
				except:
					print('File already not present locally.')
				continue
	#if len(update_req) > 24:
	#	break
	a_t = oi['value']['title'] #author and title
	#Abandoning uploads without video files
	if 'video' not in oi['value']:
		print(a_t, "wasn't a video. Abandoning it now.")
		requests.post("http://localhost:5279", json={"method": "stream_abandon", "params": {"claim_id": oi['claim_id']}}).json()
		continue
	json_update_payload = {"method": "stream_update", "params": {
		"claim_id": oi['claim_id'],
		"blocking": True
		}}
	no_of_reasons_to_update_item = 0
	if a_t in df.index:
		if df.loc[a_t,'tags'] == df.loc[a_t,'tags'] and len(oi['value']['tags']) < len(df.loc[a_t,'tags'][2:-2].split("', '")) + len(DEFAULT_TAGS):
			json_update_payload['params']['tags'] = df.loc[a_t,'tags'][2:-2].split("', '") + DEFAULT_TAGS
			no_of_reasons_to_update_item += 1	
		if 'description' not in oi['value'] and df.loc[a_t,'description'] == df.loc[a_t,'description']:
			json_update_payload['params']['description'] = df.loc[a_t,'description']
			no_of_reasons_to_update_item += 1
		if 'release_time' not in oi['value'] and df.loc[a_t,'published_date'] == df.loc[a_t,'published_date']:
			json_update_payload['params']['release_time'] = int(df.loc[a_t,'published_date'])
			no_of_reasons_to_update_item += 1
		if 'thumbnail' not in oi['value'] and df.loc[a_t]['thumb_url'] == df.loc[a_t]['thumb_url']:
			json_update_payload['params']['thumbnail_url'] = df.loc[a_t]['thumb_url']
			no_of_reasons_to_update_item += True
			
	if len(json_update_payload['params'])-2 > 0: #"-2" because claim_id and blocking are params
		print('-------------------')
		print('Doing this update to %s:' % df.loc[a_t,'authortitle'])
		print(json_update_payload)
		update_req = requests.post("http://localhost:5279", json=json_update_payload).json()
		update_reqs.append(update_req)
		if 'result' not in update_req:
			print("There was an error with the upload. Here is the response:")
			print(update_req)
			1/0
		time.sleep(10)

print('Finished updating %s previously published videos!' % str(len(update_reqs)))
		#print(item['value'])#['thumbnail_url'])
