""" Thus function automates the pull of larger studies such as Lung Transplant (LT) from the Red Cap database"""
api_url = "https://redcap.nubic.northwestern.edu/redcap/api/" #api address
api_token = "enter your token here" #change for your token
recs = requests.post(api_url, #get all the records names
  data={
    'token':   api_token,
    'content': 'record',
    'format':  'json'
  }
).json() #pulls all the records information from the studies

print(f"Got {len(recs)} records\n") #checks the number of studies


# print("First 3 records preview:") 
# print(json.dumps(recs[:3], indent=2)) 

record_ids = sorted(set([r['record_id'] for r in recs])) #keeps the unique record_ids
print(record_ids) #onle keep unique record names

meta = requests.post(api_url, data={
  'token': api_token,
  'content': 'metadata',
  'format': 'json'
}).json()  #pull the metadata of the studies (fields)


file_fields = [m['field_name']
               for m in meta
               if m['field_type']=='file']
print("File‑upload fields:", file_fields) #pulls fields that contain physical files such as fcs and pdf reports


folder = 'all_LT_new_trial'
os.makedirs(folder, exist_ok=True) #creates a folder where the studes will be saved

file_fields_project_specific = ['bal1_sort_report', 'bal1_sort_fcs', 'bal2_sort_report', 'bal2_sort_fcs', 
                                'bal3_sort_report', 'bal3_sort_fcs', 'bal4_sort_report', 'bal4_sort_fcs']   #for my purposes I needed these file fields

for rec in record_ids: #for each record id pull the file
    for fld in file_fields_project_specific:
        payload = {
            'token':   api_token,
            'content': 'file',
            'action':  'export',
            'record':  rec,
            'field':   fld,            # one field per request
        }
        r = requests.post(api_url, data=payload)
        if r.status_code == 200 and r.content:
            ctype = r.headers.get('Content-Type', '')
            if 'text/xml' in ctype:
                print(f"No file for {rec}-{fld}, got XML error")
                continue

            # extract the name="..." parameter
            m = re.search(r'name="([^"]+)"', ctype)
            filename = m.group(1)   # e.g. "20210923_LT001_LUL_23092021171938.pdf"
            # write out the file
            outpath = os.path.join(folder, filename)
            with open(outpath, 'wb') as f:
                f.write(r.content)
            print(f"Wrote {outpath}")
        else:
             print(f"No file for {rec}‑{fld} (HTTP {r.status_code})")

