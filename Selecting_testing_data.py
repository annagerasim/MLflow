"""The following scripts allow to select 20 files from each file to build specific training set inventory"""

inventory = pd.read_csv("ML flow analysis - Copy of Inventory 2025.csv") #loading a copy of ready inventory
inventory.head()

inventory_clean = inventory[inventory["Is there a PDF report?"] == True] #get rid of the files that do not have PDFs reports
#optional and recomended
#inventory_clean = inventory_clean[inventory_clean["# of PDF reports associated with fcs file"] == 1]

groups_count = inventory_clean.groupby("Study").size() #check how many files under each study


studies = inventory_clean['Study'].unique()
sampled_dfs = []

for study in studies:
    if groups_count[study] >= 20: #<- maybe changed
        study_sample = inventory_clean[inventory_clean['Study'] == study].sample(n=20, random_state=42) # Sample 20 rows for each study <-maybe changed
        sampled_dfs.append(study_sample)
        
    else:
        sampled_dfs.append(inventory_clean[inventory_clean['Study'] == study])
        
sampled_df = pd.concat(sampled_dfs, ignore_index=True)# Concatenate all samples

sampled_df.columns

"""After assembling the sataset, you may use the following scripts to seacrh through each of the folder and save those files into a seperate folder.
Assumes that the folders are in the same directory as the scripts"""

#This is a sample, please modify pathes 
SOURCE_FOLDER = "PASC"  # Where the original files are stored
DESTINATION_FOLDER = "testing_data"  # Where to copy the files
FILE_COLUMN = "FCS File"  # change to "PDF reports names:" to extract pdf reports

os.makedirs(DESTINATION_FOLDER, exist_ok=True)
sampled_studies = sampled_df[sampled_df["Study"] == "PASC"] #change PASC to the study folder you are working with 
# Get the list of files from your sampled DataFrame
files_to_copy = sampled_studies[FILE_COLUMN].tolist()

# Create a dictionary to track found/not found files
copy_results = {
    'copied': 0,
    'not_found': [],
    'errors': []
}

# Walk through all subdirectories to find the files
for root, dirs, files in os.walk(SOURCE_FOLDER):
    for file in files:
        if file in files_to_copy:
            source_path = os.path.join(root, file)
            destination_path = os.path.join(DESTINATION_FOLDER, file)
            
            try:
                shutil.copy2(source_path, destination_path)
                print(f"Copied: {file} (from {root})")
                copy_results['copied'] += 1
                # Remove found file from list to avoid duplicate searches
                files_to_copy.remove(file)
            except Exception as e:
                print(f"Error copying {file}: {str(e)}")
                copy_results['errors'].append(file)

# Check for any files that weren't found
for missing_file in files_to_copy:
    print(f"File not found in any subfolder: {missing_file}")
    copy_results['not_found'].append(missing_file)

print(f"\nSummary:")
print(f"Successfully copied: {copy_results['copied']} files")
print(f"Not found: {len(copy_results['not_found'])} files")
print(f"Errors: {len(copy_results['errors'])} files")
print(f"Destination: {DESTINATION_FOLDER}")

