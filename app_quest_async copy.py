import pandas as pd
import os, glob
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
import csv
from csv import writer
from csv import reader
import os
import shutil


# initializations 
firestore_key = r'database_key.json' 
cred = credentials.Certificate(firestore_key)
app = firebase_admin.initialize_app(cred)
collection_names = [u'task']
db = firestore.client(app=app)
#for col in db.collections():
#    collection_names.append(col.id)

#Collections extraction 
'''
Python script to export Users data from Cloud Firestore to csv file ,
The script requires Python3, and the following packages to be installed:
- pip install pandas
- pip install firebase-admin
'''


def firestore_to_csv_paginated(db, coll_to_read, fields_to_read, csv_filename='extract.csv', max_docs_to_read=-1, write_headers=True):
    """ Extract Firestore collection data and save in CSV file
    Args:
        db: Firestore database object
        coll_to_read: name of collection to read from in Unicode format (like u'CollectionName')
        fields_to_read: fields to read (like ['FIELD1', 'FIELD2']). Will be used as CSV headers if write_headers=True
        csv_filename: CSV filename to save
        max_docs_to_read: max # of documents to read. Default to -1 to read all
        write_headers: also write headers into CSV file
    """

    # Check input parameters
    if (str(type(db)) != "<class 'google.cloud.firestore_v1.client.Client'>") or (type(coll_to_read) is not str) or not (isinstance(fields_to_read, list) or isinstance(fields_to_read, tuple) or isinstance(fields_to_read, set)):
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Unexpected parameters: \n\tdb = {db} \n\tcoll_to_read = {coll_to_read} \n\tfields_to_read = {fields_to_read}')
        return

    # Read Firestore collection and write CSV file in a paginated algorithm
    page_size = 1000   # Preset page size (max # of rows per batch to fetch/write at a time). Adjust in your case to avoid timeout in default 60s
    total_count = 0
    coll_ref = db.collection(coll_to_read)
    docs = []
    cursor = None
    try:
        # Open CSV file and write header if required
        print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Started processing collection {coll_to_read}...')
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_to_read, extrasaction='ignore', restval='Null')
            if write_headers: 
                writer.writeheader()
                print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Finished writing CSV headers: {str(fields_to_read)} \n---')
        
            # Append each page of data fetched into CSV file
            while True:
                docs.clear()    # Clear page
                count = 0       # Reset page counter

                if cursor:      # Stream next page starting from cursor
                    docs = [snapshot for snapshot in coll_ref.limit(page_size).order_by('__name__').start_after(cursor).stream()]
                else:           # Stream first page if cursor not defined yet
                    docs = [snapshot for snapshot in coll_ref.limit(page_size).order_by('__name__').stream()]
            
                print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Started writing CSV row {total_count+1}...')    # +1 as total_count starts at 0
                for doc in docs:
                    doc_dict = doc.to_dict()
                    
                    # Process columns (e.g. add an id column)
                    doc_dict['FIRESTORE_ID'] = doc.id   # Capture doc id itself. Comment out if not used
                    
                    # Process rows (e.g. convert all date columns to local time). Comment out if not used
                    for header in doc_dict.keys():
                        if (header.find('DATE') >= 0) and (doc_dict[header] is not None) and (type(doc_dict[header]) is not str):
                            try:
                                doc_dict[header] = doc_dict[header].astimezone()
                            except Exception as e_time_conv:
                                print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Exception in converting timestamp of {doc.id} in {doc_dict[header]}', e_time_conv)

                    # Write rows but skip certain rows. Comment out "if" and unindent "write" and "count" lines if not used
                    if ('TO_SKIP' not in doc_dict.keys()) or (('TO_SKIP' in doc_dict.keys()) and (doc_dict['TO_SKIP'] is not None) and (doc_dict['TO_SKIP'] != 'VALUE_TO_SKIP')):
                        writer.writerow(doc_dict)
                        count += 1

                # Check if finished writing last page or exceeded max limit
                total_count += count                # Increment total_count
                if len(docs) < page_size:           # Break out of while loop after fetching/writing last page (not a full page)
                    break
                else:
                    if (max_docs_to_read >= 0) and (total_count >= max_docs_to_read):
                        break                       # Break out of while loop after preset max limit exceeded
                    else:
                        cursor = docs[page_size-1]  # Move cursor to end of current page
                        continue                    # Continue to process next page

    except Exception as e_read_write:
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Exception in reading Firestore collection / writing CSV file:', e_read_write)
    else:
        print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Finished writing CSV file with {total_count} rows of data \n---')

        


        
def firestore_to_csv_paginated_sub_collections(db, ctr, fields_to_read, csv_filename='extract.csv', max_docs_to_read=-1, write_headers=True):
    """ Extract Firestore collection data and save in CSV file
    Args:
        db: Firestore database object
        ctr: name of collection to read from in Unicode format (like u'CollectionName')
        fields_to_read: fields to read (like ['FIELD1', 'FIELD2']). Will be used as CSV headers if write_headers=True
        csv_filename: CSV filename to save
        max_docs_to_read: max # of documents to read. Default to -1 to read all
        write_headers: also write headers into CSV file
    """

    # Check input parameters
    if (str(type(db)) != "<class 'google.cloud.firestore_v1.client.Client'>") or (type(ctr) is not str) or not (isinstance(fields_to_read, list) or isinstance(fields_to_read, tuple) or isinstance(fields_to_read, set)):
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Unexpected parameters: \n\tdb = {db} \n\tctr = {ctr} \n\tfields_to_read = {fields_to_read}')
        return

    # Read Firestore collection and write CSV file in a paginated algorithm
    page_size = 1000   # Preset page size (max # of rows per batch to fetch/write at a time). Adjust in your case to avoid timeout in default 60s
    total_count = 0
    # change manually to the wanted sub-collection 
    coll_ref = db.collection(str(col_name)).document(str(ctr)).collection(str(scn))
    docs = []
    cursor = None
    try:
        # Open CSV file and write header if required
        print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Started processing collection {ctr}...')
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_to_read, extrasaction='ignore', restval='Null')
            if write_headers: 
                writer.writeheader()
                print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Finished writing CSV headers: {str(fields_to_read)} \n---')
        
            # Append each page of data fetched into CSV file
            while True:
                docs.clear()    # Clear page
                count = 0       # Reset page counter

                if cursor:      # Stream next page starting from cursor
                    docs = [snapshot for snapshot in coll_ref.limit(page_size).order_by('__name__').start_after(cursor).stream()]
                else:           # Stream first page if cursor not defined yet
                    docs = [snapshot for snapshot in coll_ref.limit(page_size).order_by('__name__').stream()]
            
                print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Started writing CSV row {total_count+1}...')    # +1 as total_count starts at 0
                for doc in docs:
                    doc_dict = doc.to_dict()
                    
                    # Process columns (e.g. add an id column)
                    doc_dict['FIRESTORE_ID'] = doc.id   # Capture doc id itself. Comment out if not used
                    
                    # Process rows (e.g. convert all date columns to local time). Comment out if not used
                    for header in doc_dict.keys():
                        if (header.find('DATE') >= 0) and (doc_dict[header] is not None) and (type(doc_dict[header]) is not str):
                            try:
                                doc_dict[header] = doc_dict[header].astimezone()
                            except Exception as e_time_conv:
                                print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Exception in converting timestamp of {doc.id} in {doc_dict[header]}', e_time_conv)

                    # Write rows but skip certain rows. Comment out "if" and unindent "write" and "count" lines if not used
                    if ('TO_SKIP' not in doc_dict.keys()) or (('TO_SKIP' in doc_dict.keys()) and (doc_dict['TO_SKIP'] is not None) and (doc_dict['TO_SKIP'] != 'VALUE_TO_SKIP')):
                        writer.writerow(doc_dict)
                        count += 1

                # Check if finished writing last page or exceeded max limit
                total_count += count                # Increment total_count
                if len(docs) < page_size:           # Break out of while loop after fetching/writing last page (not a full page)
                    break
                else:
                    if (max_docs_to_read >= 0) and (total_count >= max_docs_to_read):
                        break                       # Break out of while loop after preset max limit exceeded
                    else:
                        cursor = docs[page_size-1]  # Move cursor to end of current page
                        continue                    # Continue to process next page

    except Exception as e_read_write:
        print(f'??? {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Exception in reading Firestore collection / writing CSV file:', e_read_write)
    else:
        print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} firestore_to_csv() - Finished writing CSV file with {total_count} rows of data \n---')        
        
        
        

# Connect to Firestore with service account
print(f'>>> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Started connecting to Firestore database...')
firestore_key = r'database_key.json'  # Replace with your Firebase key file
cred = credentials.Certificate(firestore_key)
# app = firebase_admin.initialize_app(cred)
db = firestore.client(app=app)
print(f'<<< {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Firestore database connected \n---')


for col_name in collection_names:
    doc = db.collection(col_name).stream()
    # Specify Firestore schema and extract file info
    coll_to_read = str(col_name)
    counter = 0
    for i in doc:
        counter += 1
        if i and counter == 1:
            fields = list(i.to_dict().keys())
    #fields = ['final', 'chosenProposalID', 'price', 'citizenReviewed', 'dueDate', 'accepted', 'status', 'heroName', 'citizenName', 'timing', 'citizenReview', 'address', 'online', 'genre', 'latLng', 'jobImgURL', 'paymentIntent', 'posterRating', 'title', 'heroID', 'desc', 'completed', 'citizenID', 'taskID', 'numOffers', 'datePosted', 'offers', 'heroReview']
    fields = ['posterRating','dueDate','price','heroName','paymentIntent','title','citizenID','genre','chosenProposalID','bidded','completed','timing','citizenReview','online','jobImgURL','datePosted','_id','__metadata','latLng','desc','address','heroReview','heroRating','accepted','final','taskID','numOffers','heroID','offers','status','paymentMethod']    
    csv_headers = fields
    folder_name = 'collections'
    # Check whether the specified path exists or not
    isExist = os.path.exists(folder_name)
    if not isExist:
      os.makedirs(folder_name)
    csv_filename = fr'.\{folder_name}\{col_name}.csv'
    csv_filename = csv_filename[0:-4] + datetime.now().strftime("_%Y-%m-%d_%H-%M-%S.csv")   # Timestamp filename in multiple runs. Comment out if not used
    # Extract docs from Firestore
    firestore_to_csv_paginated(db, coll_to_read, csv_headers, csv_filename)
    # add user-id
    user_ref = db.collection(col_name)
    user_id_array = []
    for user_collection in user_ref.get():
        user_id_array.append(user_collection.id)
    print('----------------')
    print(len(user_id_array))
    print('----------------')
    df = pd.read_csv(csv_filename)
    uid = pd.DataFrame({'uid': user_id_array})
    res = pd.concat([uid, df], axis=1)
    path_with_uid = 'collections_with_uid'
    isExist = os.path.exists(path_with_uid)
    if not isExist:
      os.makedirs(path_with_uid)
    res.to_csv(fr'.\{path_with_uid}\{col_name}_with_uid'+ datetime.now().strftime("_%Y-%m-%d_%H-%M-%S.csv") , index = False)
    print('getting collection finished !')
    
    

    print('-----------------------------')
    sub_col_folder = 'sub_collections'
    isExist = os.path.exists(sub_col_folder)
    if not isExist:
        os.makedirs(sub_col_folder)
    sub_col_name = ['proposal','comments']
    flds = []
    # for d in db.collection(str(col_name)).stream():
    #     for collection_ref in d.reference.collections():
    #         sub_col_name.append(collection_ref.id)
    #         sub_col_name = list(dict.fromkeys(sub_col_name))
    #         print(sub_col_name)
    if len(sub_col_name) == 0:
        continue
    else:
        for user_id in user_id_array:
            ctr = user_id
            for scn in sub_col_name:
                flds.clear()
                fl = db.collection(str(col_name)).document(str(user_id)).collection(str(scn))
                for f in fl.stream():
                    flds.append(list(f.to_dict().keys()))
                if len(flds) == 0:
                    continue
                else:
                    # change fields here 
                    #fields = ['timeApplied', 'taskID', 'citizenUID']
                    csv_headers = flds[0]
                    folder_name = f'sub_collections/{col_name}_{scn}'
                    # Check whether the specified path exists or not
                    isExist = os.path.exists(folder_name)
                    if not isExist:
                      os.makedirs(folder_name)
                    csv_filename = fr'.\sub_collections\{col_name}_{scn}\{user_id}.csv'
                    csv_filename = csv_filename[0:-4] + datetime.now().strftime("_%Y-%m-%d_%H-%M-%S.csv")   # Timestamp filename in multiple runs. Comment out if not used

                    # Extract docs from Firestore
                    firestore_to_csv_paginated_sub_collections(db, ctr, csv_headers, csv_filename)

                    uid_list = []
                    df = pd.read_csv(csv_filename)
                    for i in range(len(df)):
                        uid_list.append(user_id)

                    uid = pd.DataFrame({'uid': uid_list})
                    res = pd.concat([uid, df], axis=1)
                    res.to_csv(csv_filename, index = False)

print('---------------------------------------')
print('merging files..')
print('---------------------------------------')
#  Merge Sub-collections CSV files into one file 
folders = os.listdir('sub_collections')
for folder in folders:
    #change path to your directory
    path = os.chdir(str(os.getcwd()).replace('\\sub_collections','')+f'/sub_collections/{folder}')
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    #export to csv
    combined_csv.to_csv( f"{folder}.csv", index=False, encoding='utf-8-sig')
    for item in os.listdir(os.getcwd()):
        print(os.getcwd())
        if item not in f"{folder}.csv":  # If it isn't in the list for retaining
            os.remove(item)
    os.chdir('../..')