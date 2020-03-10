#
# proksch: 2020-03-10 Adding support for .tar.gz and .tgz files
#
#
#-------------------------------------
#
# Azure specific imports
#
#-------------------------------------
from azure.storage.blob import *
from azure.identity import *

#-------------------------------------
#
# AWS specific imports
#
#-------------------------------------


#-------------------------------------
#
# GCP specific imports
#
#-------------------------------------
from datetime import datetime, timedelta
from zipfile import ZipFile
import gzip
import json

def azureGetZip(accountURL):


    service = BlobServiceClient(account_url=accountURL, credential=None)

    ai = service.get_account_information()

    all_containers = service.list_containers(include_metadata=True)
    for container in all_containers:
        #print(container['name'], container['metadata'])
        container_client = service.get_container_client(container)
        for blob in container_client.list_blobs():
            #print(blob.name)
            blob_client = service.get_blob_client(container=container, blob=blob)
            stream = blob_client.download_blob()
            wb = open(blob.name,'wb') 
            stream.download_to_stream(wb)
            wb.close()
            for f in ZipGZip(blob.name):
                print(f)
                data = open(f,"rb")
                blob_client = service.get_blob_client(container=container, \
                   blob=f)
                blob_client.upload_blob(data, blob_type="BlockBlob")
                data.close()



def ZipGZip(zipFileName):
    fnames = []
    with ZipFile(zipFileName, 'r') as z:
        for zipEntry in z.namelist():
            #print("Processing file: {}".format(zipEntry))
            with z.open(zipEntry) as zipped:
                fname = "{}.gz".format(zipEntry)
                with gzip.open(fname, mode='w', compresslevel=9) as gz:
                    gz.write(zipped.read())
            fnames.append(fname)
    return(fnames)


if __name__ == "__main__":
    with open('Config.json','r') as jsonfile:
        data = json.load(jsonfile)

    if data['cloud'] == 'azure':
        azureGetZip(data['SASToken'])
    else:
        print("Cloud Not Supported *Yet*")
