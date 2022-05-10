import multiprocessing
import re
import config
import pandas as pd
import glob
import requests
import shutil

def getMasterData(BaseDir = ""):
    files = [each.replace("\\", "/") for each in glob.glob(BaseDir + r"/*csv")]
    masterData = pd.DataFrame()
    for eachFile in files:
        data = pd.read_csv(eachFile)
        print(eachFile.split("/")[-1].split("_")[0], "\t" ,data.shape)
        masterData = masterData.append(data, ignore_index=True)
        
    masterData = masterData.drop_duplicates(subset="ID", keep="last")
    return masterData

def GetImage(imageLink, filename):
    r = requests.get(imageLink, stream = True)
    if r.status_code == 200:
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        r.raw.decode_content = True
        
        # Open a local file with wb ( write binary ) permission.
    with open(filename,'wb') as f:
            shutil.copyfileobj(r.raw, f)


# split a list into evenly sized chunks
def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def parallelFetch(i, data, TargetDir, TargetResolution):
    print("Starting Process ", i, "With Len of Data to Fetch: ", len(data))
    for link,name in data:
        link = re.sub("_60.png$", "_" + str(TargetResolution) + ".png", link) 
        GetImage(imageLink = link, filename = TargetDir + "/" + str(name) + ".png")
    print("Completed Process ", i)

def dispatch_jobs(data, job_number):
    total = len(data)
    chunk_size = total // job_number
    slice = chunks(data, chunk_size)
    jobs = []

    for i, s in enumerate(slice):
        j = multiprocessing.Process(target=parallelFetch, args=(i, s, config.TargetDir, config.TargetResolution))
        jobs.append(j)
    for j in jobs:
        j.start()


if __name__ == "__main__":
    masterData = getMasterData(config.MetaDataPath)
    masterData.to_csv(config.MetaDataPath + "/masterData.csv", index = False)
    masterData = masterData[["Photo","ID"]].to_records(index=False)
    dispatch_jobs(masterData, 8)