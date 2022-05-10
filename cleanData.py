import glob
import os
import config
import skimage.io as io
import numpy as np 
import multiprocessing
import os

def dumpQueue(q):
    q.put(None)
    return list(iter(q.get, None))

# split a list into evenly sized chunks
def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def checkFile(fname:str, baseImage):
    try:
        image = io.imread(fname=fname)
        if np.array_equal(image, baseImage):
            return False
        else:
            return True
    except Exception as e:
        return False 

def checkFiles(procId, dataSlice, baseImage, queue):
    print("Process Started With: ", procId, "with len of ds: ", len(dataSlice))
    for each in dataSlice:
        if not checkFile(each, baseImage):
            queue.put(each)
            os.rename(each, config.TargetDir + "/dummy/" + each.split("/")[-1] )
    print("Process Completed: ", procId)        

def dispatchJobs(data, job_number, baseImage):
    total = len(data)
    chunk_size = total // job_number
    slice = chunks(data, chunk_size)
    jobs = []
    
    manager = multiprocessing.Manager()
    queue = manager.Queue()
    for i, s in enumerate(slice):
        j = multiprocessing.Process(target=checkFiles, args=(i, s, baseImage, queue))
        jobs.append(j)
        j.start()
    
    [j.join() for j in jobs]
        


if __name__ == "__main__":
    #Make the dummy directory to move all problematic/dummy images to this folder before deletion 
    image_dir = config.TargetDir + '/dummy'
    if not os.path.exists(image_dir):
        os.makedirs(image_dir)
    
    data = [each.replace("\\", "/") for each in glob.glob(config.TargetDir + "/*png") ]
    baseImage = io.imread(config.TargetDir + r"/1605.png")
    dispatchJobs(data, 8, baseImage)