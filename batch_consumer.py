from kafka import KafkaConsumer
from json import loads
from collections import defaultdict
import os
import boto3
import time
import json
import uuid



class Batch:

    def __init__(self):
        self.access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.bucket = os.getenv('S3_BUCKET_NAME')
    
    def data_retriever(self):

        # create our consumer to retrieve the message from the topics
        data_stream_consumer = KafkaConsumer(
            bootstrap_servers = "localhost:9092",    
            value_deserializer = lambda message: loads(message),
            auto_offset_reset = "earliest" # This value ensures the messages are read from the beginning 
        )
    
        #checker=list(range(100)) 
        data_stream_consumer.subscribe(topics = ["coretopic"])
        clause = False
        global datapoint
        #datapoint = defaultdict(int)
        #datapoint['data']=[]
        datapoint=[]
        num = 0
        for message in data_stream_consumer:
            print(message)
                       
            num += 1
            #datapoint['data'].append(message.value)

            datapoint.append(message.value)
            print(f"next-------------------{num}--------")
            if num == 5000 :

                print("5000 Max Records Reached")
                break
                
        #print(datapoint)
        # if clause is True:
            
        #     if os.path.isdir('raw_data'):
        #         pass
        #     else:
        #         os.makedirs('raw_data') 
        #     path = 'raw_data'
        #     os.chdir(path)
        #     # To make a unique id using date and time 
        #     # timestr=datetime.now().strftime("%Y%m%d%H%M%S%")
            ###
            # #creates a file and names it after the uniques id
            # #adds data to the created json file
            # ###
            # newfolder = f'{uniqueid}'
            # os.makedirs(newfolder)
            # with open(f'{uniqueid}/data.json', 'w') as newjfile:
            #     json.dump(datapoint, newjfile)

        
    
        
    def upload_to_s3(self):
        '''
        Method to upload data to aws s3 directly for storage 
        import json
        import boto3
        '''
        
        #s3 = boto3.client('s3')
        json_object = datapoint      
        boto3.Session(aws_access_key_id = self.access_key,
        aws_secret_access_key = self.secret_key) # AWS credentials
        start_time = time.time()
        s3_client = boto3.client('s3')
        uniqueid = uuid.uuid4()
        #s3 = boto3.resource('s3')
        #path_dir = f"{os.getcwd()}/"
        bucket_name = 's3courier' #self.bucket
        s3_client.put_object(Body=json.dumps(json_object),Bucket=bucket_name, Key=(f"{uniqueid}/data.json"))
        # os.chdir(f"{path_dir}{uniqueid}")
        # print('uploading data...')
        # s3.Bucket(bucket_name).upload_file('data.json', f"{uniqueid}/data.json")  
        ####
        # sends the data into the uuid folder
        # s3_client.put_object(Bucket=bucket_name, Key=('Images/'))  
        # creates image folder in the uuid folder
        print(f"Uploaded ..........{(time.time() - start_time):.01f}s")
        
Batch().data_retriever()
Batch().upload_to_s3()
