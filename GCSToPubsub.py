import os
import time 
from google.cloud import pubsub_v1
from google.cloud import pubsub

if __name__ == "__main__":

    project = 'my-project'
    pubsub_topic = 'my-topic'
    input_file = 'my-input-file-path'

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input file is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(5)    