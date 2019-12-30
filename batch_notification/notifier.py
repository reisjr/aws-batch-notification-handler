import boto3
import datetime as dt
import time
import os
import json
from random import choice
from string import ascii_uppercase
import random

def create_random_name(size=8):
    return ''.join(choice(ascii_uppercase) for i in range(size))


# function to create a client with aws for a specific service and region
def create_client(service, region):
    return boto3.client(service, region_name=region)

# function to load data from CSV
def load_data(filename):
    df = pd.read_csv(filename)
    return df

# function to correctly display numbers in 2 value format (i.e. 06 instead of 6)
def lengthen(value):
    if len(value) == 1:
        value = "0" + value
    return value


def generate_random_purchase_event(msisdn, now):
    event = None
    
    if random.uniform(0, 100) >= 95.0:
        t_delta = dt.timedelta(minutes=random.randint(1, 2760))
        event_time = now + t_delta
        event = { 
            "Data": "{}\n".format(json.dumps({
            "ts": event_time.isoformat(),
            "msisdn": msisdn,
            "event": "PURCHASE"
            })) 
        }
    
    return event

# function for generating new runtime to be used for timefield in ES
def get_date():

    today = str(dt.datetime.today()) # get today as a string
    year = today[:4]
    month = today[5:7]
    day = today[8:10]

    hour = today[11:13]
    minutes = today[14:16]
    seconds = today[17:19]

    # return a date string in the correct format for ES
    return "%s/%s/%s %s:%s:%s" % (year, month, day, hour, minutes, seconds)

# function to modify the date time to be correctly formatted
def modify_date(data):
    
    dates = data['cdatetime'] # get the datetime field
    
    new_dates = [] # create empty lists
    load_times = [] 
    
    load_time = get_date() # get current time
    
    # loop over all records
    for date in dates:
        
        date = date.replace('/','-') # replace the slash with dash
        date = date + ":00" # add seconds to the datetime
        
        split = date.split(" ") # split the datetime
        
        date = split[0] # get just date 
        
        months = date.split('-')[0] # get months
        days = date.split('-')[1] # days
        years = "20" + date.split('-')[2] # years
        
        time = split[1] # get just time
        
        hours = time.split(':')[0] # get hours
        minutes = time.split(':')[1] # get minutes
        seconds = time.split(':')[2] # get seconds
        
        # build up a string in the right format
        new_datetime = years + "/" + lengthen(months) + "/" + lengthen(days) + " " + lengthen(hours) + ":" + lengthen(minutes) + ":" + seconds
        
        # add it the list
        new_dates.append(new_datetime)
        load_times.append(load_time)
    
    # update the datetime with our transformed version
    data['cdatetime'] = new_dates
    data['loadtime'] = load_times
    
    # return the dataframe
    return data

# function for sending data to Kinesis at the absolute maximum throughput
def send_kinesis(fh_client, kinesis_stream_name):

    currentBytes = 0 # counter for bytes
    rowCount = 0 # as we start with the first
    sendKinesis = False # flag to update when it's time to send data
    total_row_count = 0

    # loop over each of the data rows received 
    for i in range(0, 30000):
        batch = []
        
        for x in range(0, 250):
            now = dt.datetime.now().isoformat()
            msisdn = create_random_name(11)
            
            event_missed = { "Data": "{}\n".format(
                    json.dumps({
                        "ts": now,
                        "msisdn": msisdn,
                        "event": "MISSED_CALL"
                        }
                    ))
            }
            
            batch.append(event_missed)
            
            event_purchase = generate_random_purchase_event(msisdn, now)

            if event_purchase:
                batch.append(event_purchase)

        fh_client.put_record_batch(
            DeliveryStreamName=kinesis_stream_name,
            Records=batch
        )

        if i % 100 == 0:
            print("{} .".format(i))
            print(json.dumps(batch[0]))

    # log out how many records were pushed
    print('Total Records sent to Kinesis: {0}'.format(total_row_count))

# main function
def main():
    
    # start timer
    start = time. time()
    
    # create a client with kinesis
    kfh_cli = boto3.client('firehose', 'us-east-2')
    
    # send it to kinesis data stream
    stream_name = os.getenv("KFH_IS", "batch-notification-kfhdatalake-R34RRPQF40H7")
    
    send_kinesis(kfh_cli, stream_name) # send it!
    
    # end timer
    end = time.time()
    
    # log time
    print("Runtime: " + str(end - start))
    
if __name__ == "__main__":
    
    # run main
    main()