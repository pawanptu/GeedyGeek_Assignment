# helper Function
import time
datastore = {}
queues = {}

def current_time():
    return int(time.time())


def get_expiry_time(expiry):
    return current_time() + int(expiry)


def is_expired(key):
    try:
        return current_time() > datastore[key][2]
    except:
        return False
