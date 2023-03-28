from helpers.helper import *
from flask import jsonify
import threading

def set_function(data_list):
    try:
        data_list = data_list
        if data_list[0] == "SET":
            if len(data_list) == 4:
                if data_list[3] == "XX" and data_list[1] in datastore.keys() and not is_expired(data_list[1]):
                    datastore[data_list[1]][0] = data_list[2]
                    return jsonify({'status': 'OK'})
                else:
                    return jsonify({'status': 'key expired or not exist'})

            if len(data_list) == 5:
                if data_list[3] == "EX":
                    datastore[data_list[1]] = data_list[2:]
                    expiry = data_list[4]
                    datastore[data_list[1]][2] = get_expiry_time(expiry)
                    return jsonify({'status': 'OK'})
            if len(data_list) == 6:
                if data_list[5] == "NX" and data_list[1] not in datastore.keys():
                    datastore[data_list[1]] = data_list[2:]
                    expiry = data_list[4]
                    datastore[data_list[1]][2] = get_expiry_time(expiry)
                    return jsonify({'status': 'OK'})
                else:
                    return jsonify({'status': 'key already exist'})

            if len(data_list) == 3:
                datastore[data_list[1]] = data_list[2:]
                return jsonify({'status': 'OK'})
        else:
            return jsonify({'error': 'invalid command'})

    except:
        return jsonify({'error': 'invalid command'})

def get_function(data_list):
    if data_list[0] != "GET":
        return jsonify({'error': 'invalid command'})
    else:
        if data_list[1] in datastore.keys():
            return jsonify({'value': datastore[data_list[1]][0]})
        else:
            return jsonify({'error': 'key not found'})


def qpush_function(qpush_list):
    if qpush_list[0] == "QPUSH":
        if len(qpush_list) > 2:
            queues[qpush_list[1]] = qpush_list[2:]
        else:
            queues[qpush_list[1]] = []
        return jsonify({'status': 'OK'})

def qpop_function(qpop_list):
    if qpop_list[0] == "QPOP" and qpop_list[1] in queues.keys():
        if len(queues[qpop_list[1]]) == 0:
            return jsonify({'error': 'queue is empty'})
        return jsonify({'status': queues[qpop_list[1]].pop()})


def bqpop_function(bqpop_list):
    if bqpop_list[2] != 0:
        timeout = int(bqpop_list[2])
        event = threading.Event()
        timer = threading.Timer(timeout, lambda: event.set())
        timer.start()
        event.wait()
        timer.cancel()
    if bqpop_list[0] == 'BQPOP' and bqpop_list[1] in queues.keys():
        if len(queues[bqpop_list[1]]) == 0:
            return jsonify({'error': 'null'})
        return jsonify({'status': queues[bqpop_list[1]].pop()})
