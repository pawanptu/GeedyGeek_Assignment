from flask import Flask, request, jsonify
import time
import threading

app = Flask(__name__)

# for checking purpose of flask app in ui
@app.route("/")
def home():
    return "Hello, World!"

datastore = {}
queues = {}

def key_exists(key):
    return key in datastore

def current_time():
    return int(time.time())

def get_expiry_time(expiry):
    return current_time() + int(expiry)

def is_expired(key):
    try:
        return current_time() > datastore[key][2]
    except:
        return False


def get_value(key):
    if key_exists(key) and not is_expired(key):
        return datastore[key]['value']
    return None


@app.route('/set', methods=['POST'])
def set_key():
    data = request.get_json()
    print("getting jason data", data)
    data_list = data["command"].split()
    print("data list", data_list)
    try:
        if data_list[0] == "SET":
            if len(data_list) == 4:
                print(datastore)
                if data_list[3] == "XX" and data_list[1] in datastore.keys() and not is_expired(data_list[1]):
                    print(datastore)
                    datastore[data_list[1]][0] = data_list[2]
                    print(datastore)
                    return jsonify({'status': 'OK'})
                else:
                    return jsonify({'status': 'key expired or not exist'})

            print('helodo')
            if len(data_list) == 5:
                if data_list[3] == "EX":
                    datastore[data_list[1]] = data_list[2:]
                    expiry = data_list[4]
                    datastore[data_list[1]][2] = get_expiry_time(expiry)
                    return jsonify({'status': 'OK'})
            print("jdslfj")
            if len(data_list) == 6:
                if data_list[5] == "NX" and data_list[1] not in datastore.keys():
                    datastore[data_list[1]] = data_list[2:]
                    print(datastore)
                    expiry = data_list[4]
                    print(expiry)
                    datastore[data_list[1]][2] = get_expiry_time(expiry)
                    print(datastore)
                    return jsonify({'status': 'OK'})
                else:
                    return jsonify({'status': 'key already exist'})

            if len(data_list) == 3:
                datastore[data_list[1]] = data_list[2:]
                return jsonify({'status': 'OK'})

    except:
        print("data set")
        return jsonify({'error': 'invalid command'})


@app.route('/get', methods=['POST'])
def get_key():
    data = request.get_json()
    data_list = data["command"].split()
    if data_list[0] != "GET":
        return jsonify({'error': 'invalid command'})
    else:
        if data_list[1] in datastore.keys():
            print(datastore)
            print(datastore[data_list[1]][0])
            return jsonify({'value': datastore[data_list[1]][0]})
        else:
            return jsonify({'error': 'key not found'})


@app.route('/qpush', methods=['POST'])
def qpush():
    data = request.get_json()
    qpush_list = data["command"].split()
    print("qpush_list", qpush_list)
    try:
        if qpush_list[0] == "QPUSH":
            if len(qpush_list) > 2:
                queues[qpush_list[1]] = qpush_list[2:]
                print(queues)
            else:
                queues[qpush_list[1]] = []
            return jsonify({'status': 'OK'})
    except:
        return jsonify({'error': 'invalid command'})


@app.route('/qpop', methods=['POST'])
def qpop():
    data = request.get_json()
    qpop_list = data["command"].split()
    try:
        if qpop_list[0] == "QPOP" and qpop_list[1] in queues.keys():
            if len(queues[qpop_list[1]]) == 0:
                return jsonify({'error': 'queue is empty'})
            return jsonify({'status': queues[qpop_list[1]].pop()})

    except:
        return jsonify({'error': 'invalid command'})


@app.route('/bqpop', methods=['POST'])
def bqpop():
    data = request.get_json()
    bqpop_list = data["command"].split()
    try:
        if bqpop_list[2] != 0:
            timeout = int(bqpop_list[2])
            event = threading.Event()
            print(" event ")
            timer = threading.Timer(timeout, lambda: event.set())
            print(timer)
            print("timer.start()")
            timer.start()
            print("Please wait", timeout)
            event.wait()
            print("timer.cancel()")
            timer.cancel()
        print(queues)
        if bqpop_list[0] =='BQPOP' and bqpop_list[1] in queues.keys():
            print(queues)
            if len(queues[bqpop_list[1]]) == 0:
                return jsonify({'error': 'null'})
            return jsonify({'status': queues[bqpop_list[1]].pop()})
        print(datastore)
        print(queues)
    except:
        return jsonify({'error': 'invalid command'})


if __name__ == '__main__':
    app.run(debug=True)
