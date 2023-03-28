from flask import Flask, request
from services.greedygame import *
app = Flask(__name__)

# for checking purpose of flask app in ui
@app.route("/")
def home():
    return "Pawan Kumar!"


@app.route('/set', methods=['POST'])
def set_key():
    try:
        data = request.get_json()
        data_list = data["command"].split()
        return set_function(data_list)
    except:
        return jsonify({'error': 'invalid command'})


@app.route('/get', methods=['POST'])
def get_key():
    try:
        data = request.get_json()
        data_list = data["command"].split()
        return get_function(data_list)
    except:
        return jsonify({'error': 'invalid command'})


@app.route('/qpush', methods=['POST'])
def qpush():
    try:
        data = request.get_json()
        qpush_list = data["command"].split()
        return qpush_function(qpush_list)
    except:
        return jsonify({'error': 'invalid command'})


@app.route('/qpop', methods=['POST'])
def qpop():
    try:
        data = request.get_json()
        qpop_list = data["command"].split()
        return qpop_function(qpop_list)

    except:
        return jsonify({'error': 'invalid command'})


@app.route('/bqpop', methods=['POST'])
def bqpop():
    try:
        data = request.get_json()
        bqpop_list = data["command"].split()
        return bqpop_function(bqpop_list)
    except:
        return jsonify({'error': 'invalid command'})


if __name__ == '__main__':
    app.run(debug=True)
