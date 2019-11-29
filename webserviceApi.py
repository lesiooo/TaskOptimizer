import TaskOptimizer
import GmapsApi
from flask import Flask

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return "Hello Flask TaskOptimizer"

@app.route('/optymize', methods=['GET'])
def optymize():
    response = TaskOptimizer.optymize()
    return response


@app.route('/map-optymize', methods=['POST'])
def get_trace_data():
    response = GmapsApi.get_trace_data()
    return response


if __name__ == '__main__':
    app.run()

