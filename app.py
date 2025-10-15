from flask import Flask, jsonify
import redis
import yaml

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

REDIS_HOST = config['redis']['host']
REDIS_PORT = config['redis']['port']
REDIS_DB = config['redis']['db']

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

app = Flask(__name__)

@app.route("/events")
def events():
    keys = r.keys("event:*")
    data = [r.get(k).decode() for k in keys]
    return jsonify(data)

if __name__ == "__main__":
    app.run(port=5000)
