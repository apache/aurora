from sys import argv

from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello():
  return "Hello Twitter"

app.run(
  host="0.0.0.0",
  port=int(argv[1]))
