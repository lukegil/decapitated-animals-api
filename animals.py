import ConfigParser
import urllib
import logging
from flask import Flask, jsonify, request, abort, g
from utils.connector import Connect
from rate_limit import ratelimit
from redis import Redis

app = Flask(__name__)
config = ConfigParser.ConfigParser()

config.read("/var/www/flaskapi/animals_api/config/mysql.conf")
env = config.get("environment","env")
app.config.update(
    db = {
        "database" : config.get(env, "DATABASE"),
        "host" : config.get(env, "HOST"),
        "user" : config.get(env, "USER"),
        "password" : config.get(env,"PASSWORD")
        }
    )

config.read("/var/www/flaskapi/animals_api/config/server.conf")
app.config.update(SECRET_KEY=config.get("server", "SECRET_KEY"))



def build_response(success, error, data):
    return {"success" : success,
            "error" : error,
            "data" : data}


def parse_list(db_response):
    """ returns {id : {object},id2 : {object}} """
    if len(db_response) == 0:
        abort(404)
    return_dict = {}
    for element in db_response:
        id = element.pop("incident_id")
        return_dict[id] = element
    return return_dict

def parse_object(db_response):
    """returns dictionary"""
    if len(db_response) == 0:
        abort(404)
    db_response[0].pop("incident_id")
    logging.debug("\n\nreturning to user : {}".format(db_response[0]))
    return db_response[0]


@app.before_request
@ratelimit(limit=100, per=600 * 1)
def authenticate_and_connect():
    if request.method != "GET":
        if request.get_json().get("secret", "") != app.config["SECRET_KEY"]:
            abort(401)
    g.db = Connect(app.config["db"])

@app.after_request
def clean_up(response):
    try: 
        g.db.close()
    except AttributeError:
        return response
    return response

@app.errorhandler(401)
def four_oh_one(error):
    return jsonify(build_response(False, "Access Denied", {})), 401

@app.errorhandler(404)
def four_oh_four(error):
    return jsonify(build_response(False, "Your request returned no data", {})), 404


@app.route('/cases/', methods=['GET', 'POST'])
def all_cases():
    if request.method == "GET":
        query = "SELECT * FROM incidents"
        params = ()
        result = g.db.request(query, params)
        return jsonify(build_response(True, "None", parse_list(result)))       

    if request.method == "POST":
        r_json = request.get_json()
        r_json.pop("secret")
        id = g.db.insert(r_json)
        result = g.db.get_record(id) 
        return jsonify(build_response(True, "None", parse_list(result)))

    abort(404)

@app.route('/cases/<int:id>',methods=['GET', 'POST'])
def single_case(id):
    if request.method == "GET":
        result = g.db.get_record(id)
        return jsonify(build_response(True, "None", parse_object(result)))

    if request.method == "POST":
        r_json = request.get_json()
        r_json.pop("secret")
        logging.debug("\n\nattempting update, payload : {}".format(r_json))
        id = g.db.update(r_json, id)
        result = g.db.get_record(id) 
        return jsonify(build_response(True, "None", parse_object(result)))
       


@app.route('/cases/<string:field>/<value>')
def subset(field, value):
    if request.method == "GET":
        query = ("SELECT * FROM incidents "
                 "WHERE {} LIKE CONCAT('%',%s,'%') ").format(urllib.quote(str(field)))
        params = (value,)
        result = g.db.request(query, params)
        return jsonify(build_response(True, "None", parse_list(result)))

@app.route('/cases/<string:field>/<value>/count')
def subset_count(field, value):
    if request.method == "GET":
        query = ("SELECT count(*) AS 'count' FROM incidents "
                 "WHERE {} LIKE CONCAT('%',%s,'%') ").format(urllib.quote(str(field)))
        params = (value,)
        result = g.db.request(query, params)
        return jsonify(build_response(True, "None", parse_list(result)))

        

if __name__ == "__main__":
    debugger = True
    if debugger:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.DEBUG)
    logging.debug("==== server starting ====")
    app.run(debug=debugger)

