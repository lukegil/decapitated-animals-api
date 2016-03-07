import mysql.connector
import urllib
import logging

class Connect(object):
    """Class that manages Database connections"""

    def __init__(self, creds):
        self.cnx = mysql.connector.connect(**creds)

    def close(self):
        self.cnx.close()

    def request(self, query, params):
        """returns a dictionary of results"""
        cursor = self.cnx.cursor(dictionary=True)
        
        cursor.execute(query, params)
        print cursor._executed
        return cursor.fetchall()
    
    def get_record(self, id):
        query = ("SELECT * FROM incidents " 
                 "WHERE incident_id = %s")
        params = (id,)
        return self.request(query, params)

    def update(self, r_json, id):
        return self.update_or_insert(r_json, "UPDATE", id)

    def insert(self, r_json):
        return self.update_or_insert(r_json, "INSERT INTO")

    def update_or_insert(self, r_json, phrase, id=None):
        """returns id of insert/update"""
        set_phrase = ""
        for pair in r_json:
            set_phrase += "{} = %s, ".format(urllib.quote(str(pair)))
        set_phrase = set_phrase.strip()[:-1]

        query = "{verb} incidents SET {fields} ".format(verb=phrase, fields=set_phrase)
        if phrase == "UPDATE":
            query += "WHERE incident_id = %s"
        logging.debug("db query : {}".format(query))
        
        params = r_json.values()
        if id:
            params.append(id)
        params = tuple(params)
        
        cursor = self.cnx.cursor()
        cursor.execute(query, params)
        if phrase == "UPDATE":
            last_row_id = id
        else:
            last_row_id = cursor.lastrowid
        
        self.cnx.commit()
        return last_row_id
