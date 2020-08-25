from flask import Flask, request, jsonify
import sqlite3
import json
import re # regular expressions
from pprint import pprint

""" My solution requires the json1 extension for sqlite3
I expect the dataset to fit into memory, otherwise my solution would not work.  
I might re-architect this solution and use some combination of MVC, Repository pattern,
an ORM, and dependecy injection.  This would make scaling up easier.  I would look at 
whether or not an rdbms is even the right choice for data storage.  Because of the json in
the blocks tbl, a NoSql with a mapper might be a better choice.  If I had 100 instances
running simultaneously, I might cache the state and messages tbl data with Redis.  
My solution has not been optimized.  Given more time I would try to make it simpler,
and optimized for speed and performance.  There are a number of documented techniques
that I would leverage for that purpose """

app = Flask(__name__)
DBPATH = "database.db"

@app.route("/messages", methods=["GET"])
def messages_route():
    """
    Return all the messages
    """

    with sqlite3.connect(DBPATH) as conn:
        state_cursor = conn.execute("select id, value from state")
        state_dict = dict()       
        for rec in state_cursor:
            state_dict[rec[0]] = rec[1]
        
        messages_res = conn.execute("select body from messages")
        messages = [m[0] for m in messages_res]
        for key, val in state_dict.items():
            for idx, body in enumerate(messages):
                if key in body:
                    messages[idx] = re.sub("{"+ key + "\|[a-zA-Z0-9 ]*}", val, body)
        for idx, body in enumerate(messages):
            messages[idx] = re.sub("{[a-zA-Z0-9]*\||}", "", body)

        return jsonify(messages), 200

def create_sql(query):
    search_terms = str(query).split()
    length = len(search_terms)
    title_and = str()
    json_value_and = str()
    title_or = str()
    json_value_or = str()
    sql = '''select distinct a.id, a.title, b.content 
             from answers a 
             left outer join blocks b on b.answer_id = a.id
             join json_tree((select content from blocks where answer_id = a.id)) jt''' 
    if length == 0:
        return sql
    else:    
        #loop through the search terms and dynamically build the where clause
        for i in range(length):
            if i == 0:    
                title_and = f" where (a.title like '%{search_terms[i]}%'"
                json_value_and = f" and (jt.value like '%{search_terms[i]}%'"
                title_or = f" or ((a.title like '%{search_terms[i]}%'"
                json_value_or = f" and (jt.value like '%{search_terms[i]}%'"
            else:
                title_and = title_and + f" and a.title like '%{search_terms[i]}%'"
                json_value_and = json_value_and + f" and jt.value like '%{search_terms[i]}%'"
                title_or = title_or + f" or a.title like '%{search_terms[i]}%'"
                json_value_or = json_value_or + f" or jt.value like '%{search_terms[i]}%'"
        
        json_fields = "(jt.key in('body','url','alt-text','chance','wait-time')) and (jt.type not in('object','array'))"
        return sql + f"{title_and})" + f" or ({json_fields}" + f"{json_value_and}))" + f"{title_or})"  + f" and {json_fields}" + f"{json_value_or}))"     

@app.route("/search", methods=["POST"])
def search_route():
    """
    Search for answers!

    Accepts a 'query' as JSON post, returns the full answer.

    curl -d '{"query":"Star Trek"}' -H "Content-Type: application/json" -X POST http://localhost:5000/search
    """
    app.config["JSON_SORT_KEYS"] = False
    invalid_json = jsonify(status=400,message="invalid JSON object"), 400
    try:
        query = request.get_json().get("query")
        if query is None:
            return invalid_json
    except:
        return invalid_json 
    sql = create_sql(query)
    print("--->", sql)    
    with sqlite3.connect(DBPATH) as conn:
        res = conn.execute(sql)
        answers = [{"id":r[0],"title":r[1],"content":json.loads(r[2])} for r in res]
        print("--->", query)
        pprint(answers)
        return jsonify(answers), 200

if __name__ == "__main__":
    app.run(debug=True)
