import pymongo
from pymongo import MongoClient
from flask import jsonify

client = MongoClient('mongodb://localhost:27017')
db = client.richu_RideShare_dev
Ride = db.rides
User = db.users
Model = db.users

def writeData(req):
    # need to fill in
    model = req.body.model
    parameters = req.body.parameters
    operation = req.body.operation
    query = req.body.query

    if(model and operation): #RICHA WROTE THIS
        if(not parameters):
            #send res 400 status
            return jsonify({ "success": False, "message": "Parameters cannot be blank." })

        if (operation == "update" not query):
            # send res 400 status
            return jsonify({ "success": False, "message": "Query cannot be blank." })



        if (model):
            if (model == "Ride"):
                Model = Ride
            if(model == "User"):
                Model = User
        
        if(operation == "insert"):
            try:
                insertedID = Model.insert_one(jsonify(parameters)).inserted_id
                print("New document inserted " + insertedID)
            except:
                return jsonify({ "success": False, "message": "Insert one error." })

        if(operation == "delete"):
            try:
                Model.find_one_and_delete(jsonify(parameters))
            except:
                return jsonify({ "success": False, "message": "Find one and delete error." })
  
        if(operation == "update"):
            try:
                Model.find_one_and_update(jsonify(query), jsonify(parameters), jsonify({new: "true"}))
            except:
                return jsonify({ "success": False, "message": "Find one and update error." })
                
        print("DB write done")
        # return res 200 status
        return jsonify({ "success": True, "message": "DB write done" })

        else:
        # return res 500 status 
        return jsonify({ "success": False, "message": "Error: Server error." })

    else:
        # return res 400 status 
        return jsonify({ "success": False, "message": "Error: Model and operation cannot be blank." })
        
        # try:
        #     results = Model.find(parameters)
        # except:
        #     return jsonify({ "success": False, "message": "Find error." })

        # return jsonify(results)
        
    # else:
        # send res 400 status
        # return jsonify({ "success": False, "message": "Model cannot be blank." })
