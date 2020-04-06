import pymongo
from pymongo import MongoClient
from flask import jsonify

client = MongoClient('mongodb://localhost:27017')
db = client.richu_RideShare_dev
Ride = db.rides
User = db.users
Model = db.users

def readData(req):
    # need to fill in
    model = req.body.model
    parameters = req/body.parameters

    if (model):
        if (model == "Ride"):
            Model = Ride
        if(model == "User"):
            Model = User
        
        try:
            results = Model.find(parameters)
        except:
            return jsonify({ "success": False, "message": "Find error." })

        return jsonify(results)
        

    else:
        # send res 400 status
        return jsonify({ "success": False, "message": "Model cannot be blank." })
