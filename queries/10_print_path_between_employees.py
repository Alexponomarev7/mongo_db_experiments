import connect_db

pipeline = [
    {
        '$match': {
            '$or': [
                {
                    'id': 273
                }, {
                    'id': 496
                }
            ]
        }
    }, {
        '$graphLookup': {
            'from': 'staff', 
            'startWith': '$lead', 
            'connectFromField': 'lead', 
            'connectToField': 'id', 
            'as': 'managers', 
            'depthField': 'depth', 
            'restrictSearchWithMatch': {}
        }
    }, {
        '$unwind': {
            'path': '$managers', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$project': {
            'leadOf': '$name', 
            'name': '$managers.name', 
            'id': '$managers.id', 
            'lead': '$managers.lead', 
            'depth': '$managers.depth'
        }
    }, {
        '$group': {
            '_id': '$id', 
            'leadOf': {
                '$push': '$leadOf'
            }, 
            'countLeaders': {
                '$sum': 1
            }, 
            'name': {
                '$first': '$name'
            }, 
            'sumDepths': {
                '$sum': '$depth'
            }, 
            'lead': {
                '$first': '$lead'
            }
        }
    }, {
        '$group': {
            '_id': '$leadOf', 
            'countLeaders': {
                '$first': '$countLeaders'
            }, 
            'managers': {
                '$push': {
                    'sumDepths': '$sumDepths', 
                    'name': '$name', 
                    'lead': '$lead', 
                    'id': '$_id'
                }
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'leadOf': '$_id', 
            'managers': {
                '$cond': [
                    {
                        '$eq': [
                            '$countLeaders', 2
                        ]
                    }, {
                        '$min': '$managers'
                    }, '$managers'
                ]
            }
        }
    }, {
        '$unwind': {
            'path': '$managers'
        }
    }, {
        '$sort': {
            'leadOf': 1, 
            'managers.sumDepths': 1
        }
    }, {
        '$project': {
            'leadOf': 1, 
            'name': '$managers.name', 
            'id': '$managers.id', 
            'lead': '$managers.lead'
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '10_print_path_between_employees.json')

