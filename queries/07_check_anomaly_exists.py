import connect_db

pipeline = [
    {
        '$graphLookup': {
            'from': 'staff', 
            'startWith': '$lead', 
            'connectFromField': 'lead', 
            'connectToField': 'id', 
            'as': 'manager', 
            'maxDepth': 0, 
            'restrictSearchWithMatch': {}
        }
    }, {
        '$project': {
            '_id': 0, 
            'id': 1, 
            'lead': 1, 
            'name': 1, 
            'manager': 1, 
            'managerSize': {
                '$size': '$manager'
            }
        }
    }, {
        '$match': {
            'lead': -1, 
            'managerSize': 0
        }
    }, {
        '$count': 'EmployeesWithoutManager'
    }, {
        '$project': {
            'anomalyExists': {
                '$cond': [
                    {
                        '$eq': [
                            '$EmployeesWithoutManager', 1
                        ]
                    }, 0, 1
                ]
            }
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '07_check_anomaly_exists.json')

