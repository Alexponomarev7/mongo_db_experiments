import connect_db

pipeline = [
    {
        '$match': {
            'id': 1
        }
    }, {
        '$graphLookup': {
            'from': 'staff', 
            'startWith': '$id', 
            'connectFromField': 'id', 
            'connectToField': 'lead', 
            'as': 'leading', 
            'depthField': 'depth', 
            'maxDepth': 3, 
            'restrictSearchWithMatch': {}
        }
    }, {
        '$project': {
            'leading': {
                '$concatArrays': [
                    [
                        {
                            'id': '$id', 
                            'lead': '$lead', 
                            'name': '$name', 
                            'depth': -1
                        }
                    ], '$leading'
                ]
            }
        }
    }, {
        '$unwind': {
            'path': '$leading', 
            'includeArrayIndex': 'index'
        }
    }, {
        '$project': {
            '_id': 0, 
            'id': '$leading.id', 
            'lead': '$leading.lead', 
            'name': '$leading.name', 
            'depth': {
                '$sum': [
                    '$leading.depth', 1
                ]
            }
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
        '$addFields': {
            'managers': {
                '$sortArray': {
                    'input': '$managers', 
                    'sortBy': {
                        'depth': -1
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'managers': {
                '$concatArrays': [
                    {
                        '$map': {
                            'input': '$managers', 
                            'as': 'manager', 
                            'in': {
                                '$toString': '$$manager.id'
                            }
                        }
                    }, [
                        {
                            '$toString': '$id'
                        }
                    ]
                ]
            }
        }
    }, {
        '$addFields': {
            'managers': {
                '$reduce': {
                    'input': '$managers', 
                    'initialValue': '', 
                    'in': {
                        '$concat': [
                            '$$value', {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$$value', ''
                                        ]
                                    }, '', ';'
                                ]
                            }, '$$this'
                        ]
                    }
                }
            }
        }
    }, {
        '$sort': {
            'managers': 1
        }
    }, {
        '$project': {
            'name': {
                '$concat': [
                    {
                        '$substr': [
                            '------', 0, '$depth'
                        ]
                    }, '$name'
                ]
            }
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '09_print_department.json')

