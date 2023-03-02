import connect_db

pipeline = [
    {
        '$match': {
            'name': 'Charles Ahrens'
        }
    }, {
        '$graphLookup': {
            'from': 'staff', 
            'startWith': '$id', 
            'connectFromField': 'id', 
            'connectToField': 'lead', 
            'as': 'leading', 
            'depthField': 'depth', 
            'restrictSearchWithMatch': {}
        }
    }, {
        '$project': {
            '_id': 0, 
            'managerOfDepartment': '$name', 
            'id': 1, 
            'lead': 1, 
            'departmentSize': {
                '$size': '$leading'
            }
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '06_print_the_department_size.json')

