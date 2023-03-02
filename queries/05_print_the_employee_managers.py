import connect_db

pipeline = [
    {
        '$match': {
            'name': 'Lynette Weldon'
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
    }
]
connect_db.execute_pipeline('staff', pipeline, '05_print_the_employee_managers.json')

