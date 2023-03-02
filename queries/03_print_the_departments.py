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
    }
]
connect_db.execute_pipeline('staff', pipeline, '03_print_the_departments.json')

