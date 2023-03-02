import connect_db

pipeline = [
    {
        '$graphLookup': {
            'from': 'staff', 
            'startWith': '$id', 
            'connectFromField': 'id', 
            'connectToField': 'lead', 
            'as': 'leading', 
            'maxDepth': 0, 
            'restrictSearchWithMatch': {}
        }
    }, {
        '$match': {
            'leading.0': {
                '$exists': False
            }
        }
    }, {
        '$unset': 'leading'
    }
]
connect_db.execute_pipeline('staff', pipeline, '04_get_all_no_lead_employees_sampled.json', limit=10)

