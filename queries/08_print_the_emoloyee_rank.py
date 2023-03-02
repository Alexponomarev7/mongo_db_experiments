import connect_db

pipeline = [
    {
        '$match': {
            'id': 585
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
        '$project': {
            '_id': 0, 
            'id': 1, 
            'lead': 1, 
            'name': 1, 
            'rank': {
                '$size': '$managers'
            }
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '08_print_the_emoloyee_rank.json')

