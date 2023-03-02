import connect_db

connect_db.update_object('staff', {'name': 'Alex Ponomarev'}, {'$set': {'lead': 6}})

pipeline = [
    {
        '$match': {
            'name': 'Alex Ponomarev'
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '02_update_employee.json')

