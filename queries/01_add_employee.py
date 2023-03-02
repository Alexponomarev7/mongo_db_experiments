import connect_db

try:
    connect_db.add_object('staff', {'id': 100500, 'name': 'Alex Ponomarev', 'lead': 5})
except Exception as e:
    print(f'raises because id exists: {e}')

connect_db.add_object('staff', {'id': 10005000, 'name': 'Alex Ponomarev', 'lead': 5})

pipeline = [
    {
        '$match': {
            'name': 'Alex Ponomarev'
        }
    }
]
connect_db.execute_pipeline('staff', pipeline, '01_add_employee.json')

