mongoimport \
   --db 'shad_homework' --collection='staff' \
   --file=graph.csv \
   --type=csv \
   --fields="id","lead","name" \
   --uri="mongodb://user:pass@localhost:27017/?authMechanism=SCRAM-SHA-256&authSource=admin"
