from pony import orm

# init PonyORM database instance
db = orm.Database()

conn_params = {
    'username': 'mikevanmaele',
    'password': '',
    'host': 'localhost',
    'database': 'covid-npi-policy'
}

db.bind(
    'postgres',
    user=conn_params['username'],
    password=conn_params['password'],
    host=conn_params['host'],
    database=conn_params['database']
)
