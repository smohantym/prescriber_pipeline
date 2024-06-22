import os

os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'sparkuser1'
os.environ['password'] = 'user123'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

appName = "USA Prescriber Research Report"
currentPath = os.getcwd()
#stagingDimensionCity = currentPath + '\..\staging\dimension_city'
#stagingFact = currentPath + '\..\staging\\fact'
stagingDimensionCity = "prescriber_pipeline/staging/dimension_city"
stagingFact = "prescriber_pipeline/staging/fact"

outputCity = "prescriber_pipeline/output/dimension_city"
outputFact = "prescriber_pipeline/output/fact"

