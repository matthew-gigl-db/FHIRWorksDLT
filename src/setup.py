from setuptools import setup, find_packages

setup(
    name='fhirworks_dlt'
    ,version='0.1'
    ,packages=find_packages(include=['fhirworks_dlt', 'fhirworks_dlt.*'])
    # ,install_requires=[
    #     # List your package dependencies here
    #     # subprocess and tempfile are required
    # ]
)