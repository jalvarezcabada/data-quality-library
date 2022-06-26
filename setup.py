from setuptools import setup, find_packages

setup(
    name = "dq_functions",
    version = "PROD.0.1",
    packages = find_packages(),
    install_requires = ['pandas==1.3.3',
                        'py4j==0.10.9',
                        'pydeequ==1.0.1', 
                        'pymsteams==0.1.16', 
                        'pyspark==3.1.2', 
                        'python-dateutil==2.8.2', 
                        'pytz==2021.3',
                        'urllib3==1.26.7']
)