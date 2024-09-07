from setuptools import setup, find_packages

VERSION = '1.0.0' 
DESCRIPTION = 'BigQuery connection (with Simba)'
LONG_DESCRIPTION = 'BigQuery connection (with Simba)'

# Setting up
setup(
       # 'name' deve corresponder ao nome da pasta 'verysimplemodule'
        name="BigQuery", 
        version=VERSION,
        author="João Paulo Cautter",
        author_email="jcautter@gmail.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=['pyodbc', 'numpy', 'pandas'], # adicione outros pacotes que 
        # precisem ser instalados com o seu pacote. Ex: 'caer'
        
        keywords=['python', 'first package'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)
