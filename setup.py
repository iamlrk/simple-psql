from setuptools import setup, find_packages

VERSION = '0.1.0' 
DESCRIPTION = 'Wrapper around psycopg2 to simplify the process of connecting to a PostgreSQL database and executing queries.'
with open('README.md', 'r') as r:
    LONG_DESCRIPTION = r.read()

# Setting up
setup(
       # the name must match the folder name 'simpleopers'
        name = "simplepgsql", 
        version = VERSION,
        authors = "iamlrk",
        author_email="<lepakshiramkiran@hotmail.com>",
        url = 'https://github.com/iamlrk/simple-pgsql',
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        long_description_content_type="text/markdown",
        packages=find_packages(),

        keywords=['python', 'pgsql', 'psql', 'database', 'wrapper', 'psycopg2', 'simple-psql'],
        classifiers= [
            "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
            "Development Status :: 4 - Beta",
            "Programming Language :: Python :: 3",
            "Programming Language :: SQL",
            "Operating System :: OS Independent",
        ]
)