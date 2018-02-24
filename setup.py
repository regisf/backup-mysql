
"""
Set up build

use it : python3 setup.py install
"""
import setuptools

setuptools.setup(
    name="backupdb",
    version="1.0.0",
    description="Simple, safer and faster way to backup a MariaDB/MySQL database",
    author="Regis FLORET",
    author_email="regisfloret@gmail.com",
    url="https://github.com/regisf/backup-mysql",
    scripts=['backupdb']
)
