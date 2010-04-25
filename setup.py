from setuptools import setup

setup(name='holland_restore',
      version='1.0',
      description='Holland Restore Utilities',
      author='Andrew Garner',
      author_email='andrew.garner@rackspace.com',
      url='http://hollandbackup.org',
      packages=['holland_restore'],
      entry_points="""
      [console_scripts]
      mysqlrestore = holland_restore.script:main
      """,
)
