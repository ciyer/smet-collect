from setuptools import setup

setup(name='smetcollect',
      version='0.10.0',
      description='Collect data from twitter for further analysis.',
      url='http://www.smet.li',
      author='Chandrasekhar Ramakrishnan',
      author_email='ciyer@illposed.com',
      license='BSD',
      packages=['smetcollect'],
      install_requires=[
          'twython',
          'pytest',
          'pyyaml',
          'SQLAlchemy',
          'alembic',
          'python-dateutil',
          'pytz',
          'future',
          'six',
          'click'
      ],
      entry_points='''
        [console_scripts]
        smet-collect=smetcollect.scripts.cli:main
      ''',
      zip_safe=True)
