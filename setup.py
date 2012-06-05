from setuptools import setup, find_packages
from riakasaurus import VERSION

setup(name='riakasaurus',
      version=VERSION,
      description="""Riakasaurus is a Twisted client library for Riak""",
      author="Colin Alston",
      author_email="colin.alston@gmail.com",
      license="Apache 2.0",
      url="http://github.com/calston/riakasaurus",
      platforms="Linux",
      long_description="""Currently tested under Python 2.6, Python 2.7, and Linux.""",
      keywords="twisted riak riakasaurus",
      packages=find_packages(),
      package_data={
                     '':['README', '*.txt', 'docs/*.py', ]
                   },
     )

