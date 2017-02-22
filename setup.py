from setuptools import setup

setup(name='tsync',
      version='0.1',
      description='Replication Protocol POC',
      url='http://github.com/clayg/tsync',
      author='Clay Gerrard',
      author_email='clay.gerrard@gmail.com  ',
      license='MIT',
      packages=['tsync'],
      entry_points={
          'console_scripts': ['tsync=tsync.cli:main'],
      })
