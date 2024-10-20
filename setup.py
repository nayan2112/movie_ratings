from setuptools import setup
import os


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths


all_files = package_files('./src')


setup(name='movie_ratings',
      author_email='nayanjn24@gmail.com',
      description='Calculating average rating',
      platforms='Azure-Databricks',
      packages=['data', 'src'],
      package_data={'': all_files},
      include_package_data=True
      )
