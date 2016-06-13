from setuptools import setup
setup(
  name = 'btrdb',
  packages = ['btrdb'],
  version = '1.0',
  description = 'Bindings to interact with the Berkeley Tree Database',
  author = 'Sam Kumar, Michael P Andersen',
  author_email = 'samkumar99@gmail.com, michael@steelcode.com',
  url = 'https://github.com/SoftwareDefinedBuildings/btrdb-python',
  download_url = 'https://github.com/SoftwareDefinedBuildings/tarball/1.0',
  install_requires= ["pycapnp >= 0.5.8"],
  keywords = ['btrdb', 'timeseries', 'database'],
  classifiers = []
)
