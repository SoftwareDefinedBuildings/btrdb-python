from setuptools import setup
setup(
  name = 'btrdbcapnp',
  packages = ['btrdbcapnp'],
  version = '1.0',
  description = "Bindings to interact with the Berkeley Tree Database using the Cap'n Proto Interface",
  author = 'Sam Kumar, Michael P Andersen',
  author_email = 'samkumar99@gmail.com, michael@steelcode.com',
  url = 'https://github.com/SoftwareDefinedBuildings/btrdb-python',
  download_url = 'https://github.com/SoftwareDefinedBuildings/btrdb-python/tarball/1.0',
  package_data = { 'btrdbcapnp': ['interface.capnp'] },
  include_package_data = True,
  install_requires = ["pycapnp >= 0.5.8"],
  keywords = ['btrdb', 'timeseries', 'database', 'capn', 'capnp', "Cap'n Proto"],
  classifiers = []
)
