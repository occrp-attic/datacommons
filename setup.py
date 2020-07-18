from setuptools import setup, find_packages


setup(
    name="datacommons",
    version="0.2",
    author="Organized Crime and Corruption Reporting Project",
    author_email="data@occrp.org",
    url="http://github.com/alephdata/datacommons",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    package_data={"": ["datacommons/config/*"]},
    include_package_data=True,
    zip_safe=False,
    install_requires=["memorious >= 1.4.1", "followthemoney-store >= 2.1.6"],
    entry_points={"memorious.plugins": ["datacommons = datacommons:init"]},
)
