from setuptools import setup, find_packages


setup(
    name='flexicadastre',
    version='0.2',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://github.com/pudo/flexicadastre',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={
        '': ['flexicadastre/config/*']
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'memorious >= 0.4'
    ],
    entry_points={
       'memorious.plugins': [
            'flexicadastre = flexicadastre:init'
        ]
    }
)
