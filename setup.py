from setuptools import setup, find_packages

adaptation_path = "folder/"

setup(
    name='chimera',
    version='2.1.4',
    packages=find_packages(),
    install_requires=[
        'elasticsearch>=7.0.0,<7.14.0',
        'elasticsearch-dsl>=7.0.0,<=7.4.0',
        'requests>=2.18.4',
        'simplejson>=3.11.1'
    ]
)
