from setuptools import setup, find_packages

adaptation_path = "folder/"

setup(
    name='chimera',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'elasticsearch>=5.4.0', 'requests>=2.18.4', 'simplejson>=3.11.1'
    ]
)
