from setuptools import setup, find_packages

adaptation_path = "folder/"

setup(
    name='chimera',
    version='2.3.0',
    python_requires=">=3.12",
    packages=find_packages(),
    install_requires=[
        'elasticsearch>=7.10.0,<8.0.0',  # Latest 7.x with Python 3.12 support
        'elasticsearch-dsl>=7.4.0,<8.0.0',  # Latest 7.x compatible
        'requests>=2.28.0',  # Python 3.12 support
        'simplejson>=3.18.0',  # Python 3.12 support
        'PyYAML>=6.0',  # Replace yaml package, Python 3.12 support
    ]
)
