import os

from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'README.rst'), encoding='utf-8') as file:
    long_description = file.read()

version = {}
with open(os.path.join(os.path.dirname(__file__), 'kafka_manager', '__version__.py')) as file:
    exec(file.read(), version)

setup(
    name='kafka-manager',
    version=version['__version__'],
    description='A Python library for managing Kafka Producers, Consumers and Topics',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    author='Anshuman Pattnaik',
    author_email='anshuman@hackbotone.com',
    url='https://github.com/anshumanpattnaik/kafka-manager',
    packages=['kafka_manager'],
    package_dir={'kafka_manager': 'kafka_manager'},
    install_requires=[
        'kafka-python',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    python_requires='>=3.6',
)
