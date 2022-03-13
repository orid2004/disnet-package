from setuptools import setup, find_packages

try:
    with open("requirements.txt", "r") as f:
        requirements = f.readlines()
except:
    requirements = []

setup(
    name='disnet',
    version='0.1.1',
    description='A simple memcached solution for any distributed computing goal',
    url='#',
    author='Ori David',
    author_email='orid2004@gmail.com',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements
)
