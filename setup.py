import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r', encoding="utf-8") as fp:
    contents = fp.read()
    requirements = list(map(str.strip, contents.split('\n')))

setuptools.setup(
    name='gcp_sf_utils',
    version='0.0.6',
    author='Inzamam Rahaman',
    author_email='inzamam.rahaman@myguardiangroup.com',
    description='Snowflake utilities for GCP',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/guardian-group-ti/gcp-sf-utils',
    project_urls={
        "Bug Tracker": "https://github.com/guardian-group-ti/gcp-sf-utils/issues"
    },
    license='MIT',
    packages=['gcp-sf-utils'],
    install_requires=requirements
)