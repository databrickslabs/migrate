import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="databricks-migration-tool",
    version="1.0.0",
    author="Miklos C",
    author_email="mwc@databricks.com",
    description="Databricks Migration CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/workspace-migration-tool",
    license="https://github.com/databrickslabs/workspace-migration-tool/LICENSE",
    packages=setuptools.find_packages(exclude=['tests', 'tests.*',]),
    install_requires=[
          'requests>=2.17.3',
          'click>=6.7',
          'click-log==0.3.2',
          'databricks-cli==0.11.0',
      ],
    entry_points='''
        [console_scripts]
        databricks-migrate=databricks_migrate.cli:cli
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
